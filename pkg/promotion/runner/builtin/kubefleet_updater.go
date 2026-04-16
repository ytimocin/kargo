package builtin

import (
	"context"
	"fmt"
	"time"

	fleetv1 "github.com/kubefleet-dev/kubefleet/apis/placement/v1"
	"github.com/xeipuuv/gojsonschema"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kargoapi "github.com/akuity/kargo/api/v1alpha1"
	"github.com/akuity/kargo/pkg/health"
	"github.com/akuity/kargo/pkg/kubefleet"
	"github.com/akuity/kargo/pkg/logging"
	"github.com/akuity/kargo/pkg/promotion"
)

const stepKindKubeFleetUpdate = "kubefleet-update"

func init() {
	promotion.DefaultStepRunnerRegistry.MustRegister(
		promotion.StepRunnerRegistration{
			Name: stepKindKubeFleetUpdate,
			Metadata: promotion.StepRunnerMetadata{
				DefaultTimeout: 5 * time.Minute,
				RequiredCapabilities: []promotion.StepRunnerCapability{
					promotion.StepCapabilityAccessControlPlane,
				},
			},
			Value: newKubefleetUpdater,
		},
	)
}

// KubeFleetUpdateConfig is the configuration for the kubefleet-update step.
type KubeFleetUpdateConfig struct {
	Hub        *kubefleet.HubConfig       `json:"hub,omitempty"`
	Placements []KubeFleetPlacementConfig `json:"placements"`
}

// KubeFleetPlacementConfig is the configuration for a single CRP.
type KubeFleetPlacementConfig struct {
	Name              string                            `json:"name"`
	ResourceSelectors []KubeFleetResourceSelectorConfig `json:"resourceSelectors"`
	Policy            *KubeFleetPolicyConfig            `json:"policy,omitempty"`
	Strategy          *KubeFleetStrategyConfig          `json:"strategy,omitempty"`
}

// KubeFleetResourceSelectorConfig selects a cluster-scoped resource.
type KubeFleetResourceSelectorConfig struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
	Name    string `json:"name,omitempty"`
}

// KubeFleetPolicyConfig is the placement policy configuration.
type KubeFleetPolicyConfig struct {
	PlacementType             string                             `json:"placementType,omitempty"`
	NumberOfClusters          *int32                             `json:"numberOfClusters,omitempty"`
	ClusterNames              []string                           `json:"clusterNames,omitempty"`
	Affinity                  *fleetv1.Affinity                  `json:"affinity,omitempty"`
	TopologySpreadConstraints []fleetv1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
}

// KubeFleetStrategyConfig is the rollout strategy configuration.
type KubeFleetStrategyConfig struct {
	RollingUpdate *KubeFleetRollingUpdateConfig `json:"rollingUpdate,omitempty"`
}

// KubeFleetRollingUpdateConfig is the rolling update parameters.
type KubeFleetRollingUpdateConfig struct {
	MaxUnavailable           *intstr.IntOrString `json:"maxUnavailable,omitempty"`
	MaxSurge                 *intstr.IntOrString `json:"maxSurge,omitempty"`
	UnavailablePeriodSeconds *int                `json:"unavailablePeriodSeconds,omitempty"`
}

type kubefleetUpdater struct {
	schemaLoader       gojsonschema.JSONLoader
	kargoClient        client.Client
	fleetClientBuilder kubefleet.FleetClientBuilder
}

func newKubefleetUpdater(caps promotion.StepRunnerCapabilities) promotion.StepRunner {
	return &kubefleetUpdater{
		schemaLoader:       getConfigSchemaLoader(stepKindKubeFleetUpdate),
		kargoClient:        caps.KargoClient,
		fleetClientBuilder: &kubefleet.DefaultFleetClientBuilder{},
	}
}

// Run implements the promotion.StepRunner interface.
func (k *kubefleetUpdater) Run(
	ctx context.Context,
	stepCtx *promotion.StepContext,
) (promotion.StepResult, error) {
	cfg, err := k.convert(stepCtx.Config)
	if err != nil {
		return promotion.StepResult{
			Status: kargoapi.PromotionStepStatusFailed,
		}, &promotion.TerminalError{Err: err}
	}
	return k.run(ctx, stepCtx, cfg)
}

func (k *kubefleetUpdater) convert(cfg promotion.Config) (KubeFleetUpdateConfig, error) {
	return validateAndConvert[KubeFleetUpdateConfig](k.schemaLoader, cfg, stepKindKubeFleetUpdate)
}

func (k *kubefleetUpdater) run(
	ctx context.Context,
	stepCtx *promotion.StepContext,
	cfg KubeFleetUpdateConfig,
) (promotion.StepResult, error) {
	logger := logging.LoggerFromContext(ctx)

	// Resolve the fleet client: remote hub or local cluster.
	fleetClient, err := k.resolveFleetClient(ctx, stepCtx.Project, cfg.Hub)
	if err != nil {
		return promotion.StepResult{
			Status: kargoapi.PromotionStepStatusErrored,
		}, err
	}

	var (
		aggregatedStatus = kargoapi.PromotionStepStatusSucceeded
		placementChecks  []map[string]any
		outputSummaries  []map[string]any
	)

	for _, p := range cfg.Placements {
		crp := k.buildCRP(p, stepCtx.Project, stepCtx.Stage)

		// Create or update the CRP.
		existing := &fleetv1.ClusterResourcePlacement{}
		err := fleetClient.Get(ctx, client.ObjectKeyFromObject(crp), existing)
		var created bool
		if err != nil {
			if client.IgnoreNotFound(err) != nil {
				return promotion.StepResult{
					Status: kargoapi.PromotionStepStatusErrored,
				}, fmt.Errorf("error getting CRP %q: %w", p.Name, err)
			}
			// CRP does not exist, create it.
			logger.Info("creating ClusterResourcePlacement", "name", p.Name)
			if err = fleetClient.Create(ctx, crp); err != nil {
				return promotion.StepResult{
					Status: kargoapi.PromotionStepStatusErrored,
				}, fmt.Errorf("error creating CRP %q: %w", p.Name, err)
			}
			created = true
		} else {
			// CRP exists, patch it.
			logger.Info("updating ClusterResourcePlacement", "name", p.Name)
			existing.Spec = crp.Spec
			if err = fleetClient.Update(ctx, existing); err != nil {
				return promotion.StepResult{
					Status: kargoapi.PromotionStepStatusErrored,
				}, fmt.Errorf("error updating CRP %q: %w", p.Name, err)
			}
		}

		// Assess CRP status. A just-created CRP cannot be healthy yet, so
		// skip the read-back (the cached client may not have indexed it yet)
		// and go straight to Running. For updates, read back the existing
		// object which is already in the cache.
		var status kargoapi.PromotionStepStatus
		var readBack *fleetv1.ClusterResourcePlacement
		if created {
			status = kargoapi.PromotionStepStatusRunning
		} else {
			readBack = &fleetv1.ClusterResourcePlacement{}
			if err = fleetClient.Get(ctx, client.ObjectKey{Name: p.Name}, readBack); err != nil {
				return promotion.StepResult{
					Status: kargoapi.PromotionStepStatusErrored,
				}, fmt.Errorf("error reading CRP %q status: %w", p.Name, err)
			}
			status = assessCRPStatus(readBack)
		}
		// Failed takes priority: fail fast if any placement has a terminal failure.
		// Running only downgrades Succeeded, never overrides Failed.
		if status == kargoapi.PromotionStepStatusFailed {
			aggregatedStatus = kargoapi.PromotionStepStatusFailed
		} else if status == kargoapi.PromotionStepStatusRunning && aggregatedStatus != kargoapi.PromotionStepStatusFailed {
			aggregatedStatus = kargoapi.PromotionStepStatusRunning
		}

		placementChecks = append(placementChecks, map[string]any{
			"name": p.Name,
		})

		if readBack != nil {
			outputSummaries = append(outputSummaries, buildPlacementSummary(readBack))
		} else {
			outputSummaries = append(outputSummaries, map[string]any{
				"name":             p.Name,
				"selectedClusters": []string{},
				"totalClusters":    0,
				"availableCount":   0,
				"failedCount":      0,
			})
		}
	}

	var retryAfter *time.Duration
	if aggregatedStatus == kargoapi.PromotionStepStatusRunning {
		retryAfter = ptr.To(30 * time.Second)
		logger.Info("step to be retried", "interval", retryAfter)
	}

	// Propagate hub identity through health check Input so the health
	// checker can reach the same remote hub.
	healthInput := health.Input{
		"placements": placementChecks,
	}
	if cfg.Hub != nil {
		healthInput["hub"] = map[string]any{
			"secretName": cfg.Hub.SecretName,
		}
	}

	return promotion.StepResult{
		Status: aggregatedStatus,
		Output: map[string]any{
			"placements": outputSummaries,
		},
		HealthCheck: &health.Criteria{
			Kind:  stepKindKubeFleetUpdate,
			Input: healthInput,
		},
		RetryAfter: retryAfter,
	}, nil
}

// resolveFleetClient returns a Kubernetes client for CRP operations. When
// hub is nil the local kargoClient is used. When hub is set, a credential
// secret is read from the project namespace and a remote client is built.
//
// Security: the secret lookup bypasses the authorizing client. To prevent
// exfiltration of arbitrary secrets, we enforce that the secret carries the
// kargo.akuity.io/cred-type: fleet label.
func (k *kubefleetUpdater) resolveFleetClient(
	ctx context.Context,
	project string,
	hub *kubefleet.HubConfig,
) (client.Client, error) {
	if hub == nil {
		return k.kargoClient, nil
	}

	secret := &corev1.Secret{}
	if err := k.kargoClient.Get(ctx, client.ObjectKey{
		Namespace: project,
		Name:      hub.SecretName,
	}, secret); err != nil {
		return nil, fmt.Errorf("error getting fleet hub secret %q: %w", hub.SecretName, err)
	}

	if secret.Labels[kargoapi.LabelKeyCredentialType] != kargoapi.LabelValueCredentialTypeFleet {
		return nil, &promotion.TerminalError{
			Err: fmt.Errorf(
				"secret %q is not labeled as a fleet credential (%s: %s)",
				hub.SecretName,
				kargoapi.LabelKeyCredentialType,
				kargoapi.LabelValueCredentialTypeFleet,
			),
		}
	}

	c, err := k.fleetClientBuilder.BuildFromSecret(secret)
	if err != nil {
		return nil, fmt.Errorf(
			"error building fleet client from secret %q: %w",
			hub.SecretName, err,
		)
	}
	return c, nil
}

func (k *kubefleetUpdater) buildCRP(
	p KubeFleetPlacementConfig,
	project string,
	stage string,
) *fleetv1.ClusterResourcePlacement {
	crp := &fleetv1.ClusterResourcePlacement{
		ObjectMeta: metav1.ObjectMeta{
			Name: p.Name,
			Labels: map[string]string{
				kargoapi.LabelKeyProject: project,
				kargoapi.LabelKeyStage:   stage,
			},
		},
		Spec: fleetv1.PlacementSpec{
			ResourceSelectors: make([]fleetv1.ResourceSelectorTerm, len(p.ResourceSelectors)),
		},
	}

	for i, rs := range p.ResourceSelectors {
		crp.Spec.ResourceSelectors[i] = fleetv1.ResourceSelectorTerm{
			Group:   rs.Group,
			Version: rs.Version,
			Kind:    rs.Kind,
			Name:    rs.Name,
		}
	}

	if p.Policy != nil {
		crp.Spec.Policy = &fleetv1.PlacementPolicy{}
		if p.Policy.PlacementType != "" {
			crp.Spec.Policy.PlacementType = fleetv1.PlacementType(p.Policy.PlacementType)
		}
		crp.Spec.Policy.NumberOfClusters = p.Policy.NumberOfClusters
		crp.Spec.Policy.ClusterNames = p.Policy.ClusterNames
		crp.Spec.Policy.Affinity = p.Policy.Affinity
		crp.Spec.Policy.TopologySpreadConstraints = p.Policy.TopologySpreadConstraints
	}

	if p.Strategy != nil && p.Strategy.RollingUpdate != nil {
		crp.Spec.Strategy = fleetv1.RolloutStrategy{
			Type: fleetv1.RollingUpdateRolloutStrategyType,
			RollingUpdate: &fleetv1.RollingUpdateConfig{
				MaxUnavailable:           p.Strategy.RollingUpdate.MaxUnavailable,
				MaxSurge:                 p.Strategy.RollingUpdate.MaxSurge,
				UnavailablePeriodSeconds: p.Strategy.RollingUpdate.UnavailablePeriodSeconds,
			},
		}
	}

	return crp
}

// assessCRPStatus checks CRP conditions and returns a promotion step status.
// It compares condition.ObservedGeneration against crp.Generation to avoid
// trusting stale conditions from a previous spec version.
func assessCRPStatus(crp *fleetv1.ClusterResourcePlacement) kargoapi.PromotionStepStatus {
	conditions := crp.Status.Conditions
	generation := crp.Generation

	scheduled := findCondition(conditions, string(fleetv1.ClusterResourcePlacementScheduledConditionType))
	if scheduled != nil && scheduled.ObservedGeneration == generation && scheduled.Status == metav1.ConditionFalse {
		return kargoapi.PromotionStepStatusFailed
	}

	applied := findCondition(conditions, string(fleetv1.ClusterResourcePlacementAppliedConditionType))
	if applied != nil && applied.ObservedGeneration == generation && applied.Status == metav1.ConditionFalse {
		return kargoapi.PromotionStepStatusFailed
	}

	available := findCondition(conditions, string(fleetv1.ClusterResourcePlacementAvailableConditionType))
	// All conditions must be for the current generation and True to succeed.
	if available != nil && available.ObservedGeneration == generation && available.Status == metav1.ConditionTrue &&
		applied != nil && applied.ObservedGeneration == generation && applied.Status == metav1.ConditionTrue {
		return kargoapi.PromotionStepStatusSucceeded
	}

	return kargoapi.PromotionStepStatusRunning
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

func buildPlacementSummary(crp *fleetv1.ClusterResourcePlacement) map[string]any {
	summary := map[string]any{
		"name": crp.Name,
	}

	clusters := make([]string, 0, len(crp.Status.PerClusterPlacementStatuses))
	var availableCount, failedCount int
	for _, pcs := range crp.Status.PerClusterPlacementStatuses {
		clusters = append(clusters, pcs.ClusterName)
		avail := findCondition(pcs.Conditions, string(fleetv1.PerClusterAvailableConditionType))
		if avail != nil && avail.Status == metav1.ConditionTrue {
			availableCount++
		}
		if len(pcs.FailedPlacements) > 0 {
			failedCount++
		}
	}

	summary["selectedClusters"] = clusters
	summary["totalClusters"] = len(clusters)
	summary["availableCount"] = availableCount
	summary["failedCount"] = failedCount

	return summary
}
