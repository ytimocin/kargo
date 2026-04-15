package builtin

import (
	"context"
	"fmt"
	"sync"

	fleetv1 "github.com/kubefleet-dev/kubefleet/apis/placement/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kargoapi "github.com/akuity/kargo/api/v1alpha1"
	"github.com/akuity/kargo/pkg/health"
	"github.com/akuity/kargo/pkg/kubefleet"
)

const clusterStatusesKey = "clusterStatuses"

// KubeFleetHealthInput is the input for a health check associated with the
// kubefleet-update step.
type KubeFleetHealthInput struct {
	Hub        *kubefleet.HubConfig      `json:"hub,omitempty"`
	Placements []KubeFleetPlacementCheck `json:"placements"`
}

// KubeFleetPlacementCheck identifies a CRP to check.
type KubeFleetPlacementCheck struct {
	Name string `json:"name"`
}

// kubeFleetClusterStatus describes the health of a single cluster in a CRP.
type kubeFleetClusterStatus struct {
	ClusterName string   `json:"clusterName"`
	Applied     string   `json:"applied"`
	Available   string   `json:"available"`
	Issues      []string `json:"issues,omitempty"`
}

type kubefleetChecker struct {
	kargoClient        client.Client
	fleetClientBuilder kubefleet.FleetClientBuilder
	// clientCache caches remote hub clients keyed by "secretName:resourceVersion"
	// to avoid rebuilding on every health poll.
	clientCache sync.Map
}

func newKubefleetChecker(kargoClient client.Client) *kubefleetChecker {
	return &kubefleetChecker{
		kargoClient:        kargoClient,
		fleetClientBuilder: &kubefleet.DefaultFleetClientBuilder{},
	}
}

// Name implements the health.Checker interface.
func (k *kubefleetChecker) Name() string {
	return "kubefleet-update"
}

// Check implements the health.Checker interface.
func (k *kubefleetChecker) Check(
	ctx context.Context,
	project string,
	_ string,
	criteria health.Criteria,
) health.Result {
	cfg, err := health.InputToStruct[KubeFleetHealthInput](criteria.Input)
	if err != nil {
		return health.Result{
			Status: kargoapi.HealthStateUnknown,
			Issues: []string{
				fmt.Sprintf(
					"could not convert opaque input into %s health check input: %s",
					k.Name(), err.Error(),
				),
			},
		}
	}
	return k.check(ctx, project, cfg)
}

func (k *kubefleetChecker) check(
	ctx context.Context,
	project string,
	input KubeFleetHealthInput,
) health.Result {
	if k.kargoClient == nil {
		return health.Result{
			Status: kargoapi.HealthStateUnknown,
			Issues: []string{
				"KubeFleet integration requires a Kubernetes client; cannot assess " +
					"the health of ClusterResourcePlacement resources",
			},
		}
	}

	// Resolve fleet client: remote hub or local cluster.
	fleetClient, err := k.resolveFleetClient(ctx, project, input.Hub)
	if err != nil {
		return health.Result{
			Status: kargoapi.HealthStateUnknown,
			Issues: []string{err.Error()},
		}
	}

	res := health.Result{
		Status: kargoapi.HealthStateHealthy,
		Issues: make([]string, 0),
	}
	allClusterStatuses := map[string]any{}

	// Use (hub, crp-name) as identity key to avoid collisions when
	// multiple hubs share CRP names.
	hubKey := "local"
	if input.Hub != nil {
		hubKey = input.Hub.SecretName
	}

	for _, p := range input.Placements {
		crp := &fleetv1.ClusterResourcePlacement{}
		if err := fleetClient.Get(ctx, client.ObjectKey{Name: p.Name}, crp); err != nil {
			if apierrors.IsNotFound(err) {
				res.Status = res.Status.Merge(kargoapi.HealthStateUnhealthy)
				res.Issues = append(res.Issues,
					fmt.Sprintf("ClusterResourcePlacement %q not found", p.Name),
				)
				continue
			}
			res.Status = res.Status.Merge(kargoapi.HealthStateUnknown)
			res.Issues = append(res.Issues,
				fmt.Sprintf("error getting ClusterResourcePlacement %q: %s", p.Name, err.Error()),
			)
			continue
		}

		crpHealth, clusterStatuses, issues := mapCRPHealth(crp)
		res.Status = res.Status.Merge(crpHealth)
		res.Issues = append(res.Issues, issues...)
		allClusterStatuses[hubKey+"/"+p.Name] = clusterStatuses
	}

	res.Output = map[string]any{
		clusterStatusesKey: allClusterStatuses,
	}
	return res
}

// resolveFleetClient returns a Kubernetes client for reading CRP status.
// When hub is nil the local kargoClient is used. When hub is set, a
// credential secret is read and a remote client is built (or returned from
// cache if the secret's ResourceVersion hasn't changed).
//
// Security: the secret lookup bypasses the authorizing client. To prevent
// exfiltration of arbitrary secrets, we enforce that the secret carries the
// kargo.akuity.io/cred-type: fleet label.
func (k *kubefleetChecker) resolveFleetClient(
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
		return nil, fmt.Errorf(
			"secret %q is not labeled as a fleet credential (%s: %s)",
			hub.SecretName,
			kargoapi.LabelKeyCredentialType,
			kargoapi.LabelValueCredentialTypeFleet,
		)
	}

	// Check cache: reuse client if secret hasn't changed.
	cacheKey := hub.SecretName + ":" + secret.ResourceVersion
	if cached, ok := k.clientCache.Load(cacheKey); ok {
		c, ok := cached.(client.Client)
		if !ok {
			return nil, fmt.Errorf(
				"cached fleet client for %q has unexpected type %T",
				hub.SecretName, cached,
			)
		}
		return c, nil
	}

	c, err := k.fleetClientBuilder.BuildFromSecret(secret)
	if err != nil {
		return nil, fmt.Errorf(
			"error building fleet client from secret %q: %w",
			hub.SecretName, err,
		)
	}
	k.clientCache.Store(cacheKey, c)
	return c, nil
}

// mapCRPHealth maps CRP conditions and per-cluster statuses to a Kargo health
// state. It compares condition.ObservedGeneration against crp.Generation to
// avoid trusting stale conditions from a previous spec version.
func mapCRPHealth(
	crp *fleetv1.ClusterResourcePlacement,
) (kargoapi.HealthState, []kubeFleetClusterStatus, []string) {
	conditions := crp.Status.Conditions
	generation := crp.Generation
	var issues []string

	// Check fleet-level conditions — only trust conditions that match the
	// current generation.
	scheduled := findCRPCondition(conditions,
		string(fleetv1.ClusterResourcePlacementScheduledConditionType))
	if scheduled != nil && scheduled.ObservedGeneration == generation && scheduled.Status == metav1.ConditionFalse {
		issues = append(issues,
			fmt.Sprintf("CRP %q scheduling failed: %s", crp.Name, scheduled.Message),
		)
		return kargoapi.HealthStateUnhealthy, nil, issues
	}

	applied := findCRPCondition(conditions,
		string(fleetv1.ClusterResourcePlacementAppliedConditionType))
	available := findCRPCondition(conditions,
		string(fleetv1.ClusterResourcePlacementAvailableConditionType))

	if applied != nil && applied.ObservedGeneration == generation && applied.Status == metav1.ConditionFalse {
		issues = append(issues,
			fmt.Sprintf("CRP %q apply failed: %s", crp.Name, applied.Message),
		)
		return kargoapi.HealthStateUnhealthy, nil, issues
	}

	// If conditions haven't caught up to the current generation yet, the
	// controller is still reconciling — report Progressing regardless of what
	// per-cluster statuses say, since they may also be stale.
	staleGeneration := !conditionMatchesGeneration(scheduled, generation) ||
		!conditionMatchesGeneration(applied, generation) ||
		!conditionMatchesGeneration(available, generation)
	if staleGeneration {
		issues = append(issues,
			fmt.Sprintf("CRP %q conditions not yet reconciled for generation %d", crp.Name, generation),
		)
	}

	// Available=False means resources are not available *yet* — this is normal
	// during rollout convergence, not a terminal failure.
	if available != nil && available.ObservedGeneration == generation && available.Status == metav1.ConditionFalse {
		issues = append(issues,
			fmt.Sprintf("CRP %q not yet available: %s", crp.Name, available.Message),
		)
	}

	// Assess per-cluster health.
	clusterStatuses := make([]kubeFleetClusterStatus, 0, len(crp.Status.PerClusterPlacementStatuses))
	overallState := kargoapi.HealthStateHealthy
	if staleGeneration {
		overallState = overallState.Merge(kargoapi.HealthStateProgressing)
	}

	for _, pcs := range crp.Status.PerClusterPlacementStatuses {
		cs := kubeFleetClusterStatus{
			ClusterName: pcs.ClusterName,
			Applied:     "Unknown",
			Available:   "Unknown",
		}

		appliedCond := findCRPCondition(pcs.Conditions, string(fleetv1.PerClusterAppliedConditionType))
		availableCond := findCRPCondition(pcs.Conditions, string(fleetv1.PerClusterAvailableConditionType))

		if appliedCond != nil {
			cs.Applied = string(appliedCond.Status)
		}
		if availableCond != nil {
			cs.Available = string(availableCond.Status)
		}

		if appliedCond != nil && appliedCond.Status == metav1.ConditionFalse {
			msg := fmt.Sprintf("cluster %q: apply failed: %s", pcs.ClusterName, appliedCond.Message)
			cs.Issues = append(cs.Issues, msg)
			issues = append(issues, msg)
			overallState = overallState.Merge(kargoapi.HealthStateUnhealthy)
		} else if availableCond != nil && availableCond.Status == metav1.ConditionTrue &&
			appliedCond != nil && appliedCond.Status == metav1.ConditionTrue {
			// This cluster is healthy.
		} else {
			overallState = overallState.Merge(kargoapi.HealthStateProgressing)
		}

		if len(pcs.FailedPlacements) > 0 {
			for _, fp := range pcs.FailedPlacements {
				msg := fmt.Sprintf(
					"cluster %q: failed placement for %s/%s %s/%s: %s",
					pcs.ClusterName, fp.Group, fp.Version, fp.Kind, fp.Name, fp.Condition.Message,
				)
				cs.Issues = append(cs.Issues, msg)
				issues = append(issues, msg)
			}
			overallState = overallState.Merge(kargoapi.HealthStateUnhealthy)
		}

		clusterStatuses = append(clusterStatuses, cs)
	}

	// If there are no per-cluster statuses yet, the rollout hasn't started.
	if len(crp.Status.PerClusterPlacementStatuses) == 0 {
		overallState = overallState.Merge(kargoapi.HealthStateProgressing)
	}

	return overallState, clusterStatuses, issues
}

func findCRPCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

// conditionMatchesGeneration returns true if the condition exists and its
// ObservedGeneration matches the given generation.
func conditionMatchesGeneration(cond *metav1.Condition, generation int64) bool {
	return cond != nil && cond.ObservedGeneration == generation
}
