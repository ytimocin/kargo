package builtin

import (
	"context"
	"fmt"
	"testing"

	fleetv1 "github.com/kubefleet-dev/kubefleet/apis/placement/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kargoapi "github.com/akuity/kargo/api/v1alpha1"
	"github.com/akuity/kargo/pkg/kubefleet"
	"github.com/akuity/kargo/pkg/promotion"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, fleetv1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	return s
}

// fakeFleetClientBuilder returns a pre-built client, ignoring the secret.
type fakeFleetClientBuilder struct {
	client client.Client
	err    error
}

func (f *fakeFleetClientBuilder) BuildFromSecret(_ *corev1.Secret) (client.Client, error) {
	return f.client, f.err
}

func Test_kubefleetUpdater_run(t *testing.T) {
	scheme := newTestScheme(t)

	testCases := []struct {
		name   string
		cfg    KubeFleetUpdateConfig
		objs   []fleetv1.ClusterResourcePlacement
		assert func(*testing.T, promotion.StepResult, error)
	}{
		{
			name: "creates CRP when it does not exist",
			cfg: KubeFleetUpdateConfig{
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "new-crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{
								Group:   "apps",
								Version: "v1",
								Kind:    "Deployment",
								Name:    "my-app",
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.NoError(t, err)
				require.Equal(t, kargoapi.PromotionStepStatusRunning, res.Status)
				require.NotNil(t, res.HealthCheck)
				require.Equal(t, stepKindKubeFleetUpdate, res.HealthCheck.Kind)
			},
		},
		{
			name: "updates existing CRP",
			cfg: KubeFleetUpdateConfig{
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "existing-crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{
								Group:   "apps",
								Version: "v1",
								Kind:    "Deployment",
								Name:    "updated-app",
							},
						},
					},
				},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "existing-crp"},
					Spec: fleetv1.PlacementSpec{
						ResourceSelectors: []fleetv1.ResourceSelectorTerm{
							{
								Group:   "apps",
								Version: "v1",
								Kind:    "Deployment",
								Name:    "old-app",
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.NoError(t, err)
				require.Equal(t, kargoapi.PromotionStepStatusRunning, res.Status)
			},
		},
		{
			name: "keeps running when availability is false",
			cfg: KubeFleetUpdateConfig{
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "unavailable-crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "unavailable-crp"},
					Spec: fleetv1.PlacementSpec{
						ResourceSelectors: []fleetv1.ResourceSelectorTerm{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
					Status: fleetv1.PlacementStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(fleetv1.ClusterResourcePlacementScheduledConditionType),
								Status: metav1.ConditionTrue,
							},
							{
								Type:   string(fleetv1.ClusterResourcePlacementAppliedConditionType),
								Status: metav1.ConditionTrue,
							},
							{
								Type:   string(fleetv1.ClusterResourcePlacementAvailableConditionType),
								Status: metav1.ConditionFalse,
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.NoError(t, err)
				require.Equal(t, kargoapi.PromotionStepStatusRunning, res.Status)
			},
		},
		{
			name: "succeeds when CRP is fully available",
			cfg: KubeFleetUpdateConfig{
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "healthy-crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "healthy-crp"},
					Spec: fleetv1.PlacementSpec{
						ResourceSelectors: []fleetv1.ResourceSelectorTerm{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
					Status: fleetv1.PlacementStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(fleetv1.ClusterResourcePlacementScheduledConditionType),
								Status: metav1.ConditionTrue,
							},
							{
								Type:   string(fleetv1.ClusterResourcePlacementAppliedConditionType),
								Status: metav1.ConditionTrue,
							},
							{
								Type:   string(fleetv1.ClusterResourcePlacementAvailableConditionType),
								Status: metav1.ConditionTrue,
							},
						},
						PerClusterPlacementStatuses: []fleetv1.PerClusterPlacementStatus{
							{
								ClusterName: "cluster-1",
								Conditions: []metav1.Condition{
									{
										Type:   string(fleetv1.PerClusterAvailableConditionType),
										Status: metav1.ConditionTrue,
									},
								},
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.NoError(t, err)
				require.Equal(t, kargoapi.PromotionStepStatusSucceeded, res.Status)
				require.NotNil(t, res.Output)
				placements, ok := res.Output["placements"].([]map[string]any)
				require.True(t, ok)
				require.Len(t, placements, 1)
				require.Equal(t, "healthy-crp", placements[0]["name"])
				require.Equal(t, 1, placements[0]["totalClusters"])
			},
		},
		{
			name: "reports failed when scheduling fails",
			cfg: KubeFleetUpdateConfig{
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "fail-crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "fail-crp"},
					Spec: fleetv1.PlacementSpec{
						ResourceSelectors: []fleetv1.ResourceSelectorTerm{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
					Status: fleetv1.PlacementStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(fleetv1.ClusterResourcePlacementScheduledConditionType),
								Status: metav1.ConditionFalse,
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.NoError(t, err)
				require.Equal(t, kargoapi.PromotionStepStatusFailed, res.Status)
			},
		},
		{
			name: "multi-placement fail-fast when one fails and another is running",
			cfg: KubeFleetUpdateConfig{
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "failed-crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
					{
						Name: "running-crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "failed-crp"},
					Spec: fleetv1.PlacementSpec{
						ResourceSelectors: []fleetv1.ResourceSelectorTerm{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
					Status: fleetv1.PlacementStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(fleetv1.ClusterResourcePlacementScheduledConditionType),
								Status: metav1.ConditionFalse,
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "running-crp"},
					Spec: fleetv1.PlacementSpec{
						ResourceSelectors: []fleetv1.ResourceSelectorTerm{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.NoError(t, err)
				require.Equal(t, kargoapi.PromotionStepStatusFailed, res.Status)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			for i := range tc.objs {
				builder = builder.WithObjects(&tc.objs[i])
			}
			c := builder.Build()

			updater := &kubefleetUpdater{
				schemaLoader:       getConfigSchemaLoader(stepKindKubeFleetUpdate),
				kargoClient:        c,
				fleetClientBuilder: &kubefleet.DefaultFleetClientBuilder{},
			}

			res, err := updater.run(
				context.Background(),
				&promotion.StepContext{},
				tc.cfg,
			)
			tc.assert(t, res, err)
		})
	}
}

func Test_kubefleetUpdater_remoteHub(t *testing.T) {
	scheme := newTestScheme(t)

	fleetSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "remote-hub",
			Namespace: "test-project",
			Labels: map[string]string{
				kargoapi.LabelKeyCredentialType: kargoapi.LabelValueCredentialTypeFleet,
			},
		},
		Data: map[string][]byte{
			"server":      []byte("https://remote-hub:6443"),
			"bearerToken": []byte("token"),
		},
	}

	testCases := []struct {
		name               string
		cfg                KubeFleetUpdateConfig
		kargoObjs          []client.Object
		remoteObjs         []client.Object
		fleetClientBuilder kubefleet.FleetClientBuilder
		assert             func(*testing.T, promotion.StepResult, error)
	}{
		{
			name: "creates CRP on remote hub",
			cfg: KubeFleetUpdateConfig{
				Hub: &kubefleet.HubConfig{SecretName: "remote-hub"},
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "remote-crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			kargoObjs: []client.Object{fleetSecret},
			fleetClientBuilder: &fakeFleetClientBuilder{
				client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.NoError(t, err)
				require.Equal(t, kargoapi.PromotionStepStatusRunning, res.Status)
				// Health input should include hub
				require.NotNil(t, res.HealthCheck)
				hub, ok := res.HealthCheck.Input["hub"].(map[string]any)
				require.True(t, ok)
				require.Equal(t, "remote-hub", hub["secretName"])
			},
		},
		{
			name: "hub secret not found returns error",
			cfg: KubeFleetUpdateConfig{
				Hub: &kubefleet.HubConfig{SecretName: "nonexistent"},
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			kargoObjs: nil,
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "fleet hub secret")
				require.Equal(t, kargoapi.PromotionStepStatusErrored, res.Status)
			},
		},
		{
			name: "hub secret missing fleet label returns terminal error",
			cfg: KubeFleetUpdateConfig{
				Hub: &kubefleet.HubConfig{SecretName: "bad-label"},
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			kargoObjs: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "bad-label",
						Namespace: "test-project",
						Labels:    map[string]string{"other": "label"},
					},
				},
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "not labeled as a fleet credential")
				var termErr *promotion.TerminalError
				require.ErrorAs(t, err, &termErr)
				require.Equal(t, kargoapi.PromotionStepStatusErrored, res.Status)
			},
		},
		{
			name: "fleet client build failure returns error",
			cfg: KubeFleetUpdateConfig{
				Hub: &kubefleet.HubConfig{SecretName: "remote-hub"},
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			kargoObjs: []client.Object{fleetSecret},
			fleetClientBuilder: &fakeFleetClientBuilder{
				err: fmt.Errorf("connection refused"),
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "connection refused")
				require.Equal(t, kargoapi.PromotionStepStatusErrored, res.Status)
			},
		},
		{
			name: "no hub uses local client",
			cfg: KubeFleetUpdateConfig{
				Placements: []KubeFleetPlacementConfig{
					{
						Name: "local-crp",
						ResourceSelectors: []KubeFleetResourceSelectorConfig{
							{Group: "apps", Version: "v1", Kind: "Deployment"},
						},
					},
				},
			},
			assert: func(t *testing.T, res promotion.StepResult, err error) {
				require.NoError(t, err)
				require.Equal(t, kargoapi.PromotionStepStatusRunning, res.Status)
				// Health input should NOT include hub
				require.NotNil(t, res.HealthCheck)
				_, hasHub := res.HealthCheck.Input["hub"]
				require.False(t, hasHub)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kargoBuilder := fake.NewClientBuilder().WithScheme(scheme)
			for _, obj := range tc.kargoObjs {
				kargoBuilder = kargoBuilder.WithObjects(obj)
			}

			fcb := tc.fleetClientBuilder
			if fcb == nil {
				fcb = &kubefleet.DefaultFleetClientBuilder{}
			}

			updater := &kubefleetUpdater{
				schemaLoader:       getConfigSchemaLoader(stepKindKubeFleetUpdate),
				kargoClient:        kargoBuilder.Build(),
				fleetClientBuilder: fcb,
			}

			res, err := updater.run(
				context.Background(),
				&promotion.StepContext{Project: "test-project"},
				tc.cfg,
			)
			tc.assert(t, res, err)
		})
	}
}

func Test_assessCRPStatus(t *testing.T) {
	const gen int64 = 2

	testCases := []struct {
		name       string
		generation int64
		conditions []metav1.Condition
		expected   kargoapi.PromotionStepStatus
	}{
		{
			name:       "no conditions returns running",
			generation: gen,
			conditions: nil,
			expected:   kargoapi.PromotionStepStatusRunning,
		},
		{
			name:       "scheduled false returns failed",
			generation: gen,
			conditions: []metav1.Condition{
				{
					Type:               string(fleetv1.ClusterResourcePlacementScheduledConditionType),
					Status:             metav1.ConditionFalse,
					ObservedGeneration: gen,
				},
			},
			expected: kargoapi.PromotionStepStatusFailed,
		},
		{
			name:       "applied false returns failed",
			generation: gen,
			conditions: []metav1.Condition{
				{
					Type:               string(fleetv1.ClusterResourcePlacementScheduledConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen,
				},
				{
					Type:               string(fleetv1.ClusterResourcePlacementAppliedConditionType),
					Status:             metav1.ConditionFalse,
					ObservedGeneration: gen,
				},
			},
			expected: kargoapi.PromotionStepStatusFailed,
		},
		{
			name:       "applied true available true returns succeeded",
			generation: gen,
			conditions: []metav1.Condition{
				{
					Type:               string(fleetv1.ClusterResourcePlacementScheduledConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen,
				},
				{
					Type:               string(fleetv1.ClusterResourcePlacementAppliedConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen,
				},
				{
					Type:               string(fleetv1.ClusterResourcePlacementAvailableConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen,
				},
			},
			expected: kargoapi.PromotionStepStatusSucceeded,
		},
		{
			name:       "available false returns running",
			generation: gen,
			conditions: []metav1.Condition{
				{
					Type:               string(fleetv1.ClusterResourcePlacementScheduledConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen,
				},
				{
					Type:               string(fleetv1.ClusterResourcePlacementAppliedConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen,
				},
				{
					Type:               string(fleetv1.ClusterResourcePlacementAvailableConditionType),
					Status:             metav1.ConditionFalse,
					ObservedGeneration: gen,
				},
			},
			expected: kargoapi.PromotionStepStatusRunning,
		},
		{
			name:       "stale generation returns running even if conditions say healthy",
			generation: gen,
			conditions: []metav1.Condition{
				{
					Type:               string(fleetv1.ClusterResourcePlacementAppliedConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen - 1,
				},
				{
					Type:               string(fleetv1.ClusterResourcePlacementAvailableConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen - 1,
				},
			},
			expected: kargoapi.PromotionStepStatusRunning,
		},
		{
			name:       "applied true available not yet returns running",
			generation: gen,
			conditions: []metav1.Condition{
				{
					Type:               string(fleetv1.ClusterResourcePlacementScheduledConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen,
				},
				{
					Type:               string(fleetv1.ClusterResourcePlacementAppliedConditionType),
					Status:             metav1.ConditionTrue,
					ObservedGeneration: gen,
				},
			},
			expected: kargoapi.PromotionStepStatusRunning,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			crp := &fleetv1.ClusterResourcePlacement{
				ObjectMeta: metav1.ObjectMeta{Generation: tc.generation},
				Status: fleetv1.PlacementStatus{
					Conditions: tc.conditions,
				},
			}
			require.Equal(t, tc.expected, assessCRPStatus(crp))
		})
	}
}

func Test_buildCRP(t *testing.T) {
	updater := &kubefleetUpdater{}

	cfg := KubeFleetPlacementConfig{
		Name: "test-crp",
		ResourceSelectors: []KubeFleetResourceSelectorConfig{
			{
				Group:   "apps",
				Version: "v1",
				Kind:    "Deployment",
				Name:    "my-app",
			},
		},
		Policy: &KubeFleetPolicyConfig{
			PlacementType:    "PickN",
			NumberOfClusters: int32Ptr(3),
			Affinity: &fleetv1.Affinity{
				ClusterAffinity: &fleetv1.ClusterAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &fleetv1.ClusterSelector{
						ClusterSelectorTerms: []fleetv1.ClusterSelectorTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"environment": "prod",
									},
								},
							},
						},
					},
				},
			},
			TopologySpreadConstraints: []fleetv1.TopologySpreadConstraint{
				{
					TopologyKey:       "region",
					WhenUnsatisfiable: fleetv1.DoNotSchedule,
				},
			},
		},
	}

	crp := updater.buildCRP(cfg, "my-project", "my-stage")

	require.Equal(t, "test-crp", crp.Name)
	require.Equal(t, "my-project", crp.Labels[kargoapi.LabelKeyProject])
	require.Equal(t, "my-stage", crp.Labels[kargoapi.LabelKeyStage])
	require.Len(t, crp.Spec.ResourceSelectors, 1)
	require.Equal(t, "apps", crp.Spec.ResourceSelectors[0].Group)
	require.Equal(t, "v1", crp.Spec.ResourceSelectors[0].Version)
	require.Equal(t, "Deployment", crp.Spec.ResourceSelectors[0].Kind)
	require.Equal(t, "my-app", crp.Spec.ResourceSelectors[0].Name)
	require.NotNil(t, crp.Spec.Policy)
	require.Equal(t, fleetv1.PickNPlacementType, crp.Spec.Policy.PlacementType)
	require.Equal(t, int32(3), *crp.Spec.Policy.NumberOfClusters)
	require.NotNil(t, crp.Spec.Policy.Affinity)
	terms := crp.Spec.Policy.Affinity.ClusterAffinity.
		RequiredDuringSchedulingIgnoredDuringExecution.ClusterSelectorTerms
	require.Len(t, terms, 1)
	require.Equal(t, "prod", terms[0].LabelSelector.MatchLabels["environment"])
	require.Len(t, crp.Spec.Policy.TopologySpreadConstraints, 1)
	require.Equal(t, "region", crp.Spec.Policy.TopologySpreadConstraints[0].TopologyKey)
}

func int32Ptr(i int32) *int32 {
	return &i
}
