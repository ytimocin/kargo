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
	"github.com/akuity/kargo/pkg/health"
	"github.com/akuity/kargo/pkg/kubefleet"
)

// fakeFleetClientBuilder returns a pre-built client, ignoring the secret.
type fakeFleetClientBuilder struct {
	client client.Client
	err    error
}

func (f *fakeFleetClientBuilder) BuildFromSecret(_ *corev1.Secret) (client.Client, error) {
	return f.client, f.err
}

func Test_kubefleetChecker_check(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, fleetv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	testCases := []struct {
		name   string
		input  KubeFleetHealthInput
		objs   []fleetv1.ClusterResourcePlacement
		assert func(*testing.T, health.Result)
	}{
		{
			name: "nil client returns unknown",
			input: KubeFleetHealthInput{
				Placements: []KubeFleetPlacementCheck{{Name: "test"}},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateUnknown, res.Status)
				require.Len(t, res.Issues, 1)
				require.Contains(t, res.Issues[0], "requires a Kubernetes client")
			},
		},
		{
			name: "CRP not found returns unhealthy",
			input: KubeFleetHealthInput{
				Placements: []KubeFleetPlacementCheck{{Name: "missing"}},
			},
			objs: nil,
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateUnhealthy, res.Status)
				require.Len(t, res.Issues, 1)
				require.Contains(t, res.Issues[0], "not found")
			},
		},
		{
			name: "CRP with all conditions true returns healthy",
			input: KubeFleetHealthInput{
				Placements: []KubeFleetPlacementCheck{{Name: "healthy-crp"}},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "healthy-crp"},
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
										Type:   string(fleetv1.PerClusterAppliedConditionType),
										Status: metav1.ConditionTrue,
									},
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
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateHealthy, res.Status)
				require.Empty(t, res.Issues)
			},
		},
		{
			name: "CRP with scheduling failure returns unhealthy",
			input: KubeFleetHealthInput{
				Placements: []KubeFleetPlacementCheck{{Name: "sched-fail"}},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "sched-fail"},
					Status: fleetv1.PlacementStatus{
						Conditions: []metav1.Condition{
							{
								Type:    string(fleetv1.ClusterResourcePlacementScheduledConditionType),
								Status:  metav1.ConditionFalse,
								Message: "no matching clusters",
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateUnhealthy, res.Status)
				require.Len(t, res.Issues, 1)
				require.Contains(t, res.Issues[0], "scheduling failed")
			},
		},
		{
			name: "CRP with availability false returns progressing",
			input: KubeFleetHealthInput{
				Placements: []KubeFleetPlacementCheck{{Name: "unavailable-crp"}},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "unavailable-crp"},
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
								Type:    string(fleetv1.ClusterResourcePlacementAvailableConditionType),
								Status:  metav1.ConditionFalse,
								Message: "one cluster still converging",
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateProgressing, res.Status)
				require.Len(t, res.Issues, 1)
				require.Contains(t, res.Issues[0], "not yet available")
			},
		},
		{
			name: "stale generation with healthy per-cluster returns progressing not healthy",
			input: KubeFleetHealthInput{
				Placements: []KubeFleetPlacementCheck{{Name: "stale-crp"}},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:       "stale-crp",
						Generation: 2,
					},
					Status: fleetv1.PlacementStatus{
						Conditions: []metav1.Condition{
							{
								Type:               string(fleetv1.ClusterResourcePlacementAppliedConditionType),
								Status:             metav1.ConditionTrue,
								ObservedGeneration: 1,
							},
							{
								Type:               string(fleetv1.ClusterResourcePlacementAvailableConditionType),
								Status:             metav1.ConditionTrue,
								ObservedGeneration: 1,
							},
						},
						PerClusterPlacementStatuses: []fleetv1.PerClusterPlacementStatus{
							{
								ClusterName: "cluster-1",
								Conditions: []metav1.Condition{
									{
										Type:   string(fleetv1.PerClusterAppliedConditionType),
										Status: metav1.ConditionTrue,
									},
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
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateProgressing, res.Status)
				require.NotEmpty(t, res.Issues)
				require.Contains(t, res.Issues[0], "not yet reconciled")
			},
		},
		{
			name: "CRP still rolling out returns progressing",
			input: KubeFleetHealthInput{
				Placements: []KubeFleetPlacementCheck{{Name: "rolling"}},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "rolling"},
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
						},
						PerClusterPlacementStatuses: []fleetv1.PerClusterPlacementStatus{
							{
								ClusterName: "cluster-1",
								Conditions: []metav1.Condition{
									{
										Type:   string(fleetv1.PerClusterAppliedConditionType),
										Status: metav1.ConditionTrue,
									},
									{
										Type:   string(fleetv1.PerClusterAvailableConditionType),
										Status: metav1.ConditionTrue,
									},
								},
							},
							{
								ClusterName: "cluster-2",
								Conditions: []metav1.Condition{
									{
										Type:   string(fleetv1.PerClusterAppliedConditionType),
										Status: metav1.ConditionTrue,
									},
								},
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateProgressing, res.Status)
			},
		},
		{
			name: "CRP with per-cluster failed placements returns unhealthy",
			input: KubeFleetHealthInput{
				Placements: []KubeFleetPlacementCheck{{Name: "fail-crp"}},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "fail-crp"},
					Status: fleetv1.PlacementStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(fleetv1.ClusterResourcePlacementScheduledConditionType),
								Status: metav1.ConditionTrue,
							},
						},
						PerClusterPlacementStatuses: []fleetv1.PerClusterPlacementStatus{
							{
								ClusterName: "cluster-bad",
								Conditions: []metav1.Condition{
									{
										Type:    string(fleetv1.PerClusterAppliedConditionType),
										Status:  metav1.ConditionFalse,
										Message: "apply error",
									},
								},
								FailedPlacements: []fleetv1.FailedResourcePlacement{
									{
										ResourceIdentifier: fleetv1.ResourceIdentifier{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
											Name:    "my-app",
										},
										Condition: metav1.Condition{
											Message: "resource conflict",
										},
									},
								},
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateUnhealthy, res.Status)
				require.NotEmpty(t, res.Issues)
			},
		},
		{
			name: "no per-cluster statuses returns progressing",
			input: KubeFleetHealthInput{
				Placements: []KubeFleetPlacementCheck{{Name: "empty-crp"}},
			},
			objs: []fleetv1.ClusterResourcePlacement{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "empty-crp"},
					Status: fleetv1.PlacementStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(fleetv1.ClusterResourcePlacementScheduledConditionType),
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateProgressing, res.Status)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var checker *kubefleetChecker
			if tc.name == "nil client returns unknown" {
				checker = newKubefleetChecker(nil)
			} else {
				builder := fake.NewClientBuilder().WithScheme(scheme)
				for i := range tc.objs {
					builder = builder.WithObjects(&tc.objs[i])
				}
				checker = newKubefleetChecker(builder.Build())
			}

			res := checker.Check(
				context.Background(),
				"test-project",
				"test-stage",
				health.Criteria{
					Kind:  "kubefleet-update",
					Input: health.Input{"placements": tc.input.Placements},
				},
			)
			tc.assert(t, res)
		})
	}
}

func Test_kubefleetChecker_remoteHub(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, fleetv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	fleetSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "remote-hub",
			Namespace:       "test-project",
			ResourceVersion: "1",
			Labels: map[string]string{
				kargoapi.LabelKeyCredentialType: kargoapi.LabelValueCredentialTypeFleet,
			},
		},
		Data: map[string][]byte{
			"server":      []byte("https://remote:6443"),
			"bearerToken": []byte("tok"),
		},
	}

	healthyCRP := &fleetv1.ClusterResourcePlacement{
		ObjectMeta: metav1.ObjectMeta{Name: "remote-crp"},
		Status: fleetv1.PlacementStatus{
			Conditions: []metav1.Condition{
				{Type: string(fleetv1.ClusterResourcePlacementScheduledConditionType), Status: metav1.ConditionTrue},
				{Type: string(fleetv1.ClusterResourcePlacementAppliedConditionType), Status: metav1.ConditionTrue},
				{Type: string(fleetv1.ClusterResourcePlacementAvailableConditionType), Status: metav1.ConditionTrue},
			},
			PerClusterPlacementStatuses: []fleetv1.PerClusterPlacementStatus{
				{
					ClusterName: "cluster-1",
					Conditions: []metav1.Condition{
						{Type: string(fleetv1.PerClusterAppliedConditionType), Status: metav1.ConditionTrue},
						{Type: string(fleetv1.PerClusterAvailableConditionType), Status: metav1.ConditionTrue},
					},
				},
			},
		},
	}

	testCases := []struct {
		name               string
		kargoObjs          []client.Object
		remoteObjs         []client.Object
		fleetClientBuilder kubefleet.FleetClientBuilder
		input              KubeFleetHealthInput
		assert             func(*testing.T, health.Result)
	}{
		{
			name:       "remote hub healthy CRP returns healthy",
			kargoObjs:  []client.Object{fleetSecret},
			remoteObjs: []client.Object{healthyCRP},
			fleetClientBuilder: &fakeFleetClientBuilder{
				client: fake.NewClientBuilder().WithScheme(scheme).
					WithObjects(healthyCRP).Build(),
			},
			input: KubeFleetHealthInput{
				Hub:        &kubefleet.HubConfig{SecretName: "remote-hub"},
				Placements: []KubeFleetPlacementCheck{{Name: "remote-crp"}},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateHealthy, res.Status)
				require.Empty(t, res.Issues)
			},
		},
		{
			name:      "hub secret not found returns unknown",
			kargoObjs: nil,
			input: KubeFleetHealthInput{
				Hub:        &kubefleet.HubConfig{SecretName: "missing"},
				Placements: []KubeFleetPlacementCheck{{Name: "crp"}},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateUnknown, res.Status)
				require.Len(t, res.Issues, 1)
				require.Contains(t, res.Issues[0], "fleet hub secret")
			},
		},
		{
			name: "hub secret missing fleet label returns unknown",
			kargoObjs: []client.Object{
				&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "bad-secret",
						Namespace: "test-project",
						Labels:    map[string]string{"wrong": "label"},
					},
				},
			},
			input: KubeFleetHealthInput{
				Hub:        &kubefleet.HubConfig{SecretName: "bad-secret"},
				Placements: []KubeFleetPlacementCheck{{Name: "crp"}},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateUnknown, res.Status)
				require.Contains(t, res.Issues[0], "not labeled as a fleet credential")
			},
		},
		{
			name:      "fleet client build error returns unknown",
			kargoObjs: []client.Object{fleetSecret},
			fleetClientBuilder: &fakeFleetClientBuilder{
				err: fmt.Errorf("dial timeout"),
			},
			input: KubeFleetHealthInput{
				Hub:        &kubefleet.HubConfig{SecretName: "remote-hub"},
				Placements: []KubeFleetPlacementCheck{{Name: "crp"}},
			},
			assert: func(t *testing.T, res health.Result) {
				require.Equal(t, kargoapi.HealthStateUnknown, res.Status)
				require.Contains(t, res.Issues[0], "dial timeout")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kargoBuilder := fake.NewClientBuilder().WithScheme(scheme)
			for _, obj := range tc.kargoObjs {
				kargoBuilder = kargoBuilder.WithObjects(obj)
			}

			checker := &kubefleetChecker{
				kargoClient:        kargoBuilder.Build(),
				fleetClientBuilder: tc.fleetClientBuilder,
			}
			if checker.fleetClientBuilder == nil {
				checker.fleetClientBuilder = &kubefleet.DefaultFleetClientBuilder{}
			}

			res := checker.check(context.Background(), "test-project", tc.input)
			tc.assert(t, res)
		})
	}
}
