package kubefleet

import (
	"fmt"

	fleetv1 "github.com/kubefleet-dev/kubefleet/apis/placement/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HubConfig identifies a remote KubeFleet hub cluster. Used by both the
// kubefleet-update step runner and the kubefleet health checker.
type HubConfig struct {
	// SecretName is the name of a Secret in the Kargo project namespace
	// that contains credentials for a remote KubeFleet hub. The Secret
	// must be labeled kargo.akuity.io/cred-type: fleet.
	SecretName string `json:"secretName"`
}

// FleetClientBuilder builds a controller-runtime client for a remote
// KubeFleet hub cluster from a credential Secret.
type FleetClientBuilder interface {
	BuildFromSecret(secret *corev1.Secret) (client.Client, error)
}

// DefaultFleetClientBuilder is the production implementation of
// FleetClientBuilder.
type DefaultFleetClientBuilder struct{}

// BuildFromSecret builds a client.Client from a Secret. It supports two
// credential formats:
//   - data.kubeconfig: a full kubeconfig YAML
//   - data.server + data.bearerToken + optional data.caBundle
func (b *DefaultFleetClientBuilder) BuildFromSecret(
	secret *corev1.Secret,
) (client.Client, error) {
	cfg, err := RestConfigFromSecret(secret)
	if err != nil {
		return nil, err
	}

	scheme := runtime.NewScheme()
	if err = fleetv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("error adding fleet v1 scheme: %w", err)
	}
	if err = corev1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("error adding core v1 scheme: %w", err)
	}

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("error building fleet client: %w", err)
	}
	return c, nil
}

// RestConfigFromSecret builds a *rest.Config from a credential Secret.
func RestConfigFromSecret(secret *corev1.Secret) (*rest.Config, error) {
	if kubeconfig, ok := secret.Data["kubeconfig"]; ok && len(kubeconfig) > 0 {
		cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
		if err != nil {
			return nil, fmt.Errorf(
				"error parsing kubeconfig from secret %q: %w",
				secret.Name, err,
			)
		}
		return cfg, nil
	}

	serverBytes, hasServer := secret.Data["server"]
	tokenBytes, hasToken := secret.Data["bearerToken"]
	if !hasServer || !hasToken {
		return nil, fmt.Errorf(
			"secret %q must contain either 'kubeconfig' or both 'server' and 'bearerToken'",
			secret.Name,
		)
	}

	cfg := &rest.Config{
		Host:        string(serverBytes),
		BearerToken: string(tokenBytes),
		// Zero out fields that take precedence over BearerToken.
		BearerTokenFile: "",
	}

	if caBundle, ok := secret.Data["caBundle"]; ok && len(caBundle) > 0 {
		cfg.TLSClientConfig = rest.TLSClientConfig{CAData: caBundle}
	} else {
		// If no CA bundle, allow insecure for development. In production
		// the CA should always be provided.
		cfg.TLSClientConfig = rest.TLSClientConfig{Insecure: true} // nolint: gosec
	}

	return cfg, nil
}
