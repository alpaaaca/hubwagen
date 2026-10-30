package launch

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alper/hubwagen/internal/config"
)

// ServiceAccountDir is where a pod finds its service account's token, CA
// and namespace.
const ServiceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// Kube sets the replicas of Deployments, for launcher: kubernetes pools,
// through the API server's scale subresource: plain HTTP, no client
// library. In a pod it uses the service account (which needs "patch" on
// deployments/scale); with an api_url (kubectl proxy) it sends no
// credentials.
type Kube struct {
	api       string
	client    *http.Client
	tokenFile string // "": no credentials
	Namespace string // the default for pools that name none
}

// NewKube is the cluster cfg describes; saDir is ServiceAccountDir (tests
// pass another).
func NewKube(cfg config.Kubernetes, saDir string) (*Kube, error) {
	if cfg.APIURL != "" {
		return &Kube{api: strings.TrimSuffix(cfg.APIURL, "/"), client: &http.Client{Timeout: 30 * time.Second},
			Namespace: or(cfg.Namespace, "default")}, nil
	}
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" {
		return nil, errors.New("kubernetes: not in a cluster; set kubernetes.api_url (kubectl proxy: http://127.0.0.1:8001)")
	}
	ca, err := os.ReadFile(filepath.Join(saDir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("kubernetes: service account: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("kubernetes: no certificate in %s", filepath.Join(saDir, "ca.crt"))
	}
	ns := cfg.Namespace
	if ns == "" {
		b, _ := os.ReadFile(filepath.Join(saDir, "namespace"))
		ns = or(strings.TrimSpace(string(b)), "default")
	}
	return &Kube{api: "https://" + strings.Trim(host, "[]") + ":" + port,
		client:    &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}},
		tokenFile: filepath.Join(saDir, "token"), Namespace: ns}, nil
}

// Scale sets deployment's replicas in namespace ("" for the default).
func (k *Kube) Scale(ctx context.Context, namespace, deployment string, replicas int) error {
	ns := or(namespace, k.Namespace)
	u := fmt.Sprintf("%s/apis/apps/v1/namespaces/%s/deployments/%s/scale", k.api, url.PathEscape(ns), url.PathEscape(deployment))
	body := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, u, bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/merge-patch+json")
	if k.tokenFile != "" {
		token, err := os.ReadFile(k.tokenFile) // read each time: projected tokens rotate
		if err != nil {
			return fmt.Errorf("kubernetes: service account token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	res, err := k.client.Do(req)
	if err != nil {
		return fmt.Errorf("kubernetes: scale deployment %s/%s: %w", ns, deployment, err)
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("kubernetes: scale deployment %s/%s: %s: %s", ns, deployment, res.Status, bytes.TrimSpace(msg))
	}
	return nil
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
