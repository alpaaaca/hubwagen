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

	"github.com/alper/moksha/internal/config"
)

// ServiceAccountDir is where a pod finds its service account's token, CA
// and namespace.
const ServiceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// Kube manages Deployments of workers through the API server: plain
// HTTP, no client library. It creates and deletes a deployed project's
// (deployments.go) and sets replicas through the scale subresource. In a
// pod it uses the service account (deployments: get, list, create,
// delete; deployments/scale: patch); with an api_url (kubectl proxy) it
// sends no credentials.
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
	body := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	_, err := k.do(ctx, http.MethodPatch, deploymentsPath(ns, deployment)+"/scale", "application/merge-patch+json", []byte(body))
	if err != nil {
		return fmt.Errorf("kubernetes: scale deployment %s/%s: %w", ns, deployment, err)
	}
	return nil
}

// errNotFound is a 404 from the API server.
var errNotFound = errors.New("not found")

// do sends one request to the API server, with the service account's
// token if there is one, and returns the body of a 2xx answer.
func (k *Kube) do(ctx context.Context, method, path, contentType string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, k.api+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")
	if k.tokenFile != "" {
		token, err := os.ReadFile(k.tokenFile) // read each time: projected tokens rotate
		if err != nil {
			return nil, fmt.Errorf("service account token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	}
	res, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	switch {
	case res.StatusCode == http.StatusNotFound:
		return nil, errNotFound
	case res.StatusCode/100 != 2:
		msg := bytes.TrimSpace(out)
		if len(msg) > 512 {
			msg = msg[:512]
		}
		return nil, fmt.Errorf("%s: %s", res.Status, msg)
	}
	return out, nil
}

func deploymentsPath(ns, name string) string {
	p := "/apis/apps/v1/namespaces/" + url.PathEscape(ns) + "/deployments"
	if name != "" {
		p += "/" + url.PathEscape(name)
	}
	return p
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
