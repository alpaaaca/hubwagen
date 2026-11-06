package launch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// A deployed project's pool on Kubernetes (projects.launcher: kubernetes,
// docs/projects.md): a Deployment per pool and version, created at the
// pool's min, its replicas then set by the scaler. A new version is a new
// Deployment, and the old one is deleted: its pods get SIGTERM and finish
// what they run within their grace period.

// Labels on the Deployments and pods the server makes.
const (
	labelManagedBy = "app.kubernetes.io/managed-by"
	labelInstance  = "moksha.dev/instance"
	labelProject   = "moksha.dev/project"
	labelPool      = "moksha.dev/pool"
)

// GracePeriod is how long a worker pod has to finish its steps once
// stopped (its terminationGracePeriodSeconds).
const GracePeriod = 600

// SecretKey is the key a project's {secret: name} is read from, in the
// Kubernetes Secret called name.
const SecretKey = "value"

// WorkerDeployment is a deployed pool's workers as a Deployment.
type WorkerDeployment struct {
	Name     string // DeploymentName
	Instance string // the server's kubernetes.instance
	Project  string
	Pool     string // the scaler's pool: <project>-<pool>-v<version>
	Image    string
	Replicas int    // at creation; the scaler sets them after
	Server   string // the API's URL as the pods reach it
	Queues   []string
	Steps    int
	Env      map[string]string
	// Secrets: env name → the Kubernetes Secret whose SecretKey it is.
	Secrets     map[string]string
	PullSecrets []string
}

// DeploymentName is the Deployment of pool's version of project, made by
// the server instance: <instance>-<project>-<pool>-v<version> (two
// instances in one namespace stay apart), lower case with _ and . as -,
// shortened with a hash when it would be too long for a label.
func DeploymentName(instance, project, pool string, version int) string {
	name := strings.NewReplacer("_", "-", ".", "-").Replace(strings.ToLower(fmt.Sprintf("%s-%s-%s-v%d", instance, project, pool, version)))
	if len(name) <= 52 { // room for the ReplicaSet's and pod's suffixes in 63
		return name
	}
	h := fnv.New32a()
	h.Write([]byte(name))
	return fmt.Sprintf("%s-%08x", strings.TrimRight(name[:43], "-"), h.Sum32())
}

// CreateDeployment creates d in namespace ("" for the default), unless it
// exists: a version's Deployment never changes.
func (k *Kube) CreateDeployment(ctx context.Context, namespace string, d WorkerDeployment) error {
	ns := or(namespace, k.Namespace)
	body, err := json.Marshal(d.manifest())
	if err != nil {
		return err
	}
	_, err = k.do(ctx, "POST", deploymentsPath(ns, ""), "application/json", body)
	if err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("kubernetes: create deployment %s/%s: %w", ns, d.Name, err)
	}
	return nil
}

// DeleteDeployment deletes a Deployment and, in the background, its pods:
// they get SIGTERM and their grace period. One that is gone already is
// fine.
func (k *Kube) DeleteDeployment(ctx context.Context, namespace, name string) error {
	ns := or(namespace, k.Namespace)
	_, err := k.do(ctx, "DELETE", deploymentsPath(ns, name), "application/json", []byte(`{"propagationPolicy":"Background"}`))
	if err != nil && !errors.Is(err, errNotFound) {
		return fmt.Errorf("kubernetes: delete deployment %s/%s: %w", ns, name, err)
	}
	return nil
}

// WorkerDeployments lists the Deployments the server instance made in
// namespace: their names.
func (k *Kube) WorkerDeployments(ctx context.Context, namespace, instance string) ([]string, error) {
	ns := or(namespace, k.Namespace)
	sel := url.QueryEscape(labelManagedBy + "=moksha," + labelInstance + "=" + instance)
	out, err := k.do(ctx, "GET", deploymentsPath(ns, "")+"?labelSelector="+sel, "", nil)
	if err != nil {
		return nil, fmt.Errorf("kubernetes: list deployments in %s: %w", ns, err)
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, fmt.Errorf("kubernetes: list deployments in %s: %w", ns, err)
	}
	var names []string
	for _, it := range list.Items {
		names = append(names, it.Metadata.Name)
	}
	return names, nil
}

func isAlreadyExists(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "409 ")
}

type obj = map[string]any

// manifest is d as an apps/v1 Deployment.
func (d WorkerDeployment) manifest() obj {
	labels := map[string]string{labelManagedBy: "moksha", labelInstance: d.Instance, labelProject: d.Project, labelPool: d.Pool}
	env := []obj{
		{"name": "MOKSHA_SERVER", "value": d.Server},
		{"name": "MOKSHA_WORKER_ID", "valueFrom": obj{"fieldRef": obj{"fieldPath": "metadata.name"}}}, // the pod's name
		{"name": "MOKSHA_QUEUES", "value": strings.Join(d.Queues, ",")},
		{"name": "MOKSHA_STEPS", "value": strconv.Itoa(d.Steps)},
		{"name": "MOKSHA_DATA", "value": "remote"}, // pods share no files with the server
	}
	for _, k := range slices.Sorted(maps.Keys(d.Env)) {
		env = append(env, obj{"name": k, "value": d.Env[k]})
	}
	for _, k := range slices.Sorted(maps.Keys(d.Secrets)) {
		env = append(env, obj{"name": k, "valueFrom": obj{"secretKeyRef": obj{"name": d.Secrets[k], "key": SecretKey}}})
	}
	pod := obj{
		"automountServiceAccountToken":  false,
		"terminationGracePeriodSeconds": GracePeriod,
		"containers":                    []obj{{"name": "worker", "image": d.Image, "env": env}},
	}
	if len(d.PullSecrets) > 0 {
		var refs []obj
		for _, s := range d.PullSecrets {
			refs = append(refs, obj{"name": s})
		}
		pod["imagePullSecrets"] = refs
	}
	return obj{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   obj{"name": d.Name, "labels": labels},
		"spec": obj{
			"replicas": d.Replicas,
			"selector": obj{"matchLabels": map[string]string{labelInstance: d.Instance, labelPool: d.Pool}},
			"template": obj{"metadata": obj{"labels": labels}, "spec": pod},
		},
	}
}
