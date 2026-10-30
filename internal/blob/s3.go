package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config opens an s3:// bucket: AWS S3 or an S3-compatible store (MinIO,
// Cloudflare R2, Google Cloud Storage's XML API).
type S3Config struct {
	// URL is s3://<bucket>[/<prefix>].
	URL string
	// Endpoint is the store's URL, e.g. https://minio.internal:9000; empty
	// for AWS (https://s3.amazonaws.com).
	Endpoint string
	// PublicEndpoint, if set, is the endpoint in the URLs handed to workers,
	// when they reach the store under another name than the server does.
	PublicEndpoint string
	Region         string // empty: asked of the store
	PathStyle      bool   // bucket in the path, not the host name (MinIO)
	// AccessKeyID and SecretAccessKey are static credentials. Without them
	// the usual AWS sources apply: AWS_ACCESS_KEY_ID and friends, the shared
	// credentials file, then the instance or pod role.
	AccessKeyID, SecretAccessKey string
	// Transport, if set, carries the requests (tests).
	Transport http.RoundTripper
}

type s3Bucket struct {
	base    string // s3://bucket[/prefix]
	bucket  string
	client  *minio.Client
	presign *minio.Client // the same store under PublicEndpoint
}

// OpenS3 opens an s3:// bucket and checks that it is usable: reachable, the
// credentials work, and it has conditional writes (If-None-Match), which
// PutIfAbsent and so commits depend on. A store without them is refused,
// not used with weaker commits.
func OpenS3(ctx context.Context, cfg S3Config) (Bucket, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Scheme != "s3" || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("blob: %q: want s3://bucket or s3://bucket/prefix", cfg.URL)
	}
	if (cfg.AccessKeyID == "") != (cfg.SecretAccessKey == "") {
		return nil, errors.New("blob: give both an access key ID and a secret access key, or neither")
	}
	b := &s3Bucket{base: strings.TrimSuffix(cfg.URL, "/"), bucket: u.Host}
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "https://s3.amazonaws.com"
	}
	if b.client, err = s3Client(endpoint, cfg.Region, cfg); err != nil {
		return nil, err
	}
	region := cfg.Region
	if region == "" {
		if region, err = b.client.GetBucketLocation(ctx, b.bucket); err != nil {
			return nil, fmt.Errorf("blob: %s: %w", b.base, err)
		}
		if region == "" {
			region = "us-east-1"
		}
	}
	b.presign = b.client
	if cfg.PublicEndpoint != "" {
		// Presigning needs no request, but knowing the region: pass it, or
		// the client would ask the public endpoint, which may not answer here.
		if b.presign, err = s3Client(cfg.PublicEndpoint, region, cfg); err != nil {
			return nil, err
		}
	}
	if err := b.probe(ctx); err != nil {
		return nil, err
	}
	return b, nil
}

func s3Client(endpoint, region string, cfg S3Config) (*minio.Client, error) {
	e, err := url.Parse(endpoint)
	if err != nil || (e.Scheme != "http" && e.Scheme != "https") || e.Host == "" || strings.Trim(e.Path, "/") != "" {
		return nil, fmt.Errorf("blob: endpoint %q: want http(s)://host[:port]", endpoint)
	}
	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.EnvAWS{}, &credentials.FileAWSCredentials{}, &credentials.IAM{}})
	if cfg.AccessKeyID != "" {
		creds = credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, "")
	}
	lookup := minio.BucketLookupAuto
	if cfg.PathStyle {
		lookup = minio.BucketLookupPath
	}
	return minio.New(e.Host, &minio.Options{
		Creds: creds, Secure: e.Scheme == "https", Region: region, BucketLookup: lookup, Transport: cfg.Transport})
}

// probe writes an object twice with If-None-Match: the second write must
// be refused.
func (b *s3Bucket) probe(ctx context.Context) error {
	var id [8]byte
	_, _ = rand.Read(id[:])
	uri := b.base + "/.hubwagen-probe/" + hex.EncodeToString(id[:])
	defer b.DeletePrefix(ctx, b.base+"/.hubwagen-probe/") //nolint:errcheck // best effort
	if err := b.PutIfAbsent(ctx, uri, []byte("probe")); err != nil {
		return fmt.Errorf("blob: %s: writing a test object: %w", b.base, err)
	}
	switch err := b.PutIfAbsent(ctx, uri, []byte("probe")); {
	case errors.Is(err, ErrExists):
		return nil
	case err == nil:
		return fmt.Errorf("blob: %s: the store ignores conditional writes (If-None-Match: *); hubwagen needs them for exactly-once commits", b.base)
	default:
		return fmt.Errorf("blob: %s: testing conditional writes: %w", b.base, err)
	}
}

func (b *s3Bucket) Base() string { return b.base }

func (b *s3Bucket) LocalPath(string) (string, bool) { return "", false }

// key maps a URI under the base to an object key. As in the file bucket,
// escapes in the URI (run keys contain %2F) are decoded.
func (b *s3Bucket) key(uri string) (string, error) {
	if uri != b.base && !strings.HasPrefix(uri, b.base+"/") {
		return "", fmt.Errorf("blob: %q is outside bucket %q", uri, b.base)
	}
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("blob: %q: %w", uri, err)
	}
	k := strings.TrimPrefix(u.Path, "/")
	if strings.Contains("/"+k+"/", "/../") || strings.Contains("/"+k+"/", "/./") {
		return "", fmt.Errorf("blob: %q escapes bucket root", uri)
	}
	return k, nil
}

func (b *s3Bucket) Put(ctx context.Context, uri string, data []byte) error {
	return b.PutStream(ctx, uri, bytes.NewReader(data), int64(len(data)))
}

func (b *s3Bucket) PutStream(ctx context.Context, uri string, r io.Reader, size int64) error {
	k, err := b.key(uri)
	if err != nil {
		return err
	}
	_, err = b.client.PutObject(ctx, b.bucket, k, r, size, putOptions())
	return err
}

// putOptions: one plain PUT, unsigned payload (TLS carries integrity; the
// manifest has every file's sha256).
func putOptions() minio.PutObjectOptions {
	return minio.PutObjectOptions{DisableContentSha256: true, DisableMultipart: true}
}

// PutIfAbsent is a PUT with If-None-Match: *. S3 answers 412 if the object
// exists, and 409 to a write racing another conditional write on the same
// key, which is retried: the retry sees the winner's object.
func (b *s3Bucket) PutIfAbsent(ctx context.Context, uri string, data []byte) error {
	k, err := b.key(uri)
	if err != nil {
		return err
	}
	for try := 0; ; try++ {
		opts := putOptions()
		opts.SetMatchETagExcept("*")
		_, err = b.client.PutObject(ctx, b.bucket, k, bytes.NewReader(data), int64(len(data)), opts)
		resp := minio.ToErrorResponse(err)
		switch {
		case err == nil:
			return nil
		case resp.StatusCode == http.StatusPreconditionFailed:
			return ErrExists
		case resp.StatusCode == http.StatusConflict && try < 5:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(try+1) * 50 * time.Millisecond):
			}
		default:
			return err
		}
	}
}

func (b *s3Bucket) Get(ctx context.Context, uri string) ([]byte, error) {
	r, err := b.GetStream(ctx, uri)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	return data, notExist(err)
}

func (b *s3Bucket) GetStream(ctx context.Context, uri string) (io.ReadCloser, error) {
	k, err := b.key(uri)
	if err != nil {
		return nil, err
	}
	obj, err := b.client.GetObject(ctx, b.bucket, k, minio.GetObjectOptions{})
	if err != nil {
		return nil, notExist(err)
	}
	if _, err := obj.Stat(); err != nil { // the request happens here
		obj.Close()
		return nil, notExist(err)
	}
	return obj, nil
}

func notExist(err error) error {
	if err != nil && minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return ErrNotExist
	}
	return err
}

func (b *s3Bucket) List(ctx context.Context, prefix string) ([]string, error) {
	if !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("blob: list prefix %q must end in /", prefix)
	}
	k, err := b.key(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return nil, err
	}
	var out []string
	for obj := range b.client.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{Prefix: k + "/", Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out = append(out, prefix+strings.TrimPrefix(obj.Key, k+"/"))
	}
	sort.Strings(out)
	return out, nil
}

func (b *s3Bucket) DeletePrefix(ctx context.Context, prefix string) error {
	if !strings.HasSuffix(prefix, "/") {
		return fmt.Errorf("blob: delete prefix %q must end in /", prefix)
	}
	k, err := b.key(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return err
	}
	if prefix == b.base+"/" {
		return fmt.Errorf("blob: refusing to delete the whole bucket")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	objs := b.client.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{Prefix: k + "/", Recursive: true})
	var listErr error
	keys := make(chan minio.ObjectInfo)
	go func() {
		defer close(keys)
		for o := range objs {
			if o.Err != nil {
				listErr = o.Err
				return
			}
			select {
			case keys <- o:
			case <-ctx.Done():
				return
			}
		}
	}()
	for e := range b.client.RemoveObjects(ctx, b.bucket, keys, minio.RemoveObjectsOptions{}) {
		if e.Err != nil && minio.ToErrorResponse(e.Err).Code != "NoSuchKey" {
			cancel()
			for range keys { // let the lister finish
			}
			return e.Err
		}
	}
	return listErr
}

func (b *s3Bucket) PresignGet(ctx context.Context, uri string, expires time.Duration) (Transfer, error) {
	k, err := b.key(uri)
	if err != nil {
		return Transfer{}, err
	}
	u, err := b.presign.PresignedGetObject(ctx, b.bucket, k, expires, nil)
	if err != nil {
		return Transfer{}, err
	}
	return Transfer{URL: u.String()}, nil
}

// PresignPut signs the file's sha256 in: the store checks the upload
// against it and refuses other bytes.
func (b *s3Bucket) PresignPut(ctx context.Context, uri string, sha256hex string, expires time.Duration) (Transfer, error) {
	k, err := b.key(uri)
	if err != nil {
		return Transfer{}, err
	}
	sum, err := hex.DecodeString(sha256hex)
	if err != nil || len(sum) != 32 {
		return Transfer{}, fmt.Errorf("blob: sha256 %q: want 64 hex digits", sha256hex)
	}
	h := http.Header{"X-Amz-Checksum-Sha256": {base64.StdEncoding.EncodeToString(sum)}}
	u, err := b.presign.PresignHeader(ctx, http.MethodPut, b.bucket, k, expires, nil, h)
	if err != nil {
		return Transfer{}, err
	}
	return Transfer{URL: u.String(), Headers: map[string]string{"x-amz-checksum-sha256": h.Get("X-Amz-Checksum-Sha256")}}, nil
}
