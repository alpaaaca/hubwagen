package api

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/pallet"
)

// DataPath is where the server serves bucket objects to workers, with URLs
// signed by GetUploadURLs and GetDownloadURLs.
const DataPath = "/v1/data/"

// TransferExpiry is how long a transfer URL works.
const TransferExpiry = 15 * time.Minute

// Data hands workers URLs for a running lift attempt's files: presigned
// URLs of the bucket when it has them, else URLs of the server's own data
// endpoint (Handler), which streams to and from the bucket. Either way, a
// URL reaches one object: an output file of an attempt that runs, or a file
// of one of its inputs.
type Data struct {
	bucket  blob.Bucket
	presign blob.Presigner // nil: the server moves the bytes
	id      string
	key     []byte // signs the server's own URLs; new on every start
	now     func() time.Time
	pallets *pallet.Store
	log     *slog.Logger
}

// NewData serves bucket's data to workers. bucketID is the file:// bucket's
// ID (LiftTask.bucket_id). serve sends the bytes through the server even
// when the bucket can presign URLs.
func NewData(bucket blob.Bucket, bucketID string, pallets *pallet.Store, serve bool, log *slog.Logger) *Data {
	d := &Data{bucket: bucket, id: bucketID, now: time.Now, pallets: pallets, log: log}
	if p, ok := bucket.(blob.Presigner); ok && !serve {
		d.presign = p
	}
	d.key = make([]byte, 32)
	_, _ = rand.Read(d.key)
	return d
}

// relPath checks a file path relative to a pallet's or attempt's prefix.
func relPath(p string) error {
	if p == "" || strings.HasPrefix(p, "/") || strings.Contains("/"+p+"/", "/../") || strings.Contains("/"+p+"/", "/./") || strings.Contains(p, "\\") {
		return fmt.Errorf("path %q must be relative and inside the prefix", p)
	}
	return nil
}

func (s *WorkerServer) GetUploadURLs(ctx context.Context, req *connect.Request[hubwagenv1.GetUploadURLsRequest]) (*connect.Response[hubwagenv1.GetUploadURLsResponse], error) {
	d := s.data
	if d == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this server has no bucket for data versions"))
	}
	aio, err := s.core.AttemptIO(ctx, req.Msg.AttemptId)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.GetUploadURLsResponse{}
	for i, f := range req.Msg.Files {
		if err := relPath(f.Path); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("files[%d]: %w", i, err))
		}
		if b, err := hex.DecodeString(f.Sha256); err != nil || len(b) != sha256.Size || f.Size < 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("files[%d]: want a size and a sha256 of 64 hex digits", i))
		}
		uri := aio.Staging + escapePath(f.Path)
		var t blob.Transfer
		if d.presign != nil {
			if t, err = d.presign.PresignPut(ctx, uri, f.Sha256, TransferExpiry); err != nil {
				return nil, connectError(err)
			}
		} else if t, err = d.sign(http.MethodPut, uri, f.Size, f.Sha256); err != nil {
			return nil, connectError(err)
		}
		out.Files = append(out.Files, &hubwagenv1.Transfer{Path: f.Path, Url: t.URL, Headers: t.Headers})
	}
	return connect.NewResponse(out), nil
}

func (s *WorkerServer) GetDownloadURLs(ctx context.Context, req *connect.Request[hubwagenv1.GetDownloadURLsRequest]) (*connect.Response[hubwagenv1.GetDownloadURLsResponse], error) {
	d := s.data
	if d == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this server has no bucket for data versions"))
	}
	aio, err := s.core.AttemptIO(ctx, req.Msg.AttemptId)
	if err != nil {
		return nil, connectError(err)
	}
	location := ""
	for _, in := range aio.Inputs {
		if in.Pallet == req.Msg.Pallet {
			location = in.Location
		}
	}
	if location == "" {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("data version %s is not an input of lift attempt %s", req.Msg.Pallet, req.Msg.AttemptId))
	}
	raw, err := d.bucket.Get(ctx, location)
	if err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("data version %s: manifest %s: %w", req.Msg.Pallet, location, err))
	}
	m, err := d.pallets.Read(ctx, location)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.GetDownloadURLsResponse{ManifestJson: string(raw)}
	for _, f := range m.Files {
		uri := m.Data + escapePath(f.Path)
		var t blob.Transfer
		if d.presign != nil {
			if t, err = d.presign.PresignGet(ctx, uri, TransferExpiry); err != nil {
				return nil, connectError(err)
			}
		} else if t, err = d.sign(http.MethodGet, uri, f.Size, f.SHA256); err != nil {
			return nil, connectError(err)
		}
		out.Files = append(out.Files, &hubwagenv1.Transfer{Path: f.Path, Url: t.URL, Headers: t.Headers})
	}
	return connect.NewResponse(out), nil
}

// escapePath escapes a relative file path for a URI, segment by segment.
func escapePath(p string) string {
	parts := strings.Split(p, "/")
	for i, s := range parts {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}

// sign makes a URL of the data endpoint for one object: a relative URL, so
// the worker resolves it against the server it talks to.
func (d *Data) sign(method, uri string, size int64, sum string) (blob.Transfer, error) {
	base := d.bucket.Base() + "/"
	if !strings.HasPrefix(uri, base) {
		return blob.Transfer{}, fmt.Errorf("%s is outside bucket %s", uri, d.bucket.Base())
	}
	q := url.Values{}
	q.Set("m", method)
	q.Set("size", strconv.FormatInt(size, 10))
	q.Set("sha256", sum)
	q.Set("exp", strconv.FormatInt(d.now().Add(TransferExpiry).Unix(), 10))
	rel := strings.TrimPrefix(uri, base)
	q.Set("sig", d.mac(rel, q))
	return blob.Transfer{URL: DataPath + rel + "?" + q.Encode()}, nil
}

func (d *Data) mac(rel string, q url.Values) string {
	h := hmac.New(sha256.New, d.key)
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%s", q.Get("m"), rel, q.Get("size"), q.Get("sha256"), q.Get("exp"))
	return hex.EncodeToString(h.Sum(nil))
}

// Handler serves DataPath: GET streams an object to the worker, PUT
// streams one into the bucket, checking its size and sha256 before it
// becomes visible.
func (d *Data) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The path as sent: bucket URIs keep their escapes (run keys hold %2F).
		rel := strings.TrimPrefix(r.URL.EscapedPath(), DataPath)
		q := r.URL.Query()
		exp, _ := strconv.ParseInt(q.Get("exp"), 10, 64)
		size, err := strconv.ParseInt(q.Get("size"), 10, 64)
		switch {
		case q.Get("m") != r.Method && !(r.Method == http.MethodHead && q.Get("m") == http.MethodGet):
			http.Error(w, "this URL is for "+q.Get("m"), http.StatusMethodNotAllowed)
			return
		case err != nil || !hmac.Equal([]byte(q.Get("sig")), []byte(d.mac(rel, q))):
			http.Error(w, "bad signature (from a server that has restarted since? ask for a new URL)", http.StatusForbidden)
			return
		case d.now().Unix() > exp:
			http.Error(w, "this URL has expired; ask for a new one", http.StatusForbidden)
			return
		}
		uri := d.bucket.Base() + "/" + rel
		switch r.Method {
		case http.MethodGet, http.MethodHead:
			body, err := d.bucket.GetStream(r.Context(), uri)
			if errors.Is(err, blob.ErrNotExist) {
				http.Error(w, "no such object", http.StatusNotFound)
				return
			}
			if err != nil {
				d.log.Warn("data: reading the bucket", "uri", uri, "err", err)
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer body.Close()
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("Content-Type", "application/octet-stream")
			if r.Method == http.MethodGet {
				_, _ = io.Copy(w, body)
			}
		case http.MethodPut:
			if r.ContentLength >= 0 && r.ContentLength != size {
				http.Error(w, fmt.Sprintf("want %d bytes, got Content-Length %d", size, r.ContentLength), http.StatusBadRequest)
				return
			}
			v := &verifying{r: r.Body, h: sha256.New(), size: size, sum: q.Get("sha256")}
			if err := d.bucket.PutStream(r.Context(), uri, v, size); err != nil {
				status := http.StatusBadGateway
				if v.bad != nil {
					status, err = http.StatusBadRequest, v.bad
				}
				http.Error(w, err.Error(), status)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	})
}

// verifying passes size bytes through and fails the read that completes
// them if they don't have the sha256 they were signed with, so a bad
// upload never becomes an object.
type verifying struct {
	r    io.Reader
	h    hash.Hash
	size int64
	n    int64
	sum  string
	bad  error
}

func (v *verifying) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	v.h.Write(p[:n])
	v.n += int64(n)
	switch {
	case v.n > v.size:
		v.bad = fmt.Errorf("more than %d bytes", v.size)
	case v.n == v.size && hex.EncodeToString(v.h.Sum(nil)) != v.sum:
		v.bad = errors.New("the bytes don't have the sha256 the URL was signed for")
	case err == io.EOF && v.n < v.size:
		v.bad = fmt.Errorf("got %d bytes, want %d", v.n, v.size)
	}
	if v.bad != nil {
		return n, v.bad
	}
	return n, err
}
