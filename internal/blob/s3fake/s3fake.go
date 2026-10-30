// Package s3fake is an S3-compatible server in memory, for tests: the part
// of the S3 HTTP API hubwagen uses (path-style buckets; PUT with
// If-None-Match and x-amz-checksum-sha256, GET, HEAD, DELETE, ListObjectsV2,
// multi-object delete, bucket location), over real HTTP. It checks that
// requests are signed or presigned and that presigned URLs haven't expired,
// but not the signatures themselves; a real store (HUBWAGEN_TEST_S3) covers
// those.
package s3fake

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Server serves buckets from memory. Create buckets with CreateBucket.
type Server struct {
	// NoConditionalWrites makes PUT ignore If-None-Match, like stores that
	// don't support it.
	NoConditionalWrites bool
	// Now is the clock presigned URLs are checked against (default
	// time.Now).
	Now func() time.Time

	mu      sync.Mutex
	buckets map[string]map[string][]byte
}

func New() *Server { return &Server{buckets: map[string]map[string][]byte{}} }

func (s *Server) CreateBucket(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets[name] == nil {
		s.buckets[name] = map[string][]byte{}
	}
}

// Keys returns the keys in a bucket, sorted.
func (s *Server) Keys(bucket string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.buckets[bucket] {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

type errorBody struct {
	XMLName xml.Name `xml:"Error"`
	Code    string
	Message string
}

func fail(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(errorBody{Code: code, Message: msg})
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if code, msg := s.authorized(r, q); code != "" {
		fail(w, http.StatusForbidden, code, msg)
		return
	}
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	s.mu.Lock()
	objs := s.buckets[bucket]
	s.mu.Unlock()
	if objs == nil {
		fail(w, http.StatusNotFound, "NoSuchBucket", "no bucket "+bucket)
		return
	}
	switch {
	case key == "" && r.Method == http.MethodGet && q.Has("location"):
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`)
	case key == "" && r.Method == http.MethodHead:
	case key == "" && r.Method == http.MethodGet:
		s.list(w, bucket, q)
	case key == "" && r.Method == http.MethodPost && q.Has("delete"):
		s.deleteMany(w, r, bucket)
	case key == "":
		fail(w, http.StatusNotImplemented, "NotImplemented", r.Method+" on a bucket")
	case r.Method == http.MethodPut:
		s.put(w, r, bucket, key)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		s.mu.Lock()
		data, ok := s.buckets[bucket][key]
		s.mu.Unlock()
		if !ok {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fail(w, http.StatusNotFound, "NoSuchKey", "no key "+key)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("ETag", etag(data))
		w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	case r.Method == http.MethodDelete:
		s.mu.Lock()
		delete(s.buckets[bucket], key)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		fail(w, http.StatusNotImplemented, "NotImplemented", r.Method)
	}
}

// authorized wants a signature header or a presigned URL that hasn't
// expired.
func (s *Server) authorized(r *http.Request, q url.Values) (code, msg string) {
	if strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
		return "", ""
	}
	if q.Get("X-Amz-Signature") == "" {
		return "AccessDenied", "request is not signed"
	}
	signed, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	if err != nil {
		return "AuthorizationQueryParametersError", "bad X-Amz-Date"
	}
	secs, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil {
		return "AuthorizationQueryParametersError", "bad X-Amz-Expires"
	}
	if s.now().After(signed.Add(time.Duration(secs) * time.Second)) {
		return "AccessDenied", "Request has expired"
	}
	// Headers named in X-Amz-SignedHeaders must be sent.
	for _, h := range strings.Split(q.Get("X-Amz-SignedHeaders"), ";") {
		if h != "" && h != "host" && r.Header.Get(h) == "" {
			return "AccessDenied", "signed header " + h + " is missing"
		}
	}
	return "", ""
}

func etag(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf(`"%x"`, sum[:16])
}

func (s *Server) put(w http.ResponseWriter, r *http.Request, bucket, key string) {
	var body io.Reader = r.Body
	if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		body = &chunked{r: bufio.NewReader(r.Body)}
	}
	data, err := io.ReadAll(body)
	if err != nil {
		fail(w, http.StatusBadRequest, "IncompleteBody", err.Error())
		return
	}
	if want := r.Header.Get("X-Amz-Checksum-Sha256"); want != "" {
		sum := sha256.Sum256(data)
		if base64.StdEncoding.EncodeToString(sum[:]) != want {
			fail(w, http.StatusBadRequest, "BadDigest", "the sha256 you specified did not match the calculated checksum")
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[bucket][key]; exists && r.Header.Get("If-None-Match") == "*" && !s.NoConditionalWrites {
		fail(w, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold")
		return
	}
	s.buckets[bucket][key] = data
	w.Header().Set("ETag", etag(data))
}

// chunked decodes aws-chunked bodies: "<hex size>[;chunk-signature=…]\r\n
// <data>\r\n", ending with a chunk of size 0 and optional trailers.
type chunked struct {
	r    *bufio.Reader
	left int
	done bool
}

func (c *chunked) Read(p []byte) (int, error) {
	for c.left == 0 {
		if c.done {
			return 0, io.EOF
		}
		line, err := c.r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		size, _, _ := strings.Cut(strings.TrimSpace(line), ";")
		n, err := strconv.ParseInt(size, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("aws-chunked: bad chunk size %q", line)
		}
		if n == 0 {
			c.done = true
			_, _ = io.Copy(io.Discard, c.r) // trailers
			return 0, io.EOF
		}
		c.left = int(n)
	}
	n, err := c.r.Read(p[:min(len(p), c.left)])
	c.left -= n
	if c.left == 0 && err == nil {
		_, err = c.r.Discard(2) // \r\n
	}
	return n, err
}

type listResult struct {
	XMLName xml.Name `xml:"ListBucketResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Name    string
	Prefix  string
	// EncodingType "url": keys are query-escaped, as clients ask.
	EncodingType string `xml:",omitempty"`
	KeyCount     int
	MaxKeys      int
	IsTruncated  bool
	Contents     []listEntry
	// NextContinuationToken is the last key returned, when truncated.
	NextContinuationToken string `xml:",omitempty"`
}

type listEntry struct {
	Key          string
	Size         int
	ETag         string
	LastModified string
	StorageClass string
}

// pageSize is small so tests cover pagination.
const pageSize = 100

func (s *Server) list(w http.ResponseWriter, bucket string, q url.Values) {
	prefix, after := q.Get("prefix"), q.Get("continuation-token")
	if after == "" {
		after = q.Get("start-after")
	}
	s.mu.Lock()
	var keys []string
	for k := range s.buckets[bucket] {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := listResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", Name: bucket, Prefix: prefix, MaxKeys: pageSize}
	if len(keys) > pageSize {
		keys = keys[:pageSize]
		res.IsTruncated, res.NextContinuationToken = true, keys[len(keys)-1]
	}
	res.EncodingType = q.Get("encoding-type")
	for _, k := range keys {
		d := s.buckets[bucket][k]
		name := k
		if res.EncodingType == "url" {
			name = url.QueryEscape(k)
		}
		res.Contents = append(res.Contents, listEntry{Key: name, Size: len(d), ETag: etag(d),
			LastModified: time.Unix(0, 0).UTC().Format(time.RFC3339), StorageClass: "STANDARD"})
	}
	s.mu.Unlock()
	res.KeyCount = len(res.Contents)
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(res)
}

func (s *Server) deleteMany(w http.ResponseWriter, r *http.Request, bucket string) {
	var req struct {
		Objects []struct{ Key string } `xml:"Object"`
	}
	data, err := io.ReadAll(r.Body)
	if err == nil {
		err = xml.Unmarshal(data, &req)
	}
	if err != nil {
		fail(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	var out bytes.Buffer
	out.WriteString(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	s.mu.Lock()
	for _, o := range req.Objects {
		delete(s.buckets[bucket], o.Key)
		out.WriteString("<Deleted><Key>")
		_ = xml.EscapeText(&out, []byte(o.Key))
		out.WriteString("</Key></Deleted>")
	}
	s.mu.Unlock()
	out.WriteString("</DeleteResult>")
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write(out.Bytes())
}

// Start serves a new Server with one bucket over HTTP until the test ends,
// and returns it and its endpoint URL.
func Start(tb testing.TB, bucket string) (*Server, string) {
	tb.Helper()
	s := New()
	s.CreateBucket(bucket)
	hs := httptest.NewServer(s)
	tb.Cleanup(hs.Close)
	return s, hs.URL
}
