package launch

import (
	"encoding/pem"
	"net"
	"net/http/httptest"
	"net/url"
	"testing"
)

func pemOf(t *testing.T, srv *httptest.Server) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}

func netSplit(raw string) (host, port string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", err
	}
	return net.SplitHostPort(u.Host)
}
