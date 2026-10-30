// Command s3fake serves s3fake over HTTP, for tests in other languages (the
// Python SDK's): s3fake -listen 127.0.0.1:0 -bucket hubwagen prints the
// endpoint's URL on its first line, then serves until killed.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/alper/hubwagen/internal/blob/s3fake"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to listen on")
	bucket := flag.String("bucket", "hubwagen", "bucket to create")
	flag.Parse()
	srv := s3fake.New()
	srv.CreateBucket(*bucket)
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("http://%s\n", ln.Addr())
	log.Fatal(http.Serve(ln, srv))
}
