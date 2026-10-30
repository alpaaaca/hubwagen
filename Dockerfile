# The hubwagen server: the binary with the web UI built in, on a distroless
# base. Build from the repository root:
#   docker build -t hubwagen:dev .
# It runs `hubwagen server` with deploy/hubwagen.yaml (listen on 0.0.0.0:7070,
# data in /data); mount your own config at /etc/hubwagen/hubwagen.yaml.
# Multi-platform builds (--platform linux/amd64,linux/arm64) cross-compile:
# the UI and the binary are built on the build machine's platform.

FROM --platform=$BUILDPLATFORM docker.io/library/node:22-alpine AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=ui /src/ui/dist/ internal/webui/dist/
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
      -ldflags "-s -w -X github.com/alper/hubwagen/cmd.Version=${VERSION} -X github.com/alper/hubwagen/cmd.CommitSHA=${COMMIT} -X github.com/alper/hubwagen/cmd.BuildDate=${BUILD_DATE}" \
      -o /out/hubwagen . \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hubwagen /usr/local/bin/hubwagen
COPY --from=build --chown=65532:65532 /out/data /data
COPY deploy/hubwagen.yaml /etc/hubwagen/hubwagen.yaml
ENV HUBWAGEN_CONFIG=/etc/hubwagen/hubwagen.yaml
VOLUME /data
EXPOSE 7070
ENTRYPOINT ["/usr/local/bin/hubwagen"]
CMD ["server", "--log-format", "json"]
