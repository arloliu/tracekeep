# syntax=docker/dockerfile:1
# The tracekeep image (architecture.md §3.3): the one binary, the frontend embedded, static, on distroless.
# Build from the repository's root: docker buildx build -f deploy/image/Dockerfile . (make image, make image-check).
# The build stages run on the builder's platform and cross-compile, so another platform needs no emulation.

ARG GO_VERSION=1.26.10
ARG NODE_VERSION=24

FROM --platform=$BUILDPLATFORM node:${NODE_VERSION}-bookworm-slim AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN GOWORK=off go mod download
COPY cmd/ cmd/
COPY internal/ internal/
COPY migrations/ migrations/
COPY proto/ proto/
COPY web/*.go web/
COPY web/fallback/ web/fallback/
COPY --from=web /src/web/dist web/dist
# The tag embeds web/dist, and a build without it would fail to compile, so the image never ships the fallback page.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOWORK=off \
    go build -tags webdist -trimpath -ldflags="-s -w" -o /out/tracekeep ./cmd/tracekeep

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tracekeep /tracekeep
USER 65532:65532
ENTRYPOINT ["/tracekeep"]
