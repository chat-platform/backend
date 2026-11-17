# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.26.0-alpine AS build

ARG SERVICE
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -o /out/service ./services/${SERVICE}/cmd/${SERVICE}

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=build /out/service /usr/local/bin/service
ENTRYPOINT ["/usr/local/bin/service"]
