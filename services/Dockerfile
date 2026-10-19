FROM golang:1.25.14-alpine AS build
ARG SERVICE
WORKDIR /src
COPY . .
RUN go build -o /out/service ./services/${SERVICE}/cmd/${SERVICE}

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=build /out/service /usr/local/bin/service
ENTRYPOINT ["/usr/local/bin/service"]
