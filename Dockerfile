# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS builder

ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-s -w -X main.Version=${VERSION}" -o /csar-audit ./cmd/csar-audit

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
RUN adduser -D -u 10001 csar
COPY --from=builder /csar-audit /usr/local/bin/csar-audit
USER csar
ENTRYPOINT ["csar-audit"]
