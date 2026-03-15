FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder

ARG TARGETARCH
ARG TARGETOS

WORKDIR /src

# Copy the SDK source (needed for the replace directive)
COPY ../ /sdk/
COPY . /src/

# Build
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /burnin-go .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates && \
    adduser -D -u 1000 burnin

COPY --from=builder /burnin-go /usr/local/bin/burnin-go
COPY burnin-config.yaml /etc/burnin/config.yaml

USER burnin
EXPOSE 8888

ENTRYPOINT ["burnin-go"]
CMD ["--config", "/etc/burnin/config.yaml"]
