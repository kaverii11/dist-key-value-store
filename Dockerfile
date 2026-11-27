# Build a small static binary, then copy it into a minimal runtime image.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/node ./cmd/node \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvctl ./cmd/kvctl

FROM alpine:3.21
RUN adduser -D -u 10001 kv && mkdir -p /data && chown kv /data
COPY --from=build /out/node /out/kvctl /usr/local/bin/
COPY deploy/cluster.docker.json /etc/kv/cluster.json
USER kv
VOLUME /data
ENTRYPOINT ["node", "-config", "/etc/kv/cluster.json", "-data-dir", "/data", "-metrics-addr", ":2112", "-no-repl"]
