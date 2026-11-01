# meshpool coordinator and agent. GPU agents need the host's GPU and --features gpu: build
# those natively instead.
FROM rust:1-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin meshpool

FROM debian:stable-slim
# iproute2: `tc netem` for the network simulation in deploy/netsim.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates iproute2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/meshpool /usr/local/bin/meshpool
ENTRYPOINT ["meshpool"]
