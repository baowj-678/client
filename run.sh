cargo build --verbose --bin dfdaemon

cp target/debug/dfdaemon ./ && chmod +x dfdaemon

# Setup Dfdaemon.
cargo run --bin dfdaemon --config peer.yaml -l info --verbose --log-dir log

./dfdaemon --config peer.yaml -l info --verbose --log-dir log

dfget -O file http://f -e /var/run/dragonfly/peer.sock
./dfget -O file http://f -e /var/run/dragonfly/peer-dev.sock

# 限制容器带宽
apt install iproute2
# peer
tc qdisc add dev eth0 root tbf rate 30mbit burst 3mbit latency 1ms
tc -s qdisc show dev eth0
tc qdisc del dev eth0 root

# seed
tc qdisc add dev eth0 root tbf rate 150mbit burst 15mbit latency 500us
tc -s qdisc show dev eth0
tc qdisc del dev eth0 root

### build docker

docker build -f ./ci/Dockerfile -t baowj/client:v0.1.116 .