cargo build --verbose --bin dfdaemon

cp target/debug/dfdaemon ./ && chmod +x dfdaemon

# Setup Dfdaemon.
cargo run --bin dfdaemon --config peer.yaml -l info --verbose --log-dir log

./dfdaemon --config peer.yaml -l info --verbose --log-dir log

./dfget -O file http://210.28.132.19:18888/Qwen2.5-Coder-32B-Instruct/layer/lm_head_weight.pt -e /var/run/dragonfly/peer.sock
./dfget -O file http://210.28.132.19:18888/Qwen2.5-Coder-32B-Instruct/layer/lm_head_weight.pt -e /var/run/dragonfly/peer-dev.sock