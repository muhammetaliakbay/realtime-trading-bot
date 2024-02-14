## realtime-trading-bot

### Tracer
Example command line to start Binance Websocket tracer:
```bash
cargo run -- --directory tmp --save-interval 30s --seal-interval 30m --symbol btcusdt --symbol ethusdt
```
```bash
docker run --rm -v "$(pwd)"/tmp:/app/output ghcr.io/muhammetaliakbay/realtime-trading-bot:latest ./realtime-trading-bot --directory /app/output --symbol btcusdt --symbol ethusdt
```