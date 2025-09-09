# Pi Stream

Production-ready MJPEG streaming server for Raspberry Pi cameras with zero-copy performance optimizations.

## Features

- **High Performance**: Zero-copy frame sharing with `Arc<[u8]>`, batched atomic stats
- **Scalable**: Broadcast channels for efficient multi-client streaming
- **Reliable**: Automatic camera process restart with exponential backoff
- **Configurable**: Full CLI control over all parameters
- **Observable**: Structured logging with tracing, comprehensive stats endpoint
- **Secure**: Request size limits, connection limits, proper resource cleanup

## Installation

```bash
cargo build --release
sudo cp target/release/pi-stream /usr/local/bin/
```

## Usage

Basic usage with defaults:
```bash
pi-stream
```

Custom configuration:
```bash
pi-stream \
  --port 9090 \
  --address 192.168.1.100 \
  --max-clients 50 \
  --buffer-size 64 \
  --camera-args="-t 0 --width 1920 --height 1080 --codec mjpeg -o -" \
  --log-level debug
```

Using with a different camera:
```bash
pi-stream \
  --camera-cmd ffmpeg \
  --camera-args="-f v4l2 -i /dev/video0 -c:v mjpeg -f mjpeg -"
```

## Endpoints

- `/` or `/stream` - MJPEG video stream
- `/snapshot` - Single JPEG frame
- `/stats` - JSON statistics

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `--port` | 8080 | HTTP server port |
| `--address` | 0.0.0.0 | Bind address |
| `--max-clients` | 100 | Maximum concurrent clients |
| `--buffer-size` | 32 | Frame buffer size |
| `--stats-interval` | 30 | Stats reporting interval (seconds) |
| `--camera-cmd` | rpicam-vid | Camera command |
| `--camera-args` | (see help) | Camera command arguments |
| `--log-level` | info | Logging level |
| `--verbose` | false | Enable debug logging |

## Performance Optimizations

- **Zero-copy frames**: Uses `Arc<[u8]>` for efficient frame sharing
- **Batched stats**: Reduces atomic contention with 100ms batching
- **Buffer reuse**: Pre-allocated buffers for MJPEG headers
- **Async I/O**: Fully async camera process management
- **Smart backoff**: Exponential retry with maximum delay cap

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Camera    │────▶│ Frame Parser │────▶│  Broadcast  │
│   Process   │     │   (Async)    │     │   Channel   │
└─────────────┘     └──────────────┘     └─────────────┘
                                                 │
                    ┌────────────────────────────┼────────┐
                    │                            │        │
              ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
              │  Client 1 │  │  Client 2 │  │  Client N │
              │  (MJPEG)  │  │ (Snapshot)│  │  (Stats)  │
              └───────────┘  └───────────┘  └───────────┘
```

## Monitoring

View logs with different levels:
```bash
RUST_LOG=debug pi-stream
RUST_LOG=pi_stream=trace,hyper=info pi-stream
```

Monitor statistics:
```bash
curl http://localhost:8080/stats | jq
```

## License

MIT