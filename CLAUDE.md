# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a production-grade Rust MJPEG streaming server for Raspberry Pi cameras. It features zero-copy performance optimizations, lock-free client statistics, watch-based shutdown signaling, and comprehensive error handling.

## Build and Development Commands

```bash
# Build the project (debug)
cargo build

# Build for release (optimized)
cargo build --release

# Check for compilation errors without building
cargo check

# Run the application with default settings
cargo run

# Run with custom configuration
cargo run -- --port 9090 --max-clients 50 --verbose

# Run tests
cargo test

# Format code
cargo fmt

# Run linter
cargo clippy

# Apply clippy suggestions
cargo fix --bin "pi-stream" --allow-dirty
```

## Architecture

The application uses a multi-component async architecture:

### Core Components
1. **Camera Reader** (`spawn_and_read_camera`) - Async process management with automatic restart
2. **Frame Parser** (`find_jpeg_frames`) - Zero-copy JPEG extraction using `Bytes` slicing
3. **Broadcast System** - Tokio broadcast channels for efficient multi-client streaming
4. **HTTP Server** - Hyper-based async server with configurable limits
5. **Statistics System** - Lock-free per-client stats with periodic global aggregation

### Key Optimizations
- **Zero-copy frames**: Uses `Bytes` instead of `Vec<u8>` for JPEG data
- **Lock-free stats**: Atomic counters per client reduce contention
- **Smart buffer management**: `BytesMut::split_to()` instead of copying
- **Watch-based shutdown**: Instant signal propagation vs polling

### Data Flow
```
Camera Process → BytesMut Buffer → JPEG Parser → Broadcast Channel
                                                        ↓
Multiple Clients ← HTTP Response ← MJPEG Stream ← Frame Subscription
```

## Configuration

All parameters are CLI-configurable:
- Network: `--port`, `--address`
- Capacity: `--max-clients`, `--buffer-size`
- Resources: `--max-partial-frame-mb`, `--request-buffer-kb`
- Camera: `--camera-cmd`, `--camera-args`
- Logging: `--verbose`, `--log-level`

## Error Handling

The code uses structured error handling:
- Camera failures: Exponential backoff with `saturating_pow()`
- Client lag: Differentiated logging (debug vs warn)
- Resource limits: Graceful degradation with warnings
- Shutdown: Coordinated via watch channels

## Performance Characteristics

- **Memory**: Zero-copy frame sharing, bounded buffers
- **CPU**: Lock-free client stats, async I/O throughout
- **Network**: Configurable request limits, efficient streaming
- **Latency**: Watch-based shutdown, minimal copying

## Dependencies

- **tokio**: Async runtime with full features
- **hyper/hyper-util**: HTTP server framework (v1.7 API)
- **bytes**: Zero-copy buffer management
- **tracing**: Structured logging
- **clap**: CLI argument parsing
- **shell-words**: Safe shell argument parsing

## Known Limitations

- Requires `rpicam-vid` or compatible camera command
- MJPEG format only (no H.264 streaming)
- No built-in authentication (intended for LAN use)
- Single camera input (no multi-camera support)

## Testing

Currently no automated tests. Manual testing:
1. Start server: `cargo run`
2. Test endpoints: `curl http://localhost:8080/stats`
3. Test streaming: Open `http://localhost:8080/` in browser
4. Test snapshot: `curl http://localhost:8080/snapshot > test.jpg`