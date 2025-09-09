use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures_util::stream::{self, StreamExt};
use http_body_util::{BodyExt, StreamBody, combinators::BoxBody};
use hyper::body::Frame;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use memchr::memmem;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use arc_swap::ArcSwapOption;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::signal;
use tokio::sync::{Semaphore, broadcast, watch};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_RETRY_DELAY: u64 = 60;
const MAX_RETRY_ATTEMPTS: u32 = 20; // Cap retries to prevent infinite loops
const JPEG_SOI: [u8; 2] = [0xFF, 0xD8];
const JPEG_EOI: [u8; 2] = [0xFF, 0xD9];

#[derive(Parser, Debug)]
#[command(name = "pi-stream")]
#[command(about = "MJPEG streaming bridge for Raspberry Pi cameras", long_about = None)]
struct Config {
    /// Port to listen on
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Address to bind to
    #[arg(short = 'a', long, default_value = "0.0.0.0")]
    address: String,

    /// Maximum number of concurrent clients
    #[arg(short = 'm', long, default_value_t = 100)]
    max_clients: usize,

    /// Broadcast channel buffer size (number of frames)
    #[arg(short = 'b', long, default_value_t = 32)]
    buffer_size: usize,

    /// Statistics reporting interval in seconds (0 to disable)
    #[arg(short = 's', long, default_value_t = 30)]
    stats_interval: u64,

    /// Maximum partial frame size in MB
    #[arg(long, default_value_t = 5)]
    max_partial_frame_mb: usize,

    /// HTTP request buffer size in KB
    #[arg(long, default_value_t = 64)]
    request_buffer_kb: usize,

    /// Maximum camera buffer size in MB (before clearing)
    #[arg(long, default_value_t = 10)]
    max_camera_buffer_mb: usize,

    /// Maximum read buffer size in KB
    #[arg(long, default_value_t = 64)]
    read_buffer_kb: usize,

    /// Camera command to execute
    #[arg(short = 'c', long, default_value = "rpicam-vid")]
    camera_cmd: String,

    /// Camera command arguments (shell-quoted string)
    #[arg(
        short = 'A',
        long,
        default_value = "-t 0 --nopreview --codec mjpeg -o -"
    )]
    camera_args: String,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short = 'l', long, default_value = "info")]
    log_level: String,
    
    /// Global maximum memory usage in MB (0 = unlimited)
    #[arg(long, default_value_t = 100)]
    max_total_memory_mb: usize,
    
    /// Memory overhead estimation in MB (for runtime allocations, HTTP processing, etc.)
    #[arg(long, default_value_t = 50)]
    memory_overhead_mb: usize,
    
    /// Timeout in seconds for slow client connections
    #[arg(long, default_value_t = 5)]
    client_timeout_secs: u64,
}

struct FrameData {
    jpeg: Bytes,
    sequence: u64,
}

impl FrameData {
    fn new_frame(jpeg: Bytes, sequence: u64) -> Self {
        Self { jpeg, sequence }
    }
}

struct ServerStats {
    total_connections: AtomicU64,
    active_connections: AtomicUsize,
    frames_served: AtomicU64,
    bytes_sent: AtomicU64,
    rejected_connections: AtomicU64,
}

struct ClientStats {
    frames_served: AtomicU64,
    bytes_sent: AtomicU64,
    last_update: AtomicU64, // Milliseconds since start for consistency
}

impl ClientStats {
    fn new() -> Self {
        Self {
            frames_served: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            last_update: AtomicU64::new(Self::current_millis()),
        }
    }

    fn current_millis() -> u64 {
        // Use a stable start time reference for milliseconds
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        let start = START.get_or_init(Instant::now);
        start.elapsed().as_millis() as u64
    }

    fn add_frame(&self, bytes: usize) {
        self.frames_served.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
        self.last_update.store(Self::current_millis(), Ordering::Relaxed);
    }

    fn should_flush_to_global(&self, global_stats: &ServerStats) -> bool {
        let last = self.last_update.load(Ordering::Relaxed);
        let now = Self::current_millis();
        
        // More aggressive flushing: 500ms for regular flush
        if now.saturating_sub(last) > 500 { 
            self.flush_to_global(global_stats);
            true
        } else {
            false
        }
    }

    fn force_flush_if_stale(&self, global_stats: &ServerStats, max_age_ms: u64) -> bool {
        let last = self.last_update.load(Ordering::Relaxed);
        let now = Self::current_millis();
        
        if now.saturating_sub(last) > max_age_ms {
            self.flush_to_global(global_stats);
            true
        } else {
            false
        }
    }

    fn flush_to_global(&self, global_stats: &ServerStats) {
        let frames = self.frames_served.swap(0, Ordering::Relaxed);
        let bytes = self.bytes_sent.swap(0, Ordering::Relaxed);

        if frames > 0 {
            global_stats
                .frames_served
                .fetch_add(frames, Ordering::Relaxed);
            global_stats.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        }
        
        // Reset last_update to prevent repeated flushes
        self.last_update.store(Self::current_millis(), Ordering::Relaxed);
    }
}

impl ServerStats {
    fn new() -> Self {
        Self {
            total_connections: AtomicU64::new(0),
            active_connections: AtomicUsize::new(0),
            frames_served: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
        }
    }
}

fn validate_config(config: &Config) -> Result<(), String> {
    // Validate log level
    match config.log_level.as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => {},
        _ => return Err(format!("Invalid log level: {}", config.log_level)),
    }
    
    // Validate memory limits
    if config.max_partial_frame_mb == 0 {
        return Err("max_partial_frame_mb must be greater than 0".to_string());
    }
    if config.max_camera_buffer_mb == 0 {
        return Err("max_camera_buffer_mb must be greater than 0".to_string());
    }
    if config.max_total_memory_mb > 0 {
        let total_required = config.max_partial_frame_mb + config.max_camera_buffer_mb + config.memory_overhead_mb;
        if total_required > config.max_total_memory_mb {
            return Err(format!(
                "Total memory requirement ({} MB) exceeds max_total_memory_mb ({} MB)",
                total_required, config.max_total_memory_mb
            ));
        }
    }
    
    // Validate buffer sizes
    if config.buffer_size == 0 {
        return Err("buffer_size must be greater than 0".to_string());
    }
    if config.max_clients == 0 {
        return Err("max_clients must be greater than 0".to_string());
    }
    
    // Validate camera args
    if let Err(e) = shell_words::split(&config.camera_args) {
        warn!("Camera arguments may be malformed: {}", e);
        warn!("Using raw arguments string: {}", config.camera_args);
    }
    
    Ok(())
}

fn find_jpeg_frames(
    buffer: &mut BytesMut,
    partial: &mut BytesMut,
    max_partial_size: usize,
) -> Vec<Bytes> {
    let mut frames = Vec::new();

    // Prepend any partial frame from previous iteration - more efficient approach
    if !partial.is_empty() {
        // Only concatenate once when needed instead of per-iteration memmove
        let mut combined = BytesMut::with_capacity(partial.len() + buffer.len());
        combined.extend_from_slice(partial);
        combined.extend_from_slice(buffer);
        *buffer = combined;
        partial.clear();
    }

    if buffer.len() < 4 {
        return frames; // Need at least SOI + EOI
    }

    let soi_finder = memmem::Finder::new(&JPEG_SOI);
    let eoi_finder = memmem::Finder::new(&JPEG_EOI);
    
    loop {
        let data = &buffer[..];
        
        // Fast search for next JPEG start
        if let Some(soi_pos) = soi_finder.find(data) {
            // Fast search for corresponding end marker
            let search_start = soi_pos + 2;
            if search_start >= data.len() {
                break;
            }
            
            if let Some(eoi_pos) = eoi_finder.find(&data[search_start..]) {
                let absolute_eoi = search_start + eoi_pos;
                let frame_end = absolute_eoi + 2;
                
                if frame_end <= data.len() {
                    // Complete frame found - extract with true zero-copy using split_to
                    if soi_pos > 0 {
                        let _ = buffer.split_to(soi_pos); // Discard data before frame
                    }
                    let frame_bytes = buffer.split_to(frame_end - soi_pos);
                    frames.push(frame_bytes.freeze());
                    
                    // Removed debug logging from hot path - use trace if needed
                    // trace!("Found complete JPEG frame: {} bytes", frame_end - soi_pos);
                    continue; // Look for more frames in remaining buffer
                }
            }
            
            // No complete frame found - save partial if within limits
            let partial_data = &data[soi_pos..];
            // Check both individual frame size and total accumulated partial size
            if partial_data.len() < max_partial_size && 
               (partial.len() + partial_data.len()) < max_partial_size {
                partial.extend_from_slice(partial_data);
                // Removed debug logging from hot path - use trace if needed
                // trace!("Saved partial frame: {} bytes", partial_data.len());
            } else {
                // Rate-limit warning to prevent log spam
                static PARTIAL_WARN_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let count = PARTIAL_WARN_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if count == 0 || count % 100 == 0 {
                    warn!(
                        "Partial frame size limit exceeded: new={} bytes, existing={} bytes, limit={:.1}MB, discarding (occurrence #{})",
                        partial_data.len(),
                        partial.len(),
                        max_partial_size as f64 / 1_000_000.0,
                        count + 1
                    );
                } else {
                    tracing::trace!(
                        "Partial frame size limit exceeded: new={} bytes, existing={} bytes, limit={:.1}MB, discarding (occurrence #{})",
                        partial_data.len(),
                        partial.len(),
                        max_partial_size as f64 / 1_000_000.0,
                        count + 1
                    );
                }
                // Clear any existing partial data to prevent growth
                partial.clear();
            }
            buffer.clear(); // Consume all remaining data
            break;
        } else {
            // No SOI found, consume everything
            buffer.clear();
            break;
        }
    }

    frames
}

fn calculate_backoff(attempt: u32) -> Duration {
    // Use bit shift for power of 2, cap at 6 to avoid unnecessary computation
    let capped_attempt = attempt.min(6); // 2^6 = 64, which will be clamped to 60 anyway
    let base_delay = (1u64 << capped_attempt).min(MAX_RETRY_DELAY);
    
    // Add jitter to prevent thundering herd: 80-120% of base delay
    let jitter_factor = 0.8 + 0.4 * rand::random::<f64>();
    
    let jittered_delay = ((base_delay as f64 * jitter_factor).round() as u64).max(1);
    Duration::from_secs(jittered_delay)
}

async fn spawn_and_read_camera(
    config: Arc<Config>,
    frame_tx: broadcast::Sender<Arc<FrameData>>,
    latest_frame: Arc<ArcSwapOption<FrameData>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut retry_attempt = 0u32;
    let mut frame_sequence = 0u64;
    let reset_window_secs = 30;
    let mut camera_start_time: Option<Instant>;
    let max_partial_size = config.max_partial_frame_mb * 1_000_000;
    let camera_args = match shell_words::split(&config.camera_args) {
        Ok(args) => args,
        Err(e) => {
            error!("Failed to parse camera arguments: {}", e);
            vec![config.camera_args.clone()]
        }
    };

    loop {
        // Check for shutdown signal
        if shutdown_rx.has_changed().unwrap_or(true) && *shutdown_rx.borrow() {
            debug!("Camera reader shutting down");
            break;
        }

        let mut child = match Command::new(&config.camera_cmd)
            .args(&camera_args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => {
                info!(
                    "Started {} process (attempt #{})",
                    config.camera_cmd,
                    retry_attempt + 1
                );
                
                // Record when camera started successfully for sliding reset window
                camera_start_time = Some(Instant::now());
                
                child
            }
            Err(e) => {
                if retry_attempt >= MAX_RETRY_ATTEMPTS {
                    error!(
                        "Failed to start {} after {} attempts: {}. Giving up.",
                        config.camera_cmd,
                        MAX_RETRY_ATTEMPTS,
                        e
                    );
                    break;
                }
                let delay = calculate_backoff(retry_attempt);
                
                // Log suppression: log every attempt for first 3, then exponentially less frequently
                let should_log = retry_attempt < 3 || 
                    retry_attempt == 5 || 
                    retry_attempt == 10 || 
                    retry_attempt == 15 ||
                    retry_attempt == MAX_RETRY_ATTEMPTS - 1;
                
                if should_log {
                    error!(
                        "Failed to start {}: {}. Retrying in {} seconds (attempt #{}/{})...",
                        config.camera_cmd,
                        e,
                        delay.as_secs(),
                        retry_attempt + 1,
                        MAX_RETRY_ATTEMPTS
                    );
                } else {
                    // Use debug level for suppressed logs
                    debug!(
                        "Failed to start {} (attempt #{}/{}), retrying in {}s",
                        config.camera_cmd,
                        retry_attempt + 1,
                        MAX_RETRY_ATTEMPTS,
                        delay.as_secs()
                    );
                }
                
                tokio::time::sleep(delay).await;
                retry_attempt = retry_attempt.saturating_add(1);
                continue;
            }
        };

        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                error!("Failed to capture stdout from {}", config.camera_cmd);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        
        // Spawn task to capture and log stderr for debugging
        if let Some(stderr) = child.stderr.take() {
            let camera_cmd = config.camera_cmd.clone();
            tokio::spawn(async move {
                let mut stderr_reader = BufReader::new(stderr);
                let mut line_buffer = Vec::new();
                loop {
                    match stderr_reader.read_until(b'\n', &mut line_buffer).await {
                        Ok(0) => break, // EOF
                        Ok(_) => {
                            let line = String::from_utf8_lossy(&line_buffer).trim().to_string();
                            if !line.is_empty() {
                                tracing::trace!("{} stderr: {}", camera_cmd, line);
                            }
                            line_buffer.clear();
                        }
                        Err(e) => {
                            tracing::trace!("Error reading {} stderr: {}", camera_cmd, e);
                            break;
                        }
                    }
                }
            });
        }
        
        // Wrap stdout with BufReader to reduce syscalls
        let mut stdout = BufReader::with_capacity(64 * 1024, stdout);

        let mut buffer = BytesMut::with_capacity(1024 * 1024);
        let mut partial_frame = BytesMut::new();
        let read_buf_size = config.read_buffer_kb * 1024;
        let mut temp = vec![0u8; read_buf_size];

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    debug!("Camera reader received shutdown signal");
                    let _ = child.kill().await;
                    break;
                }
                result = stdout.read(&mut temp) => {
                    match result {
                        Ok(0) => {
                            info!("{} process ended, will restart...", config.camera_cmd);
                            break;
                        }
                        Ok(n) => {
                            buffer.extend_from_slice(&temp[..n]);

                            let max_buffer_size = config.max_camera_buffer_mb * 1_000_000;
                            if buffer.len() > max_buffer_size {
                                warn!(
                                    "Frame buffer exceeding {:.1}MB ({} bytes), clearing...",
                                    config.max_camera_buffer_mb,
                                    buffer.len()
                                );
                                buffer.clear();
                                partial_frame.clear();
                                continue;
                            }

                            let frames = find_jpeg_frames(&mut buffer, &mut partial_frame, max_partial_size);

                            for jpeg_bytes in frames {
                                frame_sequence = frame_sequence.wrapping_add(1);
                                let frame = Arc::new(FrameData::new_frame(jpeg_bytes, frame_sequence));

                                // Lock-free update using ArcSwap
                                latest_frame.store(Some(Arc::clone(&frame)));

                                // Sliding reset window: reset retry counter after sustained success
                                if let Some(start_time) = camera_start_time {
                                    if retry_attempt > 0 && start_time.elapsed().as_secs() >= reset_window_secs {
                                        debug!("Camera has run successfully for {}s, resetting retry counter from {} to 0", 
                                               reset_window_secs, retry_attempt);
                                        retry_attempt = 0;
                                        camera_start_time = None; // Prevent repeated resets
                                    }
                                }

                                if frame_tx.send(frame).is_err() {
                                    // Reduced logging frequency for hot path
                                    if frame_sequence % 100 == 0 {
                                        tracing::trace!("No active receivers for {} recent frames", frame_sequence);
                                    }
                                }
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                            debug!("Interrupted reading from {}, continuing", config.camera_cmd);
                            continue;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            debug!("Would block reading from {}, continuing", config.camera_cmd);
                            continue;
                        }
                        Err(e) => {
                            error!("Error reading from {}: {}", config.camera_cmd, e);
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Periodic check for shutdown
                }
            }
        }

        let _ = child.kill().await;
        let _ = child.wait().await;

        if !*shutdown_rx.borrow() {
            if retry_attempt >= MAX_RETRY_ATTEMPTS {
                error!(
                    "Camera process {} failed too many times ({}). Giving up.",
                    config.camera_cmd,
                    MAX_RETRY_ATTEMPTS
                );
                break;
            }
            let delay = calculate_backoff(retry_attempt);
            
            // Apply same log suppression pattern for restarts
            let should_log = retry_attempt < 3 || 
                retry_attempt == 5 || 
                retry_attempt == 10 || 
                retry_attempt == 15 ||
                retry_attempt == MAX_RETRY_ATTEMPTS - 1;
            
            if should_log {
                info!(
                    "Restarting {} in {} seconds (attempt #{}/{})...",
                    config.camera_cmd,
                    delay.as_secs(),
                    retry_attempt + 1,
                    MAX_RETRY_ATTEMPTS
                );
            } else {
                debug!(
                    "Restarting {} (attempt #{}/{}), waiting {}s",
                    config.camera_cmd,
                    retry_attempt + 1,
                    MAX_RETRY_ATTEMPTS,
                    delay.as_secs()
                );
            }
            tokio::time::sleep(delay).await;
            retry_attempt = retry_attempt.saturating_add(1);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(Config::parse());
    
    // Validate configuration
    if let Err(e) = validate_config(&config) {
        eprintln!("Configuration error: {}", e);
        std::process::exit(1);
    }

    // Initialize tracing
    // Check for RUST_LOG env var first, then fall back to config
    let filter = if std::env::var("RUST_LOG").is_ok() {
        // Use RUST_LOG if set
        tracing_subscriber::EnvFilter::from_default_env()
    } else {
        // Use config-based filter with module path
        let log_level = if config.verbose {
            "debug"
        } else {
            &config.log_level
        };
        // Use module_path! to be robust against binary renames
        let module_name = module_path!().split("::").next().unwrap_or("pi_cam_bridge");
        tracing_subscriber::EnvFilter::new(format!("{}={},hyper=warn", module_name, log_level))
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();

    let stats = Arc::new(ServerStats::new());
    let latest_frame = Arc::new(ArcSwapOption::from(None));
    let (frame_tx, _) = broadcast::channel::<Arc<FrameData>>(config.buffer_size);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let client_limiter = Arc::new(Semaphore::new(config.max_clients));

    // Spawn camera reader
    let camera_config = Arc::clone(&config);
    let camera_tx = frame_tx.clone();
    let camera_latest = Arc::clone(&latest_frame);
    let camera_shutdown_rx = shutdown_rx.clone();

    tokio::spawn(async move {
        spawn_and_read_camera(camera_config, camera_tx, camera_latest, camera_shutdown_rx).await;
    });

    // Spawn stats reporter
    if config.stats_interval > 0 {
        let stats_clone = Arc::clone(&stats);
        let stats_interval = config.stats_interval;
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(stats_interval));
            ticker.tick().await;

            loop {
                ticker.tick().await;
                let active = stats_clone.active_connections.load(Ordering::Relaxed);
                let total = stats_clone.total_connections.load(Ordering::Relaxed);
                let frames = stats_clone.frames_served.load(Ordering::Relaxed);
                let bytes = stats_clone.bytes_sent.load(Ordering::Relaxed);
                let rejected = stats_clone.rejected_connections.load(Ordering::Relaxed);

                info!(
                    "Stats: {} active, {} total, {} rejected connections | {} frames served | {:.2} MB sent",
                    active,
                    total,
                    rejected,
                    frames,
                    bytes as f64 / 1_048_576.0
                );
            }
        });
    }


    // Setup server
    let addr: SocketAddr = format!("{}:{}", config.address, config.port).parse()?;
    let listener = TcpListener::bind(addr).await?;

    info!("Pi Camera Bridge v{} running at http://{}/", VERSION, addr);
    info!("Configuration:");
    info!("  Max clients: {}", config.max_clients);
    info!("  Buffer size: {} frames", config.buffer_size);
    info!("  Camera: {} {}", config.camera_cmd, config.camera_args);
    info!("Available endpoints:");
    info!("  / or /stream - MJPEG stream");
    info!("  /stats       - Server statistics (JSON)");
    info!("  /snapshot    - Single JPEG frame");
    info!("Press Ctrl+C to shutdown gracefully");

    // Setup shutdown handler for multiple signals
    let shutdown_signal = async {
        let ctrl_c = async {
            signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
            let mut sighup = signal(SignalKind::hangup()).expect("Failed to install SIGHUP handler");
            
            tokio::select! {
                _ = sigterm.recv() => info!("Received SIGTERM, shutting down..."),
                _ = sighup.recv() => info!("Received SIGHUP, shutting down..."),
            }
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {
                info!("Received Ctrl+C, shutting down...");
            }
            _ = terminate => {
                // Already logged in terminate branch
            }
        }
        
        if let Err(e) = shutdown_tx.send(true) {
            error!("Failed to send shutdown signal: {}", e);
        }
    };

    // Main server loop with shutdown handling
    tokio::select! {
        _ = shutdown_signal => {},
        result = serve_connections(listener, frame_tx, latest_frame, stats, client_limiter, config, shutdown_rx) => {
            if let Err(e) = result {
                error!("Server error: {}", e);
            }
        }
    }

    info!("Server shutdown complete");
    Ok(())
}

async fn serve_connections(
    listener: TcpListener,
    frame_tx: broadcast::Sender<Arc<FrameData>>,
    latest_frame: Arc<ArcSwapOption<FrameData>>,
    stats: Arc<ServerStats>,
    client_limiter: Arc<Semaphore>,
    config: Arc<Config>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                debug!("Stopped accepting new connections");
                break;
            }
            accept_result = listener.accept() => {
                let (tcp, addr) = match accept_result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                        continue;
                    }
                };

                // Try to acquire permit before processing
                let permit = match client_limiter.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        stats.rejected_connections.fetch_add(1, Ordering::Relaxed);
                        warn!("Rejecting connection from {} - max clients ({}) reached", addr, config.max_clients);
                        drop(tcp); // Properly close the connection
                        continue;
                    }
                };

                stats.total_connections.fetch_add(1, Ordering::Relaxed);
                stats.active_connections.fetch_add(1, Ordering::Relaxed);

                let active = stats.active_connections.load(Ordering::Relaxed);
                debug!("New connection from: {} (active: {})", addr, active);

                let io = TokioIo::new(tcp);
                let frame_tx = frame_tx.clone();
                let latest_frame = Arc::clone(&latest_frame);
                let stats = Arc::clone(&stats);
                let config_clone = Arc::clone(&config);
                let shutdown_rx = shutdown_rx.clone();

                tokio::task::spawn(async move {
                    let stats_for_handler = Arc::clone(&stats);
                    let request_buf_size = config_clone.request_buffer_kb * 1024;
                    let peer_addr = addr; // Capture peer address for secure passing
                    let result = http1::Builder::new()
                        .max_buf_size(request_buf_size) // Configurable request size limit
                        .serve_connection(
                            io,
                            service_fn(move |mut req| {
                                // Securely pass peer address via extensions (not spoofable like headers)
                                req.extensions_mut().insert(peer_addr);
                                handle_request(
                                    req,
                                    frame_tx.clone(),
                                    Arc::clone(&latest_frame),
                                    Arc::clone(&stats_for_handler),
                                    config_clone.client_timeout_secs,
                                    shutdown_rx.clone(),
                                )
                            }),
                        )
                        .await;

                    if let Err(err) = result {
                        if !err.to_string().contains("connection reset") {
                            debug!("Error serving connection from {}: {:?}", addr, err);
                        }
                    }

                    drop(permit); // Release the client slot
                    stats.active_connections.fetch_sub(1, Ordering::Relaxed);
                    let active = stats.active_connections.load(Ordering::Relaxed);
                    debug!("Connection closed: {} (active: {})", addr, active);
                });
            }
        }
    }

    Ok(())
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    frame_tx: broadcast::Sender<Arc<FrameData>>,
    latest_frame: Arc<ArcSwapOption<FrameData>>,
    stats: Arc<ServerStats>,
    client_timeout_secs: u64,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    // Get peer address from secure request extensions (not spoofable like headers)
    let client_addr = req
        .extensions()
        .get::<SocketAddr>()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    debug!(
        "Handling {} request from {} to {}",
        req.method(),
        client_addr,
        req.uri().path()
    );
    let path = req.uri().path();

    if path == "/stats" {
        return serve_stats(stats).await;
    }

    if path == "/snapshot" {
        return serve_snapshot(latest_frame).await;
    }

    if path.starts_with("/stream") || path == "/" {
        return serve_mjpeg_stream(frame_tx, stats, client_timeout_secs, shutdown_rx).await;
    }

    let body = BodyExt::boxed(StreamBody::new(stream::once(async {
        Ok::<_, Infallible>(Frame::data(Bytes::from(
            "404 - Not Found\n\nAvailable endpoints:\n\
                / or /stream - MJPEG stream\n\
                /snapshot    - Single JPEG frame\n\
                /stats       - Server statistics",
        )))
    })));

    Ok(build_response(
        StatusCode::NOT_FOUND,
        "text/plain",
        body,
        &[],
    ))
}

async fn serve_stats(
    stats: Arc<ServerStats>,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    let active = stats.active_connections.load(Ordering::Relaxed);
    let total = stats.total_connections.load(Ordering::Relaxed);
    let frames = stats.frames_served.load(Ordering::Relaxed);
    let bytes = stats.bytes_sent.load(Ordering::Relaxed);
    let rejected = stats.rejected_connections.load(Ordering::Relaxed);

    let json = format!(
        r#"{{"version":"{}","active_connections":{},"total_connections":{},"rejected_connections":{},"frames_served":{},"bytes_sent":{}}}"#,
        VERSION,
        active,
        total,
        rejected,
        frames,
        bytes
    );

    let body = BodyExt::boxed(StreamBody::new(stream::once(async {
        Ok::<_, Infallible>(Frame::data(Bytes::from(json)))
    })));

    Ok(build_response(
        StatusCode::OK,
        "application/json",
        body,
        &[("Cache-Control", "no-cache")],
    ))
}

fn build_response(
    status: StatusCode,
    content_type: &str,
    body: BoxBody<Bytes, Infallible>,
    extra_headers: &[(&str, &str)],
) -> Response<BoxBody<Bytes, Infallible>> {
    let mut builder = Response::builder()
        .status(status)
        .header("Server", format!("pi-stream/{}", VERSION))
        .header("Content-Type", content_type);

    for (key, value) in extra_headers {
        builder = builder.header(*key, *value);
    }

    builder.body(body).unwrap()
}

async fn serve_snapshot(
    latest_frame: Arc<ArcSwapOption<FrameData>>,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    // Lock-free read using ArcSwap
    let frame_data = latest_frame.load().as_ref().map(|f| f.jpeg.clone());

    match frame_data {
        Some(jpeg_bytes) => {
            let body = BodyExt::boxed(StreamBody::new(stream::once(async move {
                Ok::<_, Infallible>(Frame::data(jpeg_bytes))
            })));

            Ok(build_response(
                StatusCode::OK,
                "image/jpeg",
                body,
                &[("Cache-Control", "no-cache")],
            ))
        }
        None => {
            let body = BodyExt::boxed(StreamBody::new(stream::once(async {
                Ok::<_, Infallible>(Frame::data(Bytes::from("No frame available yet")))
            })));

            Ok(build_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "text/plain",
                body,
                &[("Cache-Control", "no-cache")],
            ))
        }
    }
}

async fn serve_mjpeg_stream(
    frame_tx: broadcast::Sender<Arc<FrameData>>,
    stats: Arc<ServerStats>,
    client_timeout_secs: u64,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    debug!("Starting MJPEG stream for client");
    // Generate random boundary per connection for RFC 2046 compliance
    let boundary = format!("frame{:016x}", rand::random::<u64>());
    let rx = frame_tx.subscribe();
    let client_stats = Arc::new(ClientStats::new());

    // Pre-allocate reusable buffer for headers
    let header_buf = BytesMut::with_capacity(256);

    let stream = stream::unfold(
        (rx, stats.clone(), header_buf, client_stats.clone(), boundary.clone(), shutdown_rx.clone()),
        move |(mut rx, stats, mut buf, client_stats, boundary, shutdown_rx)| async move {
            loop {
                // Check for shutdown before recv
                if *shutdown_rx.borrow() {
                    client_stats.flush_to_global(&stats);
                    return None;
                }

                match rx.recv().await {
                    Ok(frame) => {
                        // Reuse buffer for headers
                        buf.clear();
                        buf.extend_from_slice(
                            format!(
                                "--{}\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\nX-Frame-Number: {}\r\n\r\n",
                                boundary,
                                frame.jpeg.len(),
                                frame.sequence
                            )
                            .as_bytes(),
                        );

                        // Combine header and frame data efficiently
                        let mut data = BytesMut::with_capacity(buf.len() + frame.jpeg.len() + 2);
                        data.extend_from_slice(&buf);
                        data.extend_from_slice(&frame.jpeg);
                        data.extend_from_slice(b"\r\n");

                        // Update client stats (lock-free)
                        client_stats.add_frame(data.len());
                        client_stats.should_flush_to_global(&stats); // Auto-flush if needed

                        return Some((data.freeze(), (rx, stats, buf, client_stats, boundary, shutdown_rx)));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Use trace for hot-path lag messages to reduce log noise
                        tracing::trace!(
                            "Client lagged by {} frames due to slow processing, continuing...",
                            n
                        );
                        // Continue the loop to try to get the next frame instead of ending stream
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("Frame broadcast channel closed, ending stream");
                        // Final flush of client stats
                        client_stats.flush_to_global(&stats);
                        return None;
                    }
                }
            }
        },
    );

    // Pin the stream to satisfy the Unpin requirement
    let pinned_stream = Box::pin(stream);
    
    // Wrap stream with timeout and proper cleanup
    let timeout_stream = stream::unfold(
        (pinned_stream, stats.clone(), client_stats.clone()),
        move |(mut stream, stats, client_stats)| async move {
            match tokio::time::timeout(Duration::from_secs(client_timeout_secs), stream.next()).await {
                Ok(Some(bytes)) => {
                    // Periodic flush check for slow clients (every 10 seconds of stale stats)
                    client_stats.force_flush_if_stale(&stats, 10_000);
                    Some((Ok::<_, Infallible>(Frame::data(bytes)), (stream, stats, client_stats)))
                }
                Ok(None) => {
                    // Stream ended normally, stats already flushed
                    None
                }
                Err(_) => {
                    warn!("Client write timeout - terminating slow connection");
                    // Flush stats before terminating
                    client_stats.flush_to_global(&stats);
                    None // Properly terminate the stream
                }
            }
        }
    );
    let body = BodyExt::boxed(StreamBody::new(timeout_stream));

    let builder = Response::builder()
        .status(StatusCode::OK)
        .header("Server", format!("pi-stream/{}", VERSION))
        .header(
            "Content-Type",
            format!("multipart/x-mixed-replace; boundary={}", boundary),
        )
        .header("Cache-Control", "no-cache, no-store, must-revalidate")
        .header("Pragma", "no-cache")
        .header("Expires", "0")
        .header("Connection", "keep-alive");

    Ok(builder.body(body).unwrap())
}
