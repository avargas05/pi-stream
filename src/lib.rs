// Re-export functions needed for testing
use bytes::{Bytes, BytesMut};
use memchr::memmem;
use tracing::warn;

pub const JPEG_SOI: [u8; 2] = [0xFF, 0xD8];
pub const JPEG_EOI: [u8; 2] = [0xFF, 0xD9];

pub fn find_jpeg_frames(
    buffer: &mut BytesMut,
    partial: &mut BytesMut,
    max_partial_size: usize,
) -> Vec<Bytes> {
    let mut frames = Vec::new();

    // Prepend any partial frame from previous iteration - avoid copying if possible
    if !partial.is_empty() {
        // Reserve capacity efficiently to avoid multiple allocations
        buffer.reserve(partial.len());
        
        // Move partial data to front of buffer efficiently
        let partial_len = partial.len();
        let buffer_len = buffer.len();
        
        // Extend buffer to make room at the front
        buffer.resize(partial_len + buffer_len, 0);
        
        // Shift existing buffer data to make room for partial
        buffer.copy_within(0..buffer_len, partial_len);
        
        // Copy partial data to front
        buffer[0..partial_len].copy_from_slice(partial);
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
            if partial_data.len() < max_partial_size {
                partial.extend_from_slice(partial_data);
                // Removed debug logging from hot path - use trace if needed
                // trace!("Saved partial frame: {} bytes", partial_data.len());
            } else {
                warn!(
                    "Partial frame exceeds {:.1}MB ({} bytes), discarding",
                    max_partial_size as f64 / 1_000_000.0,
                    partial_data.len()
                );
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