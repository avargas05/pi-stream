use bytes::BytesMut;
use pi_cam_bridge::*;

const JPEG_SOI: [u8; 2] = [0xFF, 0xD8];
const JPEG_EOI: [u8; 2] = [0xFF, 0xD9];

fn create_test_jpeg(size: usize) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&JPEG_SOI);
    
    // Add some dummy JPEG data (avoiding EOI patterns)
    for i in 0..(size - 4) {
        frame.push((i % 200 + 1) as u8); // Avoid 0xFF patterns
    }
    
    frame.extend_from_slice(&JPEG_EOI);
    frame
}

fn create_malformed_jpeg(has_soi: bool, has_eoi: bool, size: usize) -> Vec<u8> {
    let mut frame = Vec::new();
    
    if has_soi {
        frame.extend_from_slice(&JPEG_SOI);
    }
    
    for i in 0..(size - if has_soi && has_eoi { 4 } else if has_soi || has_eoi { 2 } else { 0 }) {
        frame.push((i % 200 + 1) as u8);
    }
    
    if has_eoi {
        frame.extend_from_slice(&JPEG_EOI);
    }
    
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_single_complete_frame() {
        let test_frame = create_test_jpeg(1000);
        let mut buffer = BytesMut::from(test_frame.as_slice());
        let mut partial = BytesMut::new();
        
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].len(), 1000);
        assert!(buffer.is_empty());
        assert!(partial.is_empty());
    }
    
    #[test]
    fn test_multiple_complete_frames() {
        let frame1 = create_test_jpeg(500);
        let frame2 = create_test_jpeg(750);
        let frame3 = create_test_jpeg(300);
        
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&frame1);
        buffer.extend_from_slice(&frame2);
        buffer.extend_from_slice(&frame3);
        
        let mut partial = BytesMut::new();
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].len(), 500);
        assert_eq!(frames[1].len(), 750);
        assert_eq!(frames[2].len(), 300);
        assert!(buffer.is_empty());
        assert!(partial.is_empty());
    }
    
    #[test]
    fn test_partial_frame_saved() {
        let incomplete_frame = create_malformed_jpeg(true, false, 800);
        let mut buffer = BytesMut::from(incomplete_frame.as_slice());
        let mut partial = BytesMut::new();
        
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(partial.len(), 800);
        
        // Now complete the frame
        buffer.extend_from_slice(&JPEG_EOI);
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].len(), 802); // 800 + 2 for EOI
        assert!(buffer.is_empty());
        assert!(partial.is_empty());
    }
    
    #[test]
    fn test_partial_frame_discarded_when_too_large() {
        let large_partial = create_malformed_jpeg(true, false, 6_000_000); // > 5MB limit
        let mut buffer = BytesMut::from(large_partial.as_slice());
        let mut partial = BytesMut::new();
        
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 0);
        assert!(buffer.is_empty());
        assert!(partial.is_empty()); // Should be discarded due to size
    }
    
    #[test]
    fn test_mixed_complete_and_partial_frames() {
        let complete_frame = create_test_jpeg(400);
        let partial_frame = create_malformed_jpeg(true, false, 600);
        
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&complete_frame);
        buffer.extend_from_slice(&partial_frame);
        
        let mut partial = BytesMut::new();
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].len(), 400);
        assert!(buffer.is_empty());
        assert_eq!(partial.len(), 600);
    }
    
    #[test]
    fn test_back_to_back_frames() {
        // Create frames with shared EOI/SOI boundary (EOI of frame1 = SOI of frame2)
        let mut frame1 = create_test_jpeg(300);
        frame1.pop(); frame1.pop(); // Remove EOI
        
        let frame2 = create_test_jpeg(400);
        
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&frame1);
        buffer.extend_from_slice(&JPEG_EOI); // End frame1
        buffer.extend_from_slice(&frame2);   // Start frame2 immediately
        
        let mut partial = BytesMut::new();
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].len(), 300);
        assert_eq!(frames[1].len(), 400);
        assert!(buffer.is_empty());
        assert!(partial.is_empty());
    }
    
    #[test]
    fn test_corrupted_data_without_markers() {
        let corrupted_data = vec![0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56];
        let mut buffer = BytesMut::from(corrupted_data.as_slice());
        let mut partial = BytesMut::new();
        
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 0);
        assert!(buffer.is_empty()); // Should be consumed
        assert!(partial.is_empty());
    }
    
    #[test]
    fn test_soi_without_eoi() {
        let mut data = Vec::new();
        data.extend_from_slice(&JPEG_SOI);
        data.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]);
        
        let mut buffer = BytesMut::from(data.as_slice());
        let mut partial = BytesMut::new();
        
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 0);
        assert!(buffer.is_empty());
        assert_eq!(partial.len(), 6); // SOI + 4 bytes of data
    }
    
    #[test]
    fn test_eoi_without_soi() {
        let mut data = Vec::new();
        data.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]);
        data.extend_from_slice(&JPEG_EOI);
        
        let mut buffer = BytesMut::from(data.as_slice());
        let mut partial = BytesMut::new();
        
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 0);
        assert!(buffer.is_empty());
        assert!(partial.is_empty());
    }
    
    #[test]
    fn test_minimum_buffer_size() {
        let mut buffer = BytesMut::from(&[0xFF, 0xD8][..]);
        let mut partial = BytesMut::new();
        
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 0);
        assert_eq!(buffer.len(), 2); // Too small, not processed
        assert!(partial.is_empty());
    }
    
    #[test]
    fn test_frame_boundary_edge_cases() {
        // Test frame that ends exactly at buffer boundary
        let frame = create_test_jpeg(100);
        let mut buffer = BytesMut::from(frame.as_slice());
        let mut partial = BytesMut::new();
        
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].len(), 100);
        assert!(buffer.is_empty());
        assert!(partial.is_empty());
    }
    
    #[test]
    fn test_multiple_soi_markers() {
        // Create data with multiple SOI markers but only one complete frame
        let mut data = Vec::new();
        data.extend_from_slice(&JPEG_SOI); // First SOI
        data.extend_from_slice(&[0x12, 0x34]);
        data.extend_from_slice(&JPEG_SOI); // Second SOI (should start new frame)
        data.extend_from_slice(&[0x56, 0x78]);
        data.extend_from_slice(&JPEG_EOI); // Complete the second frame
        
        // The data is: [0xFF, 0xD8, 0x12, 0x34, 0xFF, 0xD8, 0x56, 0x78, 0xFF, 0xD9]
        // Expected behavior: First SOI at 0, second SOI at 4, EOI at 8
        // Should extract from first SOI (0) to EOI (8+2=10), total 10 bytes
        
        let mut buffer = BytesMut::from(data.as_slice());
        let mut partial = BytesMut::new();
        
        let frames = find_jpeg_frames(&mut buffer, &mut partial, 5_000_000);
        
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].len(), 10); // All data from first SOI to EOI
        assert!(buffer.is_empty());
        assert!(partial.is_empty());
    }
    
    #[test]
    fn test_performance_with_large_buffer() {
        // Test with a large buffer containing multiple frames
        let mut large_buffer = BytesMut::new();
        
        // Add 100 frames of varying sizes
        for i in 0..100 {
            let frame = create_test_jpeg(100 + (i * 10));
            large_buffer.extend_from_slice(&frame);
        }
        
        let mut partial = BytesMut::new();
        let start = std::time::Instant::now();
        
        let frames = find_jpeg_frames(&mut large_buffer, &mut partial, 5_000_000);
        
        let duration = start.elapsed();
        
        assert_eq!(frames.len(), 100);
        assert!(large_buffer.is_empty());
        assert!(partial.is_empty());
        
        // Should be significantly faster than 1ms for memchr optimization
        assert!(duration.as_millis() < 10, "Frame parsing took too long: {:?}", duration);
    }
}