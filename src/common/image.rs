use base64_simd::STANDARD;
use bytes::Bytes;
use image::{guess_format, ImageFormat, ImageReader};
use serde::Serializer;
use std::io::Cursor;

use crate::config::ImageConfig;
use crate::http3::error::ServerError;

pub fn serialize_image_base64<S>(image: &Option<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match image {
        None => serializer.serialize_none(),
        Some(bytes) => serializer.serialize_str(&STANDARD.encode_to_string(bytes)),
    }
}

#[derive(Debug, Clone)]
pub struct ImageValidationResult {
    pub width: u32,
    pub height: u32,
    pub format: ImageFormat,
    pub data: Bytes,
}

pub fn validate_image(
    data: Bytes,
    config: &ImageConfig,
) -> Result<ImageValidationResult, ServerError> {
    if (data.len()) > config.max_size_bytes {
        return Err(ServerError::BadRequest(format!(
            "Image too large: {} bytes (max: {} bytes)",
            data.len(),
            config.max_size_bytes
        )));
    }

    let format = guess_format(&data)
        .map_err(|e| ServerError::BadRequest(format!("Failed to guess image format: {}", e)))?;

    let format_name = format_to_str(format);
    if !config
        .allowed_formats
        .contains(&format_name.to_ascii_lowercase())
    {
        return Err(ServerError::BadRequest(format!(
            "Image format '{}' not allowed. Allowed formats: {:?}",
            format_name, config.allowed_formats
        )));
    }

    let reader = ImageReader::new(Cursor::new(&data))
        .with_guessed_format()
        .map_err(|e| ServerError::BadRequest(format!("Failed to read image header: {}", e)))?;
    let (width, height) = reader
        .into_dimensions()
        .map_err(|e| ServerError::BadRequest(format!("Failed to read image dimensions: {}", e)))?;

    if width > config.max_width {
        return Err(ServerError::BadRequest(format!(
            "Image too wide: {}px (max: {}px)",
            width, config.max_width
        )));
    }

    if height > config.max_height {
        return Err(ServerError::BadRequest(format!(
            "Image too tall: {}px (max: {}px)",
            height, config.max_height
        )));
    }

    Ok(ImageValidationResult {
        width,
        height,
        format,
        data,
    })
}

fn format_to_str(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Png => "png",
        ImageFormat::Jpeg => "jpg",
        ImageFormat::WebP => "webp",
        _ => "unknown",
    }
}
