use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use multer::Field;
use salvo::prelude::Request;
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::StreamReader;

use crate::http3::error::ServerError;
use crate::http3::handlers::core::{BOUNDARY_PREFIX, MULTIPART_PREFIX};

#[inline(always)]
pub async fn read_text_field(field: Field<'_>, name: &str) -> Result<String, ServerError> {
    field
        .text()
        .await
        .map_err(|e| ServerError::BadRequest(format!("Failed to read {}: {}", name, e)))
}

pub fn is_multipart_form(content_type: &str) -> bool {
    content_type
        .get(..MULTIPART_PREFIX.len())
        .is_some_and(|s| s.eq_ignore_ascii_case(MULTIPART_PREFIX))
}

pub fn parse_boundary(content_type: &str) -> Result<&str, ServerError> {
    content_type
        .split(';')
        .map(|s| s.trim())
        .find(|part| {
            part.get(..BOUNDARY_PREFIX.len())
                .is_some_and(|p| p.eq_ignore_ascii_case(BOUNDARY_PREFIX))
        })
        .and_then(|part| part.split('=').nth(1))
        .ok_or(ServerError::BadRequest(
            "Missing boundary in content-type".into(),
        ))
}

pub fn multipart_stream(
    req: &mut Request,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    let raw_stream = req
        .take_body()
        .into_stream()
        .map_err(|err| std::io::Error::other(format!("Stream error: {}", err)))
        .and_then(|frame| async move {
            frame
                .into_data()
                .map_err(|_| std::io::Error::other("Frame data error".to_string()))
        });

    let stream_reader = StreamReader::new(raw_stream);
    FramedRead::new(stream_reader, BytesCodec::new()).map_ok(|b| b.freeze())
}

fn label_title(label: &str) -> String {
    if label.is_empty() {
        return "Field".to_string();
    }
    let mut bytes = label.as_bytes().to_vec();
    bytes[0] = bytes[0].to_ascii_uppercase();
    String::from_utf8_lossy(&bytes).into_owned()
}

pub async fn read_binary_field(
    mut field: Field<'_>,
    max_bytes: u64,
    label: &str,
) -> Result<Vec<u8>, ServerError> {
    let (lower, upper) = field.size_hint();
    let hinted = upper.or(Some(lower)).filter(|&bytes| bytes > 0);
    let max_bytes_usize = usize::try_from(max_bytes).unwrap_or(usize::MAX);
    let capacity = hinted.unwrap_or(64 * 1024).min(max_bytes_usize);
    let mut content = Vec::with_capacity(capacity);
    let mut total = 0usize;
    let title = label_title(label);

    while let Some(chunk) = field.chunk().await.map_err(|e| {
        if let multer::Error::FieldSizeExceeded { limit, .. } = e {
            ServerError::BadRequest(format!(
                "{} exceeds maximum allowed size ({} bytes)",
                title, limit
            ))
        } else {
            ServerError::BadRequest(format!("Failed to read {} chunk: {}", label, e))
        }
    })? {
        total = total
            .checked_add(chunk.len())
            .ok_or_else(|| ServerError::BadRequest(format!("{} size overflowed usize", title)))?;

        if total > max_bytes_usize {
            return Err(ServerError::BadRequest(format!(
                "{} exceeds maximum allowed size ({} bytes)",
                title, max_bytes
            )));
        }

        content.extend_from_slice(&chunk);
    }

    Ok(content)
}
