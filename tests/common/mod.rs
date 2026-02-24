use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;
use image::{ImageFormat, Rgba, RgbaImage};
use salvo::prelude::Response;

pub(crate) fn tiny_png_bytes() -> Vec<u8> {
    let img = RgbaImage::from_pixel(1, 1, Rgba([255, 0, 0, 255]));
    let mut cursor = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(img)
        .write_to(&mut cursor, ImageFormat::Png)
        .expect("encode png");
    cursor.into_inner()
}

pub(crate) async fn read_response(res: Response) -> (StatusCode, HeaderMap, Vec<u8>) {
    let hyper_res = res.into_hyper();
    let status = hyper_res.status();
    let headers = hyper_res.headers().clone();
    let body = hyper_res
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    (status, headers, body.to_vec())
}
