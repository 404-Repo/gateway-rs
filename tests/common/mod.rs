pub(crate) mod real_harness;

use http::{HeaderMap, StatusCode};
use image::{ImageFormat, Rgba, RgbaImage};

pub(crate) use real_harness::{
    GatewayHarnessOptions, GatewayRuntimeHarness, TestClient, TestResponse, TestService,
};

pub(crate) fn tiny_png_bytes() -> Vec<u8> {
    let img = RgbaImage::from_pixel(1, 1, Rgba([255, 0, 0, 255]));
    let mut cursor = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(img)
        .write_to(&mut cursor, ImageFormat::Png)
        .expect("encode png");
    cursor.into_inner()
}

pub(crate) async fn read_response(res: TestResponse) -> (StatusCode, HeaderMap, Vec<u8>) {
    (res.status, res.headers, res.body)
}
