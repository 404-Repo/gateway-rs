use salvo::http::StatusCode;
use salvo::prelude::*;

const TOO_MANY_REQUESTS_HTML: &str = include_str!("responses/429.html");
const METHOD_NOT_ALLOWED_HTML: &str = include_str!("responses/405.html");

#[handler]
pub async fn custom_response(
    _req: &Request,
    _depot: &Depot,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) {
    if let Some(StatusCode::TOO_MANY_REQUESTS) = res.status_code {
        res.render(Text::Html(TOO_MANY_REQUESTS_HTML));
        res.status_code(StatusCode::TOO_MANY_REQUESTS);
        ctrl.skip_rest();
        return;
    }
    if let Some(StatusCode::METHOD_NOT_ALLOWED) = res.status_code {
        res.render(Text::Html(METHOD_NOT_ALLOWED_HTML));
        res.status_code(StatusCode::METHOD_NOT_ALLOWED);
        ctrl.skip_rest();
        return;
    }
}
