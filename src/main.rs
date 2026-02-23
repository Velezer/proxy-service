use std::{convert::Infallible, env, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::service::service_fn;
use hyper::{
    body::Incoming,
    header::{HeaderValue, HOST},
    http::uri::PathAndQuery,
    Request, Response, StatusCode, Uri,
};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use tokio::net::TcpListener;

type RespBody = BoxBody<Bytes, Infallible>;

#[derive(Clone)]
struct AppState {
    target: Uri,
    host_header: HeaderValue,
    client: Client<HttpConnector, Incoming>,
}

#[tokio::main]
async fn main() {
    let listen_addr: SocketAddr = env::var("LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_owned())
        .parse()
        .expect("LISTEN_ADDR must be host:port");

    let target = env::var("TARGET_BASE")
        .unwrap_or_else(|_| "https://example.com".to_owned())
        .parse::<Uri>()
        .expect("TARGET_BASE must be a valid absolute URI");

    let authority = target
        .authority()
        .expect("TARGET_BASE must include authority")
        .as_str();
    let host_header = HeaderValue::from_str(authority).expect("invalid authority in TARGET_BASE");

    let mut connector = HttpConnector::new();
    connector.enforce_http(false);

    let state = Arc::new(AppState {
        target,
        host_header,
        client: Client::builder(TokioExecutor::new()).build(connector),
    });

    let listener = TcpListener::bind(listen_addr)
        .await
        .expect("failed to bind LISTEN_ADDR");

    println!("proxy listening on http://{listen_addr}");

    loop {
        let (stream, _) = listener.accept().await.expect("accept failed");
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let service = service_fn(move |req| proxy(req, state.clone()));
            if let Err(err) = Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await
            {
                eprintln!("connection error: {err}");
            }
        });
    }
}

async fn proxy(
    mut req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<RespBody>, Infallible> {
    let path_and_query = req
        .uri()
        .path_and_query()
        .cloned()
        .unwrap_or_else(|| PathAndQuery::from_static("/"));

    let uri = Uri::builder()
        .scheme(
            state
                .target
                .scheme_str()
                .expect("TARGET_BASE needs a scheme"),
        )
        .authority(
            state
                .target
                .authority()
                .expect("TARGET_BASE needs authority")
                .as_str(),
        )
        .path_and_query(path_and_query)
        .build();

    let Ok(uri) = uri else {
        return Ok(simple_response(
            StatusCode::BAD_REQUEST,
            "invalid request URI",
        ));
    };

    *req.uri_mut() = uri;
    req.headers_mut().insert(HOST, state.host_header.clone());

    match state.client.request(req).await {
        Ok(res) => Ok(res.map(|body| body.boxed())),
        Err(err) => Ok(simple_response(
            StatusCode::BAD_GATEWAY,
            format!("upstream error: {err}"),
        )),
    }
}

fn simple_response(status: StatusCode, body: impl Into<Bytes>) -> Response<RespBody> {
    Response::builder()
        .status(status)
        .body(Full::new(body.into()).boxed())
        .unwrap_or_else(|_| Response::new(Empty::new().boxed()))
}
