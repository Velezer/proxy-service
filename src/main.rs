use std::{convert::Infallible, env, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::service::service_fn;
use hyper::{
    body::Incoming,
    header::{HeaderValue, HOST, CONTENT_TYPE},
    http::uri::PathAndQuery,
    Request, Response, StatusCode, Uri,
};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use tokio::net::TcpListener;

type RespBody = BoxBody<Bytes, hyper::Error>;

/// Shared application state passed to every request handler.
///
/// `scheme` and `authority` are pre-extracted from `TARGET_BASE` at startup so
/// that the hot-path `proxy` function never needs to call `.expect()` on them.
///
/// The `Client` is parameterised over `Incoming` as its *request* body type
/// because we forward the raw incoming body directly to the upstream without
/// buffering it.
#[derive(Clone)]
struct AppState {
    /// Full target URI (kept for reference / future use).
    target: Uri,
    /// Scheme string extracted from `target` (e.g. `"http"` or `"https"`).
    scheme: String,
    /// Authority string extracted from `target` (e.g. `"example.com:443"`).
    authority: String,
    /// `Host` header value derived from `authority`.
    host_header: HeaderValue,
    /// HTTP client used to forward requests to the upstream.
    ///
    /// # Body-type note
    /// The body type parameter here is `Incoming` — the same type that arrives
    /// from the downstream TCP connection — because we stream the request body
    /// straight through to the upstream without buffering or transforming it.
    /// If you ever need to send a *synthesised* request (e.g. for retries or
    /// health-checks) you should use a separate `Client<HttpConnector,
    /// Full<Bytes>>` instance.
    client: Client<HttpConnector, Incoming>,
}

#[tokio::main]
async fn main() {
    let listen_addr: SocketAddr = env::var("LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_owned())
        .parse()
        .expect("LISTEN_ADDR must be a valid host:port");

    let target = env::var("TARGET_BASE")
        .unwrap_or_else(|_| "http://example.com".to_owned())
        .parse::<Uri>()
        .expect("TARGET_BASE must be a valid absolute URI");

    // Validate and extract scheme / authority *once* at startup so the
    // per-request hot path never has to call `.expect()` on them.
    let scheme = target
        .scheme_str()
        .expect("TARGET_BASE must include a scheme (e.g. http:// or https://)")
        .to_owned();

    let authority = target
        .authority()
        .expect("TARGET_BASE must include an authority (e.g. example.com)")
        .as_str()
        .to_owned();

    let host_header =
        HeaderValue::from_str(&authority).expect("TARGET_BASE authority is not a valid header value");

    let mut connector = HttpConnector::new();
    // Allow non-HTTP schemes to pass through the connector (needed when the
    // upstream is reached via a tunnel or when `enforce_http` would otherwise
    // reject `https` URIs before TLS is negotiated at a higher layer).
    connector.enforce_http(false);

    let state = Arc::new(AppState {
        target,
        scheme,
        authority,
        host_header,
        client: Client::builder(TokioExecutor::new()).build(connector),
    });

    let listener = TcpListener::bind(listen_addr)
        .await
        .expect("failed to bind LISTEN_ADDR");

    eprintln!("proxy listening on http://{listen_addr}");

    loop {
        // A single failed `accept` (e.g. "too many open files") must not bring
        // down the whole server — log it and keep looping.
        let (stream, peer_addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                eprintln!("accept error: {err}");
                continue;
            }
        };

        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let service = service_fn(move |req| proxy(req, state.clone()));
            if let Err(err) = Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await
            {
                eprintln!("connection error from {peer_addr}: {err}");
            }
        });
    }
}

/// Forward an incoming request to the configured upstream and return its
/// response.  All errors are translated into appropriate HTTP error responses
/// so that the function signature remains `Result<_, Infallible>`.
async fn proxy(
    mut req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<RespBody>, Infallible> {
    // Preserve the original path and query string; fall back to "/" if absent.
    let path_and_query = req
        .uri()
        .path_and_query()
        .cloned()
        .unwrap_or_else(|| PathAndQuery::from_static("/"));

    // Build the upstream URI by combining the pre-validated scheme/authority
    // from `AppState` with the path/query from the incoming request.
    let upstream_uri = match Uri::builder()
        .scheme(state.scheme.as_str())
        .authority(state.authority.as_str())
        .path_and_query(path_and_query)
        .build()
    {
        Ok(uri) => uri,
        Err(err) => {
            eprintln!("failed to build upstream URI: {err}");
            return Ok(error_response(
                StatusCode::BAD_REQUEST,
                "invalid request URI",
            ));
        }
    };

    *req.uri_mut() = upstream_uri;
    req.headers_mut().insert(HOST, state.host_header.clone());

    match state.client.request(req).await {
        Ok(res) => Ok(res.map(|body| body.boxed())),
        Err(err) => {
            eprintln!("upstream error: {err}");
            Ok(error_response(
                StatusCode::BAD_GATEWAY,
                format!("upstream error: {err}"),
            ))
        }
    }
}

/// Build a plain-text error response with the given status code and body.
fn error_response(status: StatusCode, body: impl Into<Bytes>) -> Response<RespBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(
            Full::new(body.into())
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap_or_else(|_| {
            // Constructing the response failed (should be unreachable), so
            // return a bare empty response rather than panicking.
            Response::new(Empty::new().map_err(|never| match never {}).boxed())
        })
}
