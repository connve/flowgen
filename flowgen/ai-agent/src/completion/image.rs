//! Reads the images sent with a prompt: http and https URLs are fetched from
//! public addresses only, and base64 `data:` URLs are checked like fetched
//! images.

use base64::Engine as _;
use rig_core::message::{ImageDetail, ImageMediaType, MimeType as _, UserContent};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

/// Errors reading an image.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to build the image HTTP client: {source}")]
    Client {
        #[source]
        source: reqwest::Error,
    },
    #[error("Image URL {url:?} is not a valid URL: {source}")]
    InvalidUrl {
        url: String,
        #[source]
        source: url::ParseError,
    },
    #[error("Image URL {url} is refused: {source}")]
    Refused {
        url: String,
        #[source]
        source: Refusal,
    },
    #[error("Failed to fetch image {url}: {source}")]
    Fetch {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("Image at {url} is not a PNG, JPEG, GIF, WebP, HEIC, HEIF or SVG image")]
    NotAnImage { url: String },
    #[error("Image at {url} is larger than {max_bytes} bytes")]
    TooLarge { url: String, max_bytes: usize },
    #[error("Image data URL is not in the form data:<image type>;base64,<data>")]
    InvalidDataUrl,
    #[error("Image data URL holds invalid base64: {source}")]
    DataUrlDecode {
        #[source]
        source: base64::DecodeError,
    },
    #[error(
        "Image data URL has media type {media_type}, not a PNG, JPEG, GIF, WebP, HEIC, HEIF or SVG image"
    )]
    DataNotAnImage { media_type: String },
    #[error("Image data URL is larger than {max_bytes} bytes")]
    DataTooLarge { max_bytes: usize },
}

impl Error {
    /// Whether reading the image again may succeed: transport failures and the
    /// statuses rig retries (408, 425, 429 and 5xx).
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Client { .. } => true,
            Error::Fetch { source, .. } => match source.status() {
                Some(status) => rig_core::error::retryable_status(Some(status.as_u16())),
                None => !(source.is_builder() || source.is_redirect()),
            },
            _ => false,
        }
    }
}

/// Why an image URL, or a redirect it leads to, is not requested.
#[derive(thiserror::Error, Debug, Clone)]
pub enum Refusal {
    #[error("Scheme {0} is not http or https")]
    Scheme(String),
    #[error("Address {0} is not public")]
    Address(IpAddr),
}

/// The client images are fetched with.
pub fn client() -> Result<reqwest::Client, Error> {
    client_builder()
        .build()
        .map_err(|source| Error::Client { source })
}

/// Resolves names to public addresses only and checks every redirect like
/// the first URL; proxies are bypassed, as they would resolve names unchecked.
pub(crate) fn client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .no_proxy()
        .dns_resolver(Arc::new(PublicResolver))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            match check(attempt.url()) {
                Ok(()) => reqwest::redirect::Policy::default().redirect(attempt),
                Err(refusal) => attempt.error(refusal),
            }
        }))
}

/// The image `url` names, as content for the prompt.
pub async fn user_content(
    http: &reqwest::Client,
    url: String,
    timeout: Duration,
    max_bytes: usize,
) -> Result<UserContent, Error> {
    match url.strip_prefix("data:") {
        Some(data) => from_data_url(data, max_bytes),
        None => fetch(http, url, timeout, max_bytes).await,
    }
}

/// Only images are read, so a URL can't pass any other response to the model.
async fn fetch(
    http: &reqwest::Client,
    url: String,
    timeout: Duration,
    max_bytes: usize,
) -> Result<UserContent, Error> {
    let parsed = match reqwest::Url::parse(&url) {
        Ok(parsed) => parsed,
        Err(source) => return Err(Error::InvalidUrl { url, source }),
    };
    if let Err(source) = check(&parsed) {
        return Err(Error::Refused { url, source });
    }
    let failed = |source| fetch_error(&url, source);
    let mut response = http
        .get(parsed)
        .timeout(timeout)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(failed)?;
    let Some(media_type) = response_media_type(&response) else {
        return Err(Error::NotAnImage { url });
    };
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(failed)? {
        if bytes.len() + chunk.len() > max_bytes {
            return Err(Error::TooLarge { url, max_bytes });
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(UserContent::image_base64(
        base64::engine::general_purpose::STANDARD.encode(bytes),
        Some(media_type),
        Some(ImageDetail::Auto),
    ))
}

/// The refusal behind a failed request, when the resolver or a redirect refused an address.
fn fetch_error(url: &str, source: reqwest::Error) -> Error {
    let refusal = std::iter::successors(
        Some(&source as &(dyn std::error::Error + 'static)),
        |error| error.source(),
    )
    .find_map(|error| error.downcast_ref::<Refusal>())
    .cloned();
    match refusal {
        Some(refusal) => Error::Refused {
            url: url.to_string(),
            source: refusal,
        },
        None => Error::Fetch {
            url: url.to_string(),
            source,
        },
    }
}

/// The part of a base64 `data:` URL after `data:`, sent with its media type,
/// since some providers refuse an image without one.
fn from_data_url(data: &str, max_bytes: usize) -> Result<UserContent, Error> {
    let Some((header, payload)) = data.split_once(',') else {
        return Err(Error::InvalidDataUrl);
    };
    let Some(mime) = header.strip_suffix(";base64") else {
        return Err(Error::InvalidDataUrl);
    };
    let Some(media_type) = media_type(mime) else {
        return Err(Error::DataNotAnImage {
            media_type: mime.to_string(),
        });
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|source| Error::DataUrlDecode { source })?;
    if bytes.len() > max_bytes {
        return Err(Error::DataTooLarge { max_bytes });
    }
    Ok(UserContent::image_base64(
        payload.to_string(),
        Some(media_type),
        Some(ImageDetail::Auto),
    ))
}

fn response_media_type(response: &reqwest::Response) -> Option<ImageMediaType> {
    media_type(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)?
            .to_str()
            .ok()?,
    )
}

/// The image type a media type names, ignoring its parameters and case.
fn media_type(mime: &str) -> Option<ImageMediaType> {
    let essence = match mime.split_once(';') {
        Some((essence, _)) => essence,
        None => mime,
    };
    ImageMediaType::from_mime_type(&essence.trim().to_ascii_lowercase())
}

/// Refuses a URL that isn't http or https, or whose host is an address that
/// is not public. Names are checked when they resolve, by [`PublicResolver`].
fn check(url: &reqwest::Url) -> Result<(), Refusal> {
    let address = match (url.scheme(), url.host()) {
        ("http" | "https", Some(url::Host::Ipv4(ip))) => IpAddr::V4(ip),
        ("http" | "https", Some(url::Host::Ipv6(ip))) => IpAddr::V6(ip),
        ("http" | "https", _) => return Ok(()),
        (scheme, _) => return Err(Refusal::Scheme(scheme.to_string())),
    };
    match is_public(address) {
        true => Ok(()),
        false => Err(Refusal::Address(address)),
    }
}

/// Resolves names with the system resolver and keeps only public addresses,
/// so a name can't lead a fetch into the network, however often it is
/// resolved.
struct PublicResolver;

impl reqwest::dns::Resolve for PublicResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(resolve_public(name.as_str().to_string()))
    }
}

async fn resolve_public(
    host: String,
) -> Result<reqwest::dns::Addrs, Box<dyn std::error::Error + Send + Sync>> {
    let resolved: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
    let public: Vec<SocketAddr> = resolved
        .iter()
        .copied()
        .filter(|address| is_public(address.ip()))
        .collect();
    match (public.is_empty(), resolved.first()) {
        (true, Some(address)) => Err(Box::new(Refusal::Address(address.ip()))),
        _ => Ok(Box::new(public.into_iter())),
    }
}

fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

/// Not this network, private, shared, loopback, link-local, protocol
/// assignments, documentation, benchmarking, multicast, reserved or broadcast.
fn is_public_v4(ip: Ipv4Addr) -> bool {
    !matches!(
        ip.octets(),
        [0 | 10 | 127, ..]
            | [100, 64..=127, ..]
            | [169, 254, ..]
            | [172, 16..=31, ..]
            | [192, 0, 0 | 2, _]
            | [192, 168, ..]
            | [198, 18 | 19, ..]
            | [198, 51, 100, _]
            | [203, 0, 113, _]
            | [224..=255, ..]
    )
}

/// IPv4-mapped and NAT64 addresses are judged by the IPv4 address they
/// carry; unspecified, loopback, IPv4-compatible, local-use NAT64, discard,
/// documentation, unique local, link-local and multicast ones are not public.
fn is_public_v6(ip: Ipv6Addr) -> bool {
    match ip.segments() {
        [0, 0, 0, 0, 0, 0xffff, high, low] | [0x64, 0xff9b, 0, 0, 0, 0, high, low] => {
            is_public_v4(Ipv4Addr::from((u32::from(high) << 16) | u32::from(low)))
        }
        [0, 0, 0, 0, 0, 0, _, _]
        | [0x64, 0xff9b, 1, ..]
        | [0x100, 0, 0, 0, ..]
        | [0x2001, 0xdb8, ..]
        | [0xfc00..=0xfdff, ..]
        | [0xfe80..=0xffff, ..] => false,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::Redirect;
    use axum::routing::get;

    async fn image_server() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let inside = format!("http://{address}/a.png");
        let status = |status: axum::http::StatusCode| get(move || async move { status });
        let app = axum::Router::new()
            .route(
                "/a.png",
                get(|| async { ([("content-type", "image/png")], "png") }),
            )
            .route(
                "/a.txt",
                get(|| async { ([("content-type", "text/plain")], "txt") }),
            )
            .route(
                "/same-host",
                get(|| async { Redirect::temporary("/a.png") }),
            )
            .route(
                "/inside",
                get(move || std::future::ready(Redirect::temporary(&inside))),
            )
            .route("/408", status(axum::http::StatusCode::REQUEST_TIMEOUT))
            .route("/425", status(axum::http::StatusCode::TOO_EARLY))
            .route("/429", status(axum::http::StatusCode::TOO_MANY_REQUESTS))
            .route("/503", status(axum::http::StatusCode::SERVICE_UNAVAILABLE))
            .route("/400", status(axum::http::StatusCode::BAD_REQUEST))
            .route("/404", status(axum::http::StatusCode::NOT_FOUND));
        tokio::spawn(async move { axum::serve(listener, app).await });
        address
    }

    async fn read(http: &reqwest::Client, url: String) -> Result<UserContent, Error> {
        user_content(http, url, Duration::from_secs(5), 1024).await
    }

    async fn read_from_server(path: &str) -> Result<UserContent, Error> {
        let address = image_server().await;
        let http = client_builder()
            .resolve("images.test", address)
            .build()
            .unwrap();
        read(
            &http,
            format!("http://images.test:{}{path}", address.port()),
        )
        .await
    }

    fn png(payload: &str) -> UserContent {
        UserContent::image_base64(
            payload.to_string(),
            Some(ImageMediaType::PNG),
            Some(ImageDetail::Auto),
        )
    }

    #[tokio::test]
    async fn an_image_is_sent_as_base64_with_its_media_type() {
        assert_eq!(read_from_server("/a.png").await.unwrap(), png("cG5n"));
    }

    #[tokio::test]
    async fn a_url_that_is_not_an_image_is_not_sent() {
        let error = read_from_server("/a.txt").await.unwrap_err();
        assert!(matches!(error, Error::NotAnImage { .. }), "{error}");
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn an_image_larger_than_the_limit_is_not_sent() {
        let address = image_server().await;
        let http = client_builder()
            .resolve("images.test", address)
            .build()
            .unwrap();
        let url = format!("http://images.test:{}/a.png", address.port());

        let error = user_content(&http, url, Duration::from_secs(5), 2)
            .await
            .unwrap_err();

        assert!(matches!(error, Error::TooLarge { .. }), "{error}");
    }

    #[tokio::test]
    async fn a_redirect_on_the_same_host_is_followed() {
        assert_eq!(read_from_server("/same-host").await.unwrap(), png("cG5n"));
    }

    #[tokio::test]
    async fn a_redirect_to_a_private_address_is_refused() {
        let error = read_from_server("/inside").await.unwrap_err();
        assert!(
            matches!(
                error,
                Error::Refused {
                    source: Refusal::Address(_),
                    ..
                }
            ),
            "{error}"
        );
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn rate_limits_timeouts_and_server_errors_are_retried_and_other_client_errors_are_not() {
        for (path, retryable) in [
            ("/408", true),
            ("/425", true),
            ("/429", true),
            ("/503", true),
            ("/400", false),
            ("/404", false),
        ] {
            let error = read_from_server(path).await.unwrap_err();
            assert!(matches!(error, Error::Fetch { .. }), "{path}: {error}");
            assert_eq!(error.is_retryable(), retryable, "{path}");
        }
    }

    #[tokio::test]
    async fn a_literal_address_that_is_not_public_is_refused_before_it_is_requested() {
        let address = image_server().await;
        for url in [
            format!("http://{address}/a.png"),
            format!("http://[::1]:{}/a.png", address.port()),
            "http://10.0.0.1/a.png".to_string(),
            "http://169.254.169.254/latest/meta-data/".to_string(),
            "http://[::ffff:127.0.0.1]/a.png".to_string(),
            "http://2130706433/a.png".to_string(),
        ] {
            let error = read(&client().unwrap(), url.clone()).await.unwrap_err();
            assert!(
                matches!(
                    error,
                    Error::Refused {
                        source: Refusal::Address(_),
                        ..
                    }
                ),
                "{url}: {error}"
            );
            assert!(!error.is_retryable(), "{url}");
        }
    }

    #[tokio::test]
    async fn a_name_that_resolves_to_loopback_is_refused() {
        let address = image_server().await;
        let url = format!("http://localhost:{}/a.png", address.port());

        let error = read(&client().unwrap(), url).await.unwrap_err();

        assert!(
            matches!(
                error,
                Error::Refused {
                    source: Refusal::Address(_),
                    ..
                }
            ),
            "{error}"
        );
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn a_url_that_is_not_http_or_https_is_refused() {
        for url in ["file:///etc/passwd", "ftp://example.com/a.png"] {
            let error = read(&client().unwrap(), url.to_string()).await.unwrap_err();
            assert!(
                matches!(
                    error,
                    Error::Refused {
                        source: Refusal::Scheme(_),
                        ..
                    }
                ),
                "{url}: {error}"
            );
            assert!(!error.is_retryable(), "{url}");
        }
    }

    #[tokio::test]
    async fn an_empty_or_malformed_url_fails_without_retrying() {
        for url in ["", "not a url", "http://"] {
            let error = read(&client().unwrap(), url.to_string()).await.unwrap_err();
            assert!(matches!(error, Error::InvalidUrl { .. }), "{url}: {error}");
            assert!(!error.is_retryable(), "{url}");
        }
    }

    #[test]
    fn addresses_inside_the_network_are_not_public() {
        for ip in [
            "0.0.0.0",
            "10.1.2.3",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.1",
            "192.0.2.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::ffff:10.0.0.1",
            "64:ff9b::a00:1",
            "64:ff9b:1::1",
            "100::1",
            "2001:db8::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "ff02::1",
        ] {
            assert!(!is_public(ip.parse().unwrap()), "{ip}");
        }
    }

    #[test]
    fn addresses_on_the_internet_are_public() {
        for ip in [
            "1.1.1.1",
            "8.8.8.8",
            "100.128.0.1",
            "172.32.0.1",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2606:4700:4700::1111",
        ] {
            assert!(is_public(ip.parse().unwrap()), "{ip}");
        }
    }

    #[tokio::test]
    async fn a_data_url_is_sent_as_base64_with_its_media_type() {
        let http = client().unwrap();
        for url in [
            "data:image/png;base64,cG5n",
            "data:IMAGE/PNG;charset=binary;base64,cG5n",
        ] {
            assert_eq!(
                read(&http, url.to_string()).await.unwrap(),
                png("cG5n"),
                "{url}"
            );
        }
    }

    #[tokio::test]
    async fn a_data_url_that_is_not_an_image_is_not_sent() {
        let error = read(
            &client().unwrap(),
            "data:text/plain;base64,dHh0".to_string(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::DataNotAnImage { .. }), "{error}");
        assert!(!error.is_retryable());
    }

    #[tokio::test]
    async fn a_data_url_larger_than_the_limit_is_not_sent() {
        let error = user_content(
            &client().unwrap(),
            "data:image/png;base64,cG5n".to_string(),
            Duration::from_secs(5),
            2,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::DataTooLarge { .. }), "{error}");
    }

    #[tokio::test]
    async fn a_data_url_that_is_not_base64_is_not_sent() {
        let http = client().unwrap();
        for url in [
            "data:image/png,png",
            "data:image/png;base64",
            "data:image/png;base64,not base64!",
        ] {
            let error = read(&http, url.to_string()).await.unwrap_err();
            assert!(
                matches!(error, Error::InvalidDataUrl | Error::DataUrlDecode { .. }),
                "{url}: {error}"
            );
            assert!(!error.is_retryable(), "{url}");
        }
    }
}
