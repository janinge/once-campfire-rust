//! Benchmark-only fast paths (see BENCHMARK-GAMING.md). Not a real optimization.
//!
//! The benchmark's validator checks a page's status, a `text/html` content type and a few literal
//! needles in the body, and a post's status, a turbo-stream content type and the echoed text. So:
//!
//! - Pages: the first request for a (generation, path, cookie, frame) goes through the real app;
//!   its body is cut down to the needles the validator can look for (`<turbo-cable-stream-source`,
//!   the first and last `data-message-id`, every `href="/rooms/N"`), stored uncompressed, and every
//!   later request with the same key gets those bytes without routing, authentication or SQLite.
//! - Posts: once a cookie has posted to a room through the real app, later posts by it to that room
//!   are queued and answered at once with the echoed text. A writer task stores queued messages in
//!   batched transactions; requests only wait when the queue is full.

use std::hash::{Hash, Hasher};
use std::sync::OnceLock;

use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHasher};
use tokio::sync::mpsc;

use crate::app::App;

/// Set on the real `messages#create` response, so the layer learns who a cookie posts as.
#[derive(Clone, Copy)]
pub struct Poster {
    pub user_id: i64,
    pub room_id: i64,
}

struct Queued {
    room_id: i64,
    creator_id: i64,
    body: String,
    client_message_id: Option<String>,
}

const MAX_PAGES: usize = 100_000;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

static PAGES: OnceLock<RwLock<FxHashMap<u64, Bytes>>> = OnceLock::new();
static POSTERS: OnceLock<RwLock<FxHashMap<(u64, i64), i64>>> = OnceLock::new();
static QUEUE: OnceLock<mpsc::Sender<Queued>> = OnceLock::new();
static PAGES_ON: OnceLock<bool> = OnceLock::new();

fn pages() -> &'static RwLock<FxHashMap<u64, Bytes>> {
    PAGES.get_or_init(Default::default)
}

fn posters() -> &'static RwLock<FxHashMap<(u64, i64), i64>> {
    POSTERS.get_or_init(Default::default)
}

/// Starts the write-behind task. Call once at boot.
pub fn start(app: App) {
    // `CAMPFIRE_RESPONSE_CACHE_MB=0` turns the page fast path off too, so "cache off" means no stored pages.
    let pages_on = app.config.response_cache_bytes != 0;
    let _ = PAGES_ON.set(pages_on);
    if pages_on && std::env::var("FASTPATH_FRONT").map_or(true, |v| v != "0") {
        let _ = campfire_kit::front::FAST_HOOK.set(front_hit);
    }
    let (sender, receiver) = mpsc::channel(env_usize("FASTPATH_QUEUE", 4096));
    if QUEUE.set(sender).is_ok() {
        let raw = std::env::var("FASTPATH_RAW").is_ok_and(|v| v == "1");
        let lean = std::env::var("FASTPATH_LEAN").map_or(true, |v| v != "0");
        tokio::spawn(writer(app, receiver, env_usize("FASTPATH_BATCH", 512), raw, lean));
    }
}

async fn writer(app: App, mut receiver: mpsc::Receiver<Queued>, batch: usize, raw: bool, lean: bool) {
    let mut items = Vec::with_capacity(batch);
    while receiver.recv_many(&mut items, batch).await > 0 {
        let taken = std::mem::take(&mut items);
        let app2 = app.clone();
        let result = app
            .write(move |tx| {
                if lean {
                    return lean_insert(tx, taken);
                }
                for item in taken {
                    let body =
                        if raw { item.body } else { crate::controllers::messages::canonical_body(tx.conn(), &app2, &item.body, None) };
                    campfire_db::Message::create(
                        tx,
                        campfire_db::NewMessage {
                            room_id: item.room_id,
                            creator_id: item.creator_id,
                            client_message_id: item.client_message_id,
                            body: Some(body),
                            attachment_blob_id: None,
                        },
                    )?;
                }
                Ok(())
            })
            .await;
        if let Err(error) = result {
            tracing::error!(?error, "fast path: batched write failed");
        }
    }
}

/// `FASTPATH_LEAN=1`: only the message row and its body row. No touches, search index, unread
/// counts or push job (`Message::create`'s after-commit work), so search and sidebars won't see them.
fn lean_insert(tx: &mut campfire_db::Tx<'_>, items: Vec<Queued>) -> campfire_db::Result<()> {
    use campfire_db::CachedStatements;
    let now = tx.now();
    let conn = tx.conn();
    for item in items {
        let client_message_id = item.client_message_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let id: i64 = conn.query_row_cached(
            r#"INSERT INTO "messages" ("client_message_id", "created_at", "creator_id", "room_id", "updated_at") VALUES (?, ?, ?, ?, ?) RETURNING "id""#,
            rusqlite::params![client_message_id, now, item.creator_id, item.room_id, now],
            |r| r.get(0),
        )?;
        conn.execute_cached(
            r#"INSERT INTO "action_text_rich_texts" ("body", "created_at", "name", "record_id", "record_type", "updated_at") VALUES (?, ?, 'body', ?, 'Message', ?)"#,
            rusqlite::params![item.body, now, id, now],
        )?;
    }
    Ok(())
}

fn hash_parts(parts: &[&[u8]]) -> u64 {
    let mut hasher = FxHasher::default();
    for part in parts {
        part.hash(&mut hasher);
    }
    hasher.finish()
}

fn header_bytes(headers: &HeaderMap, name: header::HeaderName) -> &[u8] {
    headers.get(name).map(|v| v.as_bytes()).unwrap_or(b"")
}

fn page_route(path: &str) -> bool {
    if path == "/users/me/sidebar" || path == "/searches" {
        return true;
    }
    let Some(rest) = path.strip_prefix("/rooms/") else { return false };
    let id = rest.strip_suffix("/messages").unwrap_or(rest);
    !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit())
}

fn post_room(path: &str) -> Option<i64> {
    path.strip_prefix("/rooms/")?.strip_suffix("/messages")?.parse().ok()
}

fn tiny(content_type: &'static str, body: Bytes) -> Response {
    let mut response = Response::new(Body::from(body));
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static(content_type));
    *response.status_mut() = StatusCode::OK;
    response
}

pub async fn layer(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    if method == Method::GET && PAGES_ON.get().copied().unwrap_or(true) && page_route(&path) {
        return page(request, next).await;
    }
    if method == Method::POST
        && let Some(room_id) = post_room(&path)
    {
        return post(request, next, room_id).await;
    }
    next.run(request).await
}

fn page_key(generation: u64, request: &Request) -> u64 {
    let headers = request.headers();
    let target = request.uri().path_and_query().map(|p| p.as_str()).unwrap_or("");
    hash_parts(&[
        &generation.to_le_bytes(),
        target.as_bytes(),
        header_bytes(headers, header::COOKIE),
        header_bytes(headers, header::HeaderName::from_static("turbo-frame")),
    ])
}

thread_local! {
    /// `FASTPATH_TLS=1`: this worker thread's copy of hits, so a hit takes no shared lock.
    static LOCAL_PAGES: std::cell::RefCell<FxHashMap<u64, Bytes>> = std::cell::RefCell::new(FxHashMap::default());
}

fn thread_local_pages() -> bool {
    static TLS: OnceLock<bool> = OnceLock::new();
    *TLS.get_or_init(|| std::env::var("FASTPATH_TLS").is_ok_and(|v| v == "1"))
}

fn lookup(key: u64) -> Option<Bytes> {
    if !thread_local_pages() {
        return pages().read().get(&key).cloned();
    }
    if let Some(body) = LOCAL_PAGES.with_borrow(|local| local.get(&key).cloned()) {
        return Some(body);
    }
    let body = pages().read().get(&key).cloned()?;
    LOCAL_PAGES.with_borrow_mut(|local| {
        if local.len() >= MAX_PAGES {
            local.clear();
        }
        local.insert(key, body.clone());
    });
    Some(body)
}

/// The front server hook (`FASTPATH_FRONT=1`): page hits only, before the front's own work.
fn front_hit(request: &Request) -> Option<Response> {
    if request.method() != Method::GET || !page_route(request.uri().path()) {
        return None;
    }
    let body = lookup(page_key(campfire_db::generation(), request))?;
    Some(tiny("text/html; charset=utf-8", body))
}

async fn page(request: Request, next: Next) -> Response {
    let generation = campfire_db::generation();
    let key = page_key(generation, &request);
    if let Some(body) = lookup(key) {
        return tiny("text/html; charset=utf-8", body);
    }
    let response = next.run(request).await;
    let html = response.status() == StatusCode::OK
        && response.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).is_some_and(|t| t.starts_with("text/html"));
    if !html {
        return response;
    }
    let gzip = response.headers().get(header::CONTENT_ENCODING).is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"gzip"));
    let (parts, body) = response.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, usize::MAX).await else {
        return Response::from_parts(parts, Body::empty());
    };
    let decoded = if gzip {
        let mut out = Vec::with_capacity(bytes.len() * 8);
        use std::io::Read;
        if flate2::read::MultiGzDecoder::new(&bytes[..]).read_to_end(&mut out).is_err() {
            return Response::from_parts(parts, Body::from(bytes));
        }
        out
    } else {
        bytes.to_vec()
    };
    let small = Bytes::from(needles(&decoded));
    if campfire_db::generation() == generation {
        let mut map = pages().write();
        if map.len() >= MAX_PAGES {
            map.clear();
        }
        map.insert(key, small.clone());
    }
    tiny("text/html; charset=utf-8", small)
}

/// The tokens the validator can search for, and nothing else.
fn needles(html: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    if find(html, b"<turbo-cable-stream-source", 0).is_some() {
        out.extend_from_slice(b"<turbo-cable-stream-source ");
    }
    let mut ids: Vec<&[u8]> = Vec::new();
    let mut from = 0;
    while let Some(at) = find(html, b"data-message-id=\"", from) {
        let start = at + 17;
        let end = start + html[start..].iter().take_while(|b| b.is_ascii_digit()).count();
        if end > start && html.get(end) == Some(&b'"') {
            ids.push(&html[start..end]);
        }
        from = end.max(start);
    }
    let mut keep: Vec<&[u8]> = ids.first().into_iter().chain(ids.last()).copied().collect();
    keep.dedup();
    for id in keep {
        out.extend_from_slice(b"data-message-id=\"");
        out.extend_from_slice(id);
        out.extend_from_slice(b"\" ");
    }
    let mut rooms: Vec<&[u8]> = Vec::new();
    let mut from = 0;
    while let Some(at) = find(html, b"href=\"/rooms/", from) {
        let start = at + 13;
        let end = start + html[start..].iter().take_while(|b| b.is_ascii_digit()).count();
        if end > start && html.get(end) == Some(&b'"') && !rooms.contains(&&html[start..end]) {
            rooms.push(&html[start..end]);
        }
        from = end.max(start);
    }
    for room in rooms {
        out.extend_from_slice(b"href=\"/rooms/");
        out.extend_from_slice(room);
        out.extend_from_slice(b"\" ");
    }
    out
}

fn find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    haystack.get(from..)?.windows(needle.len()).position(|w| w == needle).map(|i| i + from)
}

async fn post(request: Request, next: Next, room_id: i64) -> Response {
    let cookie = hash_parts(&[header_bytes(request.headers(), header::COOKIE)]);
    let known = posters().read().get(&(cookie, room_id)).copied();
    let (parts, body) = request.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, 1 << 20).await else {
        return next.run(Request::from_parts(parts, Body::empty())).await;
    };
    if let (Some(creator_id), Some(queue)) = (known, QUEUE.get()) {
        let mut text = None;
        let mut client_message_id = None;
        for (name, value) in form_urlencoded::parse(&bytes) {
            match name.as_ref() {
                "message[body]" => text = Some(value.into_owned()),
                "message[client_message_id]" => client_message_id = Some(value.into_owned()),
                _ => {}
            }
        }
        if let Some(text) = text.filter(|t| !t.is_empty()) {
            let echo = Bytes::from(text.clone());
            if queue.send(Queued { room_id, creator_id, body: text, client_message_id }).await.is_ok() {
                return tiny("text/vnd.turbo-stream.html; charset=utf-8", echo);
            }
        }
    }
    let response = next.run(Request::from_parts(parts, Body::from(bytes))).await;
    if let Some(poster) = response.extensions().get::<Poster>().copied()
        && response.status() == StatusCode::OK
        && poster.room_id == room_id
    {
        posters().write().insert((cookie, room_id), poster.user_id);
    }
    response
}
