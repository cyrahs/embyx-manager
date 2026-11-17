//! Emby client: request shapes, index paging, and error mapping.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use embyx_core::avid::AvidParser;
use serde_json::json;

use super::*;
use crate::net::fakeweb::{FakeWeb, Reply, Seen};

const BASE: &str = "http://emby.test";
const KEY: &str = "k3y";

type Params = BTreeMap<String, Vec<String>>;

/// `urllib.parse.parse_qs`.
fn parse_qs(query: &str) -> Params {
    let mut params = Params::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let decode = |text: &str| unquote(&text.replace('+', " "));
        if !value.is_empty() {
            params.entry(decode(name)).or_default().push(decode(value));
        }
    }
    params
}

/// Answers the handful of routes the client uses; records every request.
#[derive(Default)]
struct FakeServer {
    movies: Vec<serde_json::Value>,
    /// Playlist id and its `(entry_id, item_id)` pairs, in creation order.
    playlists: Vec<(String, Vec<(String, String)>)>,
    names: BTreeMap<String, String>,
    calls: Vec<(String, String, Params)>,
    next: u32,
    status_override: Option<u16>,
}

impl FakeServer {
    fn handle(&mut self, request: &Seen) -> Reply {
        let params = parse_qs(request.query());
        let path = request.path().to_owned();
        let method = request.method.as_str();
        self.calls.push((request.method.clone(), path.clone(), params.clone()));
        if request.header("X-Emby-Token") != Some(KEY) {
            return Reply::status(401);
        }
        if let Some(status) = self.status_override {
            return Reply::status(status);
        }
        let first = |name: &str| params.get(name).and_then(|values| values.first()).cloned();
        let include = first("IncludeItemTypes");
        if path == "/emby/System/Info" {
            return Reply::json(
                200,
                &json!({"ServerName": "embyx", "Version": "4.10.0.40", "Id": "srv"}),
            );
        }
        if path == "/emby/Items" && include.as_deref() == Some("Movie") {
            let start: usize = first("StartIndex")
                .unwrap_or_else(|| "0".to_owned())
                .parse()
                .expect("start");
            let limit: usize = first("Limit")
                .unwrap_or_else(|| "100".to_owned())
                .parse()
                .expect("limit");
            let page: Vec<_> = self.movies.iter().skip(start).take(limit).cloned().collect();
            return Reply::json(200, &json!({"Items": page, "TotalRecordCount": self.movies.len()}));
        }
        if path == "/emby/Items" && include.as_deref() == Some("Playlist") {
            let items: Vec<_> = self
                .playlists
                .iter()
                .map(|(id, _)| json!({"Id": id, "Name": self.names[id]}))
                .collect();
            return Reply::json(200, &json!({"Items": items, "TotalRecordCount": items.len()}));
        }
        if path == "/emby/Playlists" && method == "POST" {
            let id = self.new_id();
            let name = first("Name").expect("name");
            self.names.insert(id.clone(), name.clone());
            let ids = first("Ids").unwrap_or_default();
            let entries = ids
                .split(',')
                .filter(|item| !item.is_empty())
                .map(|item| (self.new_id(), item.to_owned()))
                .collect();
            self.playlists.push((id.clone(), entries));
            return Reply::json(200, &json!({"Id": id, "Name": name}));
        }
        if path.starts_with("/emby/Playlists/") && path.ends_with("/Items") {
            let id = path.split('/').nth(3).expect("id").to_owned();
            let index = self
                .playlists
                .iter()
                .position(|(known, _)| *known == id)
                .expect("playlist");
            match method {
                "GET" => {
                    let items: Vec<_> = self.playlists[index]
                        .1
                        .iter()
                        .map(|(entry, item)| json!({"Id": item, "PlaylistItemId": entry}))
                        .collect();
                    return Reply::json(200, &json!({"Items": items, "TotalRecordCount": items.len()}));
                }
                "POST" => {
                    let ids = first("Ids").expect("ids");
                    for item in ids.split(',') {
                        let entry = self.new_id();
                        self.playlists[index].1.push((entry, item.to_owned()));
                    }
                    return Reply::status(204);
                }
                "DELETE" => {
                    let gone = first("EntryIds").expect("entry ids");
                    let gone: Vec<&str> = gone.split(',').collect();
                    self.playlists[index]
                        .1
                        .retain(|(entry, _)| !gone.contains(&entry.as_str()));
                    return Reply::status(204);
                }
                _ => {}
            }
        }
        if path == "/emby/Items/Delete" && method == "POST" {
            for id in first("Ids").expect("ids").split(',') {
                self.playlists.retain(|(known, _)| known != id);
                self.names.remove(id);
            }
            return Reply::status(204);
        }
        Reply::status(404)
    }

    fn new_id(&mut self) -> String {
        self.next += 1;
        (100 + self.next).to_string()
    }
}

async fn serve(server: FakeServer) -> (FakeWeb, Arc<Mutex<FakeServer>>) {
    let server = Arc::new(Mutex::new(server));
    let shared = Arc::clone(&server);
    let web = FakeWeb::start(move |seen: &Seen| shared.lock().expect("lock").handle(seen)).await;
    (web, server)
}

fn make_client(web: &FakeWeb, key: &str) -> EmbyClient {
    let options = ClientOptions {
        proxy: Some(web.proxy.clone()),
        ..EmbyClient::client_options(key, Duration::from_secs(5))
    };
    EmbyClient::with_client(&format!("{BASE}/"), &options).expect("client")
}

fn movie(item_id: &str, path: &str) -> serde_json::Value {
    json!({"Id": item_id, "Name": path.rsplit('/').next().unwrap_or_default(), "Path": path})
}

// py: tests/test_emby.py::test_system_info_sends_the_key_under_the_emby_prefix
#[tokio::test]
async fn system_info_sends_the_key_under_the_emby_prefix() {
    let (web, server) = serve(FakeServer::default()).await;
    let info = make_client(&web, KEY).system_info().await.expect("info");
    assert_eq!(
        info,
        EmbyServerInfo {
            name: "embyx".to_owned(),
            version: "4.10.0.40".to_owned(),
            server_id: "srv".to_owned(),
        }
    );
    let calls = &server.lock().expect("lock").calls;
    assert_eq!((calls[0].0.as_str(), calls[0].1.as_str()), ("GET", "/emby/System/Info"));
    assert_eq!(web.seen()[0].header("Accept"), Some("application/json"));
}

// py: tests/test_emby.py::test_rejected_key_is_an_auth_error
#[tokio::test]
async fn rejected_key_is_an_auth_error() {
    let (web, _server) = serve(FakeServer::default()).await;
    let error = make_client(&web, "wrong").system_info().await.unwrap_err();
    assert_eq!(
        error,
        EmbyError::Auth("GET /System/Info: the API key was rejected".to_owned())
    );
    assert_eq!(error.class_name(), "EmbyAuthError");
    assert!(error.is_emby_error());
}

// py: tests/test_emby.py::test_other_http_errors_and_transport_failures_are_emby_errors
#[tokio::test]
async fn other_http_errors_and_transport_failures_are_emby_errors() {
    let (web, _server) = serve(FakeServer {
        status_override: Some(503),
        ..FakeServer::default()
    })
    .await;
    let error = make_client(&web, KEY).system_info().await.unwrap_err();
    assert_eq!(error, EmbyError::Server("GET /System/Info: HTTP 503".to_owned()));

    let refused = EmbyClient::with_client(
        BASE,
        &ClientOptions {
            proxy: Some("http://127.0.0.1:9".to_owned()),
            ..EmbyClient::client_options(KEY, Duration::from_secs(5))
        },
    )
    .expect("client");
    let error = refused.system_info().await.unwrap_err();
    assert!(
        error.to_string().starts_with("GET /System/Info: ConnectError: "),
        "{error}"
    );
    assert_eq!(error.class_name(), "EmbyError");

    // A hang-up, a body cut short and a stall fail the same way.
    for (reply, class) in [
        (Reply::Close, "RemoteProtocolError"),
        (
            Reply::Truncated {
                status: 200,
                sent: b"{\"Id\"".to_vec(),
                length: 50,
            },
            "RemoteProtocolError",
        ),
        (Reply::Stall(Duration::from_secs(5)), "ReadTimeout"),
        (Reply::Reset, "ReadError"),
    ] {
        let web = FakeWeb::start(move |_: &Seen| reply.clone()).await;
        let options = ClientOptions {
            proxy: Some(web.proxy.clone()),
            ..EmbyClient::client_options(KEY, Duration::from_millis(300))
        };
        let client = EmbyClient::with_client(BASE, &options).expect("client");
        let error = client.system_info().await.unwrap_err();
        assert!(
            error.to_string().starts_with(&format!("GET /System/Info: {class}: ")),
            "{error}"
        );
    }
}

// py: tests/test_emby.py::test_non_json_answers_are_reported_not_raised_raw
#[tokio::test]
async fn non_json_answers_are_reported_not_raised_raw() {
    for (body, message) in [
        (
            b"<html>login</html>".to_vec(),
            "GET /emby/System/Info: not a JSON response",
        ),
        (b"[1, 2]".to_vec(), "GET /emby/System/Info: unexpected JSON shape"),
        (b"\xff\xfe".to_vec(), "GET /emby/System/Info: not a JSON response"),
    ] {
        let web = FakeWeb::start(move |_: &Seen| Reply::bytes(200, body.clone())).await;
        let error = make_client(&web, KEY).system_info().await.unwrap_err();
        assert_eq!(error, EmbyError::Server(message.to_owned()));
    }
    // `body.get('Items') or []` on a dict that is not one: Python's `AttributeError` passes.
    let web = FakeWeb::start(|_: &Seen| Reply::json(200, &json!({"Items": [1], "TotalRecordCount": 1}))).await;
    let error = make_client(&web, KEY).list_playlists().await.unwrap_err();
    assert_eq!(error.class_name(), "AttributeError");
    assert!(!error.is_emby_error());
}

// py: tests/test_emby.py::test_playlist_creation_without_an_id_is_an_error
#[tokio::test]
async fn playlist_creation_without_an_id_is_an_error() {
    for body in [json!({}), json!({"Id": ""}), json!({"Id": null})] {
        let web = FakeWeb::start(move |_: &Seen| Reply::json(200, &body)).await;
        let error = make_client(&web, KEY)
            .create_playlist("x", &["1".to_owned()])
            .await
            .unwrap_err();
        assert_eq!(error, EmbyError::Server("playlist creation returned no id".to_owned()));
    }
}

// py: tests/test_emby.py::test_movie_index_keys_on_the_parent_directory_and_pages_through
#[tokio::test]
async fn movie_index_keys_on_the_parent_directory_and_pages_through() {
    let (web, server) = serve(FakeServer {
        movies: vec![
            movie("1", "/media/local/actor/HMN/HMN-911/HMN-911.strm"),
            // The directory, not the file name, names the title: a subtitled part keeps its
            // suffix on the file only.
            movie("2", "/media/local/rank/SNOS/SNOS-377/SNOS-377-C.strm"),
            // Padding differs across the library; the parser normalizes both spellings.
            movie("3", "/media/local/rest/LAF/LAF-06/LAF-06.strm"),
            // A second copy of an AVID does not displace the first.
            movie("4", "/media/local/type/HMN/HMN-911/HMN-911-4K.strm"),
            json!({"Id": "5", "Name": "no path"}),
        ],
        ..FakeServer::default()
    })
    .await;
    let parser = AvidParser::default();
    let index = make_client(&web, KEY)
        .with_index_page_size(2)
        .movie_index(&|name: &str| parser.get_avid(name))
        .await
        .expect("index");
    let expected: Vec<(String, String)> = [("HMN-911", "1"), ("SNOS-377", "2"), ("LAF-006", "3")]
        .iter()
        .map(|(avid, id)| ((*avid).to_owned(), (*id).to_owned()))
        .collect();
    assert_eq!(index, expected);
    let calls = &server.lock().expect("lock").calls;
    let pages: Vec<&Params> = calls
        .iter()
        .filter(|call| call.1 == "/emby/Items")
        .map(|call| &call.2)
        .collect();
    let starts: Vec<&[String]> = pages.iter().map(|page| page["StartIndex"].as_slice()).collect();
    assert_eq!(starts, [["0"], ["2"], ["4"]]);
    assert!(
        pages
            .iter()
            .all(|page| page["Fields"] == ["Path"] && page["Recursive"] == ["true"])
    );
}

// py: tests/test_emby.py::test_playlist_round_trip_keeps_order_and_addresses_entries
#[tokio::test]
async fn playlist_round_trip_keeps_order_and_addresses_entries() {
    let (web, server) = serve(FakeServer::default()).await;
    let client = make_client(&web, KEY);
    let ids = |items: &[&str]| items.iter().map(|item| (*item).to_owned()).collect::<Vec<_>>();
    let playlist_id = client
        .create_playlist("JavDB 有码 TOP250", &ids(&["30", "10", "20"]))
        .await
        .expect("create");
    let names: Vec<String> = client
        .list_playlists()
        .await
        .expect("list")
        .into_iter()
        .map(|p| p.name)
        .collect();
    assert_eq!(names, ["JavDB 有码 TOP250"]);

    let entries = client.playlist_entries(&playlist_id).await.expect("entries");
    let items: Vec<&str> = entries.iter().map(|entry| entry.item_id.as_str()).collect();
    assert_eq!(items, ["30", "10", "20"]);

    client
        .remove_entries(&playlist_id, &[entries[1].entry_id.clone()])
        .await
        .expect("remove");
    client.add_entries(&playlist_id, &ids(&["40"])).await.expect("add");
    let items: Vec<String> = client
        .playlist_entries(&playlist_id)
        .await
        .expect("entries")
        .into_iter()
        .map(|entry| entry.item_id)
        .collect();
    assert_eq!(items, ["30", "20", "40"]);

    let before = server.lock().expect("lock").calls.len();
    client.remove_entries(&playlist_id, &[]).await.expect("remove nothing");
    client.add_entries(&playlist_id, &[]).await.expect("add nothing");
    assert_eq!(
        server.lock().expect("lock").calls.len(),
        before,
        "empty changes send nothing"
    );
    client.delete_playlist(&playlist_id).await.expect("delete");
    assert_eq!(client.list_playlists().await.expect("list"), []);

    let calls = &server.lock().expect("lock").calls;
    let create = calls
        .iter()
        .find(|call| call.0 == "POST" && call.1 == "/emby/Playlists")
        .expect("create call");
    let expected: Params = [
        ("Name", "JavDB 有码 TOP250"),
        ("Ids", "30,10,20"),
        ("MediaType", "Video"),
    ]
    .iter()
    .map(|(name, value)| ((*name).to_owned(), vec![(*value).to_owned()]))
    .collect();
    assert_eq!(create.2, expected);
    let delete = calls
        .iter()
        .find(|call| call.1 == "/emby/Items/Delete")
        .expect("delete call");
    let expected: Params = std::iter::once(("Ids".to_owned(), vec![playlist_id.clone()])).collect();
    assert_eq!(delete.2, expected);
    // Empty edits never reach the server.
    assert_eq!(
        calls
            .iter()
            .filter(|call| matches!(call.0.as_str(), "POST" | "DELETE") && call.1.ends_with("/Items"))
            .count(),
        2
    );
}

#[test]
fn the_key_stays_out_of_debug_output() {
    let client = EmbyClient::new(BASE, "secret-key").expect("client");
    assert!(!format!("{client:?}").contains("secret-key"));
}
