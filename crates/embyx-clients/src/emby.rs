//! The Emby server client: the movie index and playlist maintenance.
//!
//! Python origin: `src/embyx_manager/clients/emby.py`. Every request carries the API key in
//! `X-Emby-Token` under the server's `/emby` prefix; a rejected key is [`EmbyError::Auth`],
//! so the settings page and the pipeline can name the cause.

use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

use embyx_core::posixpath;
use embyx_core::pyjson::{self, JsonEncoding, Value};
use embyx_core::pyurl::unquote;
use reqwest::Method;

use crate::http::{HttpxError, with_params};
use crate::net::{BuildError, ClientOptions, HttpClient, Request};
use crate::pyvalue::{self, Raised};

/// Items per page when walking the library; the whole index is a few pages.
pub const INDEX_PAGE_SIZE: usize = 10_000;
const HTTP_UNAUTHORIZED: u16 = 401;

/// What a call fails with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbyError {
    /// `EmbyError`: the server could not be reached or answered with an error.
    Server(String),
    /// `EmbyAuthError` (an `EmbyError`): the API key was rejected.
    Auth(String),
    /// A `TypeError`/`AttributeError`/`ValueError` from walking an answer of the wrong shape,
    /// which the Python client lets through.
    Raised(Raised),
}

impl EmbyError {
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Server(_) => "EmbyError",
            Self::Auth(_) => "EmbyAuthError",
            Self::Raised(raised) => raised.class,
        }
    }

    /// `isinstance(exc, EmbyError)`.
    pub fn is_emby_error(&self) -> bool {
        !matches!(self, Self::Raised(_))
    }
}

impl fmt::Display for EmbyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Server(message) | Self::Auth(message) => f.write_str(message),
            Self::Raised(raised) => f.write_str(&raised.message),
        }
    }
}

impl std::error::Error for EmbyError {}

impl From<Raised> for EmbyError {
    fn from(raised: Raised) -> Self {
        Self::Raised(raised)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbyServerInfo {
    pub name: String,
    pub version: String,
    pub server_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbyPlaylist {
    pub playlist_id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbyPlaylistEntry {
    /// The membership's own id, which is what removal addresses.
    pub entry_id: String,
    pub item_id: String,
}

/// `EmbyClient`.
pub struct EmbyClient {
    base: String,
    client: HttpClient,
    index_page_size: usize,
}

impl fmt::Debug for EmbyClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbyClient")
            .field("base", &self.base)
            .finish_non_exhaustive()
    }
}

impl EmbyClient {
    /// The httpx client `EmbyClient.__init__` builds: the key and `Accept: application/json`
    /// on every request, `timeout=60.0`, redirects not followed.
    pub fn client_options(api_key: &str, timeout: Duration) -> ClientOptions {
        ClientOptions {
            headers: vec![
                ("X-Emby-Token".to_owned(), api_key.to_owned()),
                ("Accept".to_owned(), "application/json".to_owned()),
            ],
            timeout,
            ..ClientOptions::default()
        }
    }

    /// `EmbyClient(base_url, api_key)` with its defaults.
    pub fn new(base_url: &str, api_key: &str) -> Result<Self, BuildError> {
        Self::with_client(base_url, &Self::client_options(api_key, Duration::from_mins(1)))
    }

    pub fn with_client(base_url: &str, options: &ClientOptions) -> Result<Self, BuildError> {
        Ok(Self {
            base: format!("{}/emby", base_url.trim_end_matches('/')),
            client: HttpClient::new(options)?,
            index_page_size: INDEX_PAGE_SIZE,
        })
    }

    /// The page size [`Self::movie_index`] asks for (tests shrink it, as the Python tests
    /// patch `INDEX_PAGE_SIZE`).
    #[must_use]
    pub fn with_index_page_size(mut self, size: usize) -> Self {
        self.index_page_size = size;
        self
    }

    /// The server's identity; also the cheapest check that the key is accepted.
    pub async fn system_info(&self) -> Result<EmbyServerInfo, EmbyError> {
        let body = self.get_json("/System/Info", &[]).await?;
        let field = |key: &str| -> Result<String, EmbyError> { Ok(text_or_empty(pyvalue::get(&body, key)?)) };
        Ok(EmbyServerInfo {
            name: field("ServerName")?,
            version: field("Version")?,
            server_id: field("Id")?,
        })
    }

    /// Every movie's AVID mapped to its item id, in the order the server lists them.
    ///
    /// `avid_of` reads an AVID out of a directory name: the STRM mapping keeps one directory
    /// per title named after it, so the parent directory of the item's path is the key. When
    /// two items share an AVID the first one listed wins.
    pub async fn movie_index(
        &self,
        avid_of: &(dyn Fn(&str) -> String + Sync),
    ) -> Result<Vec<(String, String)>, EmbyError> {
        let mut index: Vec<(String, String)> = Vec::new();
        let mut known: HashSet<String> = HashSet::new();
        let mut start: i128 = 0;
        loop {
            let params = [
                ("Recursive", "true".to_owned()),
                ("IncludeItemTypes", "Movie".to_owned()),
                ("Fields", "Path".to_owned()),
                ("StartIndex", start.to_string()),
                ("Limit", self.index_page_size.to_string()),
            ];
            let body = self.get_json("/Items", &params).await?;
            let items = pyvalue::iterate(pyvalue::get(&body, "Items")?)?;
            for item in &items {
                let path = text_or_empty(pyvalue::get(item, "Path")?);
                let item_id = text_or_empty(pyvalue::get(item, "Id")?);
                if path.is_empty() || item_id.is_empty() {
                    continue;
                }
                let avid = avid_of(posixpath::basename(posixpath::dirname(&path)));
                if !avid.is_empty() && known.insert(avid.clone()) {
                    index.push((avid, item_id));
                }
            }
            start += i128::try_from(items.len()).unwrap_or(i128::MAX);
            let total = pyvalue::py_int(pyvalue::or(pyvalue::get(&body, "TotalRecordCount")?, &Value::Int(0)))?;
            if items.is_empty() || start >= total {
                return Ok(index);
            }
        }
    }

    pub async fn list_playlists(&self) -> Result<Vec<EmbyPlaylist>, EmbyError> {
        let params = [
            ("Recursive", "true".to_owned()),
            ("IncludeItemTypes", "Playlist".to_owned()),
        ];
        let body = self.get_json("/Items", &params).await?;
        let mut playlists = Vec::new();
        for item in pyvalue::iterate(pyvalue::get(&body, "Items")?)? {
            if !pyvalue::truthy(pyvalue::get(&item, "Id")?) {
                continue;
            }
            playlists.push(EmbyPlaylist {
                playlist_id: pyvalue::py_str(pyvalue::item(&item, "Id")?),
                name: text_or_empty(pyvalue::get(&item, "Name")?),
            });
        }
        Ok(playlists)
    }

    /// Create a video playlist holding `item_ids` in that order; returns its id.
    pub async fn create_playlist(&self, name: &str, item_ids: &[String]) -> Result<String, EmbyError> {
        let params = [
            ("Name", name.to_owned()),
            ("Ids", item_ids.join(",")),
            ("MediaType", "Video".to_owned()),
        ];
        let body = self.request_json(Method::POST, "/Playlists", &params).await?;
        let playlist_id = text_or_empty(pyvalue::get(&body, "Id")?);
        if playlist_id.is_empty() {
            return Err(EmbyError::Server("playlist creation returned no id".to_owned()));
        }
        Ok(playlist_id)
    }

    pub async fn playlist_entries(&self, playlist_id: &str) -> Result<Vec<EmbyPlaylistEntry>, EmbyError> {
        let body = self.get_json(&format!("/Playlists/{playlist_id}/Items"), &[]).await?;
        let mut entries = Vec::new();
        for item in pyvalue::iterate(pyvalue::get(&body, "Items")?)? {
            if !pyvalue::truthy(pyvalue::get(&item, "PlaylistItemId")?) || !pyvalue::truthy(pyvalue::get(&item, "Id")?)
            {
                continue;
            }
            entries.push(EmbyPlaylistEntry {
                entry_id: pyvalue::py_str(pyvalue::item(&item, "PlaylistItemId")?),
                item_id: pyvalue::py_str(pyvalue::item(&item, "Id")?),
            });
        }
        Ok(entries)
    }

    pub async fn add_entries(&self, playlist_id: &str, item_ids: &[String]) -> Result<(), EmbyError> {
        if item_ids.is_empty() {
            return Ok(());
        }
        let path = format!("/Playlists/{playlist_id}/Items");
        self.request(Method::POST, &path, &[("Ids", item_ids.join(","))])
            .await
            .map(drop)
    }

    pub async fn remove_entries(&self, playlist_id: &str, entry_ids: &[String]) -> Result<(), EmbyError> {
        if entry_ids.is_empty() {
            return Ok(());
        }
        let path = format!("/Playlists/{playlist_id}/Items");
        self.request(Method::DELETE, &path, &[("EntryIds", entry_ids.join(","))])
            .await
            .map(drop)
    }

    pub async fn delete_playlist(&self, playlist_id: &str) -> Result<(), EmbyError> {
        self.request(Method::POST, "/Items/Delete", &[("Ids", playlist_id.to_owned())])
            .await
            .map(drop)
    }

    async fn get_json(&self, path: &str, params: &[(&str, String)]) -> Result<Value, EmbyError> {
        self.request_json(Method::GET, path, params).await
    }

    async fn request_json(&self, method: Method, path: &str, params: &[(&str, String)]) -> Result<Value, EmbyError> {
        let answer = self.request(method, path, params).await?;
        json_object(&answer)
    }

    /// `_request`: the answer, a transport failure or an error status an [`EmbyError`]
    /// naming the method and the path as the caller gave it.
    async fn request(&self, method: Method, path: &str, params: &[(&str, String)]) -> Result<Answer, EmbyError> {
        let url = with_params(&format!("{}{path}", self.base), params);
        let transport =
            |error: HttpxError| EmbyError::Server(format!("{method} {path}: {}: {error}", error.class_name()));
        let response = self
            .client
            .send(Request::new(method.clone(), url))
            .await
            .map_err(|error| transport(HttpxError::from(error)))?;
        let status = response.status();
        let request_path = unquote(response.url().path());
        // httpx reads the body inside `client.request`, so a body cut short fails here too.
        let body = response
            .bytes()
            .await
            .map_err(|error| transport(HttpxError::from(error)))?;
        if status == HTTP_UNAUTHORIZED {
            return Err(EmbyError::Auth(format!("{method} {path}: the API key was rejected")));
        }
        if (400..600).contains(&status) {
            return Err(EmbyError::Server(format!("{method} {path}: HTTP {status}")));
        }
        Ok(Answer {
            method,
            path: request_path,
            body: body.to_vec(),
        })
    }
}

/// A successful answer and the request it answers (`response.request`).
struct Answer {
    method: Method,
    /// `response.request.url.path`: the full path, `/emby` prefix included.
    path: String,
    body: Vec<u8>,
}

/// `_json_object`: the body as a JSON object.
fn json_object(answer: &Answer) -> Result<Value, EmbyError> {
    let Answer { method, path, body } = answer;
    let not_json = || EmbyError::Server(format!("{method} {path}: not a JSON response"));
    let text = match pyjson::detect_json_encoding(body) {
        JsonEncoding::Utf8(bytes) => std::str::from_utf8(bytes).map_err(|_| not_json())?.to_owned(),
        JsonEncoding::Decoded(text) => text.ok_or_else(not_json)?,
    };
    let value = pyjson::loads(&text).map_err(|_| not_json())?;
    if !matches!(value, Value::Dict(_)) {
        return Err(EmbyError::Server(format!("{method} {path}: unexpected JSON shape")));
    }
    Ok(value)
}

/// `str(x or '')`.
fn text_or_empty(value: &Value) -> String {
    pyvalue::py_str(pyvalue::or(value, &pyvalue::EMPTY_STR))
}

#[cfg(test)]
mod tests;
