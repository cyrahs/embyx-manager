//! ElementTree as `defusedxml.ElementTree.fromstring(data, forbid_dtd=True)` builds it.
//!
//! Python origin: `monitor/feeds.py` `parse_feed` and `clients/avbase.py`
//! `_talent_name_from_feed`, which read feeds with expat behind defusedxml and walk the tree
//! with `find`, `findall`, `findtext`, `get` and `.text`. The tree here is ElementTree's model:
//! a tag is `name` or `{namespace}name`, `.text` is the character data before the first child
//! element (CDATA and entities included; comments and processing instructions are not part of
//! the tree, so the text on both sides of one joins up), attributes by their expanded name.
//!
//! Parsing is `roxmltree` on the bytes decoded the way expat decodes them: a UTF-8 or UTF-16
//! byte-order mark, else the declared encoding (UTF-8, ISO-8859-1 or US-ASCII; UTF-8 without a
//! declaration). A document type declaration is refused, as `forbid_dtd=True` refuses it. The
//! error classes are Python's (`ParseError`, `DTDForbidden`); the message after them is this
//! parser's, not expat's (docs/DIVERGENCES.md D27).

use std::fmt;

use embyx_core::pytext::py_repr;

/// An element of the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlElement {
    /// `element.tag`: `name`, or `{namespace}name`.
    pub tag: String,
    /// `element.attrib`, in document order, keyed like the tag.
    pub attributes: Vec<(String, String)>,
    /// `element.text`.
    pub text: Option<String>,
    pub children: Vec<XmlElement>,
}

impl XmlElement {
    /// `element.find(tag)`: the first child with that tag.
    pub fn find(&self, tag: &str) -> Option<&XmlElement> {
        self.children.iter().find(|child| child.tag == tag)
    }

    /// `element.findall(tag)`: the children with that tag.
    pub fn findall<'a>(&'a self, tag: &'a str) -> impl Iterator<Item = &'a XmlElement> + 'a {
        self.children.iter().filter(move |child| child.tag == tag)
    }

    /// `element.findtext(tag)`: the first such child's text, `''` when it has none, `None`
    /// when there is no such child.
    pub fn findtext(&self, tag: &str) -> Option<&str> {
        self.find(tag).map(|child| child.text.as_deref().unwrap_or(""))
    }

    /// `element.get(name)`.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Why the bytes are not a tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XmlError {
    /// `xml.etree.ElementTree.ParseError`: not well-formed.
    Syntax(String),
    /// `defusedxml.DTDForbidden`: the document has a document type declaration, with its
    /// name and external identifiers.
    DtdForbidden {
        name: String,
        system_id: Option<String>,
        public_id: Option<String>,
    },
}

impl XmlError {
    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Syntax(_) => "ParseError",
            Self::DtdForbidden { .. } => "DTDForbidden",
        }
    }
}

impl fmt::Display for XmlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax(message) => f.write_str(message),
            Self::DtdForbidden {
                name,
                system_id,
                public_id,
            } => {
                let literal = |value: &Option<String>| value.as_deref().map_or_else(|| "None".to_owned(), py_repr);
                write!(
                    f,
                    "DTDForbidden(name={}, system_id={}, public_id={})",
                    py_repr(name),
                    literal(system_id),
                    literal(public_id)
                )
            }
        }
    }
}

impl std::error::Error for XmlError {}

/// `defusedxml.ElementTree.fromstring(data, forbid_dtd=True)`.
pub fn fromstring(data: &[u8]) -> Result<XmlElement, XmlError> {
    let text = decode(data)?;
    let options = roxmltree::ParsingOptions {
        allow_dtd: false,
        ..roxmltree::ParsingOptions::default()
    };
    let document = roxmltree::Document::parse_with_options(&text, options).map_err(|error| match error {
        roxmltree::Error::DtdDetected => doctype(&text),
        other => XmlError::Syntax(other.to_string()),
    })?;
    Ok(build(document.root_element()))
}

/// The `DTDForbidden` for the document's `<!DOCTYPE name [SYSTEM "s" | PUBLIC "p" "s"]>`.
fn doctype(text: &str) -> XmlError {
    let rest = text.find("<!DOCTYPE").map_or("", |at| &text[at + "<!DOCTYPE".len()..]);
    let mut rest = rest.trim_start();
    let end = rest
        .find(|ch: char| ch.is_whitespace() || ch == '[' || ch == '>')
        .unwrap_or(rest.len());
    let name = rest[..end].to_owned();
    rest = rest[end..].trim_start();
    let literal = |rest: &mut &str| -> Option<String> {
        let trimmed = rest.trim_start();
        let quote = trimmed.chars().next().filter(|ch| *ch == '"' || *ch == '\'')?;
        let body = &trimmed[1..];
        let close = body.find(quote)?;
        *rest = &body[close + 1..];
        Some(body[..close].to_owned())
    };
    let (system_id, public_id) = if let Some(after) = rest.strip_prefix("SYSTEM") {
        rest = after;
        (literal(&mut rest), None)
    } else if let Some(after) = rest.strip_prefix("PUBLIC") {
        rest = after;
        let public_id = literal(&mut rest);
        (literal(&mut rest), public_id)
    } else {
        (None, None)
    };
    XmlError::DtdForbidden {
        name,
        system_id,
        public_id,
    }
}

fn expanded(namespace: Option<&str>, name: &str) -> String {
    match namespace {
        Some(namespace) if !namespace.is_empty() => format!("{{{namespace}}}{name}"),
        _ => name.to_owned(),
    }
}

fn build(node: roxmltree::Node<'_, '_>) -> XmlElement {
    let tag = node.tag_name();
    let mut element = XmlElement {
        tag: expanded(tag.namespace(), tag.name()),
        attributes: node
            .attributes()
            .map(|attribute| {
                (
                    expanded(attribute.namespace(), attribute.name()),
                    attribute.value().to_owned(),
                )
            })
            .collect(),
        text: None,
        children: Vec::new(),
    };
    let mut text = String::new();
    let mut before_first_child = true;
    for child in node.children() {
        if child.is_element() {
            before_first_child = false;
            element.children.push(build(child));
        } else if child.is_text() && before_first_child {
            text.push_str(child.text().unwrap_or_default());
        }
    }
    if !text.is_empty() {
        element.text = Some(text);
    }
    element
}

/// The document as text, decoded as expat decodes bytes.
fn decode(data: &[u8]) -> Result<String, XmlError> {
    if let Some(rest) = data.strip_prefix(b"\xef\xbb\xbf") {
        return utf8(rest);
    }
    if let Some(rest) = data.strip_prefix(b"\xff\xfe") {
        return utf16(rest, false);
    }
    if let Some(rest) = data.strip_prefix(b"\xfe\xff") {
        return utf16(rest, true);
    }
    match declared_encoding(data).as_deref() {
        None | Some("utf-8" | "utf8") => utf8(data),
        Some("iso-8859-1" | "iso8859-1" | "latin-1" | "latin1" | "l1" | "iso_8859-1") => {
            Ok(data.iter().map(|&byte| char::from(byte)).collect())
        }
        Some("us-ascii" | "ascii") => {
            if data.is_ascii() {
                utf8(data)
            } else {
                Err(XmlError::Syntax("not well-formed (invalid token)".to_owned()))
            }
        }
        Some(other) => Err(XmlError::Syntax(format!("unknown encoding: {other}"))),
    }
}

fn utf8(data: &[u8]) -> Result<String, XmlError> {
    String::from_utf8(data.to_vec()).map_err(|_| XmlError::Syntax("not well-formed (invalid token)".to_owned()))
}

fn utf16(data: &[u8], big_endian: bool) -> Result<String, XmlError> {
    if !data.len().is_multiple_of(2) {
        return Err(XmlError::Syntax("not well-formed (invalid token)".to_owned()));
    }
    let units = data.chunks_exact(2).map(|pair| {
        if big_endian {
            u16::from_be_bytes([pair[0], pair[1]])
        } else {
            u16::from_le_bytes([pair[0], pair[1]])
        }
    });
    char::decode_utf16(units)
        .collect::<Result<String, _>>()
        .map_err(|_| XmlError::Syntax("not well-formed (invalid token)".to_owned()))
}

/// The `encoding` of an XML declaration at the very start, lower-cased.
fn declared_encoding(data: &[u8]) -> Option<String> {
    let head = data.strip_prefix(b"<?xml")?;
    let end = head.windows(2).position(|window| window == b"?>")?;
    let declaration = std::str::from_utf8(&head[..end]).ok()?;
    let at = declaration.find("encoding")?;
    let rest = declaration[at + "encoding".len()..].trim_start();
    let rest = rest.strip_prefix('=')?.trim_start();
    let quote = rest.chars().next().filter(|ch| *ch == '"' || *ch == '\'')?;
    let value = rest[1..].split(quote).next()?;
    Some(value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_joins_across_comments_and_cdata() {
        let root = fromstring(b"<a>x<!-- c -->y<![CDATA[<z>]]><b>in</b>tail</a>").unwrap();
        assert_eq!(root.text.as_deref(), Some("xy<z>"));
        assert_eq!(root.findtext("b"), Some("in"));
        assert_eq!(root.findtext("c"), None);
    }

    #[test]
    fn namespaced_tags_are_expanded() {
        let root = fromstring(br#"<feed xmlns="urn:x" xmlns:c="urn:c"><c:t c:k="v">1</c:t></feed>"#).unwrap();
        assert_eq!(root.tag, "{urn:x}feed");
        let child = root.find("{urn:c}t").unwrap();
        assert_eq!(child.get("{urn:c}k"), Some("v"));
    }

    #[test]
    fn a_doctype_is_forbidden_and_garbage_is_a_syntax_error() {
        let error = fromstring(b"<!DOCTYPE x><x/>").unwrap_err();
        assert_eq!(
            error.to_string(),
            "DTDForbidden(name='x', system_id=None, public_id=None)"
        );
        let error = fromstring(
            br#"<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"><r/>"#,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "DTDForbidden(name='html', system_id='http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd', \
             public_id='-//W3C//DTD XHTML 1.0 Strict//EN')"
        );
        let error = fromstring(br#"<!DOCTYPE r SYSTEM "a.dtd"><r/>"#).unwrap_err();
        assert_eq!(
            error.to_string(),
            "DTDForbidden(name='r', system_id='a.dtd', public_id=None)"
        );
        assert_eq!(
            fromstring(b"<!DOCTYPE x><x/>").unwrap_err().class_name(),
            "DTDForbidden"
        );
        assert!(matches!(fromstring(b"not xml"), Err(XmlError::Syntax(_))));
    }

    #[test]
    fn bytes_are_decoded_by_mark_and_declaration() {
        let latin = b"<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><t>caf\xe9</t>";
        assert_eq!(fromstring(latin).unwrap().text.as_deref(), Some("caf\u{e9}"));
        let utf16: Vec<u8> = [0xff, 0xfe]
            .into_iter()
            .chain("<t>é</t>".encode_utf16().flat_map(u16::to_le_bytes))
            .collect();
        assert_eq!(fromstring(&utf16).unwrap().text.as_deref(), Some("é"));
        assert!(matches!(fromstring(b"<t>\xff</t>"), Err(XmlError::Syntax(_))));
        assert!(matches!(
            fromstring(b"<?xml version=\"1.0\" encoding=\"Shift_JIS\"?><t/>"),
            Err(XmlError::Syntax(_))
        ));
    }
}
