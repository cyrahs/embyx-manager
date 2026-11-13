//! pyquery over lxml, the way the JavBus client and the feed magnet tables read HTML.
//!
//! Python origin: `PyQuery(text)` (pyquery 2.1, lxml 5.4) as `clients/javbus.py` and
//! `clients/rss_magnet.py` call it, and `etree.fromstring(text, etree.HTMLParser())` as nyaapy
//! calls it for sukebei. pyquery parses in this order, and so does [`Document::parse`]:
//!
//! 1. text that is empty after `str.strip()` is an empty document (no element, no error);
//! 2. `etree.fromstring(text)`: if the text is well-formed XML it becomes an XML tree, where
//!    `A` is not `a` and a `<tr>` outside a table is still a row; a `str` that declares an
//!    encoding is refused with lxml's `ValueError`;
//! 3. otherwise `lxml.html.fromstring(text)`, libxml2's HTML parser, which fails with
//!    `ParserError: Document is empty` when the text has no start tag and no text.
//!
//! The XML tree is built from `roxmltree`; the HTML tree comes from html5ever (`scraper`),
//! which differs from libxml2 where the HTML5 algorithm adds elements of its own: an implied
//! `tbody` (and `tr`, `colgroup`) in a table, a formatting element re-opened after a block.
//! Every element html5ever creates without a start tag for it, other than `html`, `head` and
//! `body`, is therefore unwrapped (its children take its place), which is what libxml2's tree
//! has there (`goldens/html/selectors.json`). A table part (`<tr>`, `<td>`, ...) outside a
//! table, which libxml2 keeps and html5ever drops, is kept by opening a table of no start tag
//! of its own before it, closed again before the next content that is not part of a table, and
//! unwrapped like the rest (JavBus's magnet answer is bare rows).
//! What still differs is listed in docs/DIVERGENCES.md (D26).
//!
//! Selections follow pyquery: `doc(selector)` and `selection(selector)` match the
//! descendants of each element *and the element itself*, each compound of a descendant
//! selector inside the previous one (cssselect's `descendant-or-self::a/descendant-or-self::*/b`),
//! in document order without repeats. `.text()` is pyquery's whitespace-squashing
//! `extract_text`, `.attr()` reads the first element.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::LazyLock;

use ego_tree::NodeId;
use html5ever::TokenizerResult;
use html5ever::tendril::StrTendril;
use html5ever::tokenizer::{BufferQueue, TagKind, Token, TokenSink, TokenSinkResult, Tokenizer};
use html5ever::tree_builder::{ElementFlags, NodeOrText, QuirksMode, TreeBuilder, TreeSink};
use html5ever::{Attribute, LocalName, Namespace, QualName, local_name, ns};
use scraper::node::{Comment, Element, Node, ProcessingInstruction, Text};
use scraper::{ElementRef, Html, HtmlTreeSink, Selector};

/// What pyquery or lxml raises instead of a document.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// `lxml.etree.ParserError: Document is empty`: no start tag and no text.
    #[error("Document is empty")]
    Empty,
    /// lxml's `ValueError` for a `str` with an XML encoding declaration.
    #[error(
        "Unicode strings with encoding declaration are not supported. Please use bytes input or XML fragments without \
         declaration."
    )]
    EncodingDeclaration,
}

impl ParseError {
    /// `type(exc).__name__`.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Empty => "ParserError",
            Self::EncodingDeclaration => "ValueError",
        }
    }
}

/// A parsed page: pyquery's root, or an empty selection.
pub struct Document {
    html: Html,
    /// False for pyquery's empty document (whitespace-only text): every selection is empty.
    has_root: bool,
    /// Every node's position in document order, for ordering and de-duplicating selections.
    order: HashMap<NodeId, usize>,
}

impl fmt::Debug for Document {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Document")
            .field("has_root", &self.has_root)
            .field("nodes", &self.order.len())
            .finish_non_exhaustive()
    }
}

/// lxml's `_hasEncodingDeclaration`: `^(<\?xml[^>]+)\s+encoding\s*=\s*["'][^"']*["'](\s*\?>|)`.
static XML_ENCODING_DECLARATION: LazyLock<regex::Regex> = LazyLock::new(|| {
    embyx_core::pyre::compile(
        r#"^(<\?xml[^>]+)\s+encoding\s*=\s*["'][^"']*["'](\s*\?>|)"#,
        false,
        false,
    )
});

impl Document {
    /// `PyQuery(text)`.
    pub fn parse(text: &str) -> Result<Self, ParseError> {
        if embyx_core::pytext::py_strip(text).is_empty() {
            return Ok(Self::new(Html::new_document(), false));
        }
        if XML_ENCODING_DECLARATION.is_match(text) {
            return Err(ParseError::EncodingDeclaration);
        }
        if let Some(html) = xml_tree(text) {
            return Ok(Self::new(html, true));
        }
        Self::parse_html(text)
    }

    /// `lxml.html.fromstring(text)` / `etree.fromstring(text, etree.HTMLParser())`, without the
    /// XML attempt (nyaapy).
    pub fn parse_html(text: &str) -> Result<Self, ParseError> {
        if XML_ENCODING_DECLARATION.is_match(text) {
            return Err(ParseError::EncodingDeclaration);
        }
        let (html, has_content) = html_tree(text);
        if !has_content {
            return Err(ParseError::Empty);
        }
        Ok(Self::new(html, true))
    }

    fn new(html: Html, has_root: bool) -> Self {
        let order = html
            .tree
            .root()
            .descendants()
            .enumerate()
            .map(|(index, node)| (node.id(), index))
            .collect();
        Self { html, has_root, order }
    }

    /// `doc(selector)`.
    pub fn select(&self, selector: &str) -> Selection<'_> {
        if !self.has_root {
            return Selection {
                document: self,
                elements: Vec::new(),
            };
        }
        let roots: Vec<ElementRef<'_>> = self.html.tree.root().children().filter_map(ElementRef::wrap).collect();
        Selection {
            document: self,
            elements: roots,
        }
        .select(selector)
    }

    /// Whether the text parsed as XML (pyquery's first try), for the probes and goldens.
    pub fn is_xml(&self) -> bool {
        self.html
            .tree
            .root()
            .children()
            .filter_map(ElementRef::wrap)
            .any(|element| element.value().name.ns != ns!(html))
    }
}

/// A pyquery selection: elements in document order.
#[derive(Clone)]
pub struct Selection<'a> {
    document: &'a Document,
    elements: Vec<ElementRef<'a>>,
}

impl fmt::Debug for Selection<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.elements.iter().map(|element| element.value().name()))
            .finish()
    }
}

/// One compound of a selector (`td:first-child`, `a[href^="magnet"]`).
fn compound(text: &str) -> Selector {
    // The selectors are the code's own literals; a bad one is a programming error.
    Selector::parse(text).unwrap_or_else(|error| panic!("selector {text:?}: {error}"))
}

impl<'a> Selection<'a> {
    /// `selection(selector)`: descendant-or-self of every element, compound by compound.
    #[must_use]
    pub fn select(&self, selector: &str) -> Selection<'a> {
        let mut scopes = self.elements.clone();
        for (index, part) in selector.split_whitespace().enumerate() {
            let matcher = compound(part);
            let mut found: HashSet<NodeId> = HashSet::new();
            let mut next = Vec::new();
            for scope in &scopes {
                // The first compound may be the scope itself; later ones are below the match.
                let skip = usize::from(index > 0);
                for node in scope.descendants().skip(skip) {
                    if let Some(element) = ElementRef::wrap(node)
                        && matcher.matches(&element)
                        && found.insert(element.id())
                    {
                        next.push(element);
                    }
                }
            }
            next.sort_by_key(|element| self.document.order.get(&element.id()).copied().unwrap_or(usize::MAX));
            scopes = next;
        }
        Selection {
            document: self.document,
            elements: scopes,
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// `.items()`: each element as a selection of its own.
    pub fn items(&self) -> impl Iterator<Item = Selection<'a>> + '_ {
        self.elements.iter().map(|element| Selection {
            document: self.document,
            elements: vec![*element],
        })
    }

    /// `.eq(index)`.
    #[must_use]
    pub fn eq(&self, index: usize) -> Selection<'a> {
        Selection {
            document: self.document,
            elements: self.elements.get(index).copied().into_iter().collect(),
        }
    }

    /// `.filter(predicate)`.
    #[must_use]
    pub fn filter(&self, mut predicate: impl FnMut(&Selection<'a>) -> bool) -> Selection<'a> {
        Selection {
            document: self.document,
            elements: self
                .items()
                .filter(|item| predicate(item))
                .flat_map(|item| item.elements)
                .collect(),
        }
    }

    /// `.attr(name)`: the first element's attribute; `None` when there is no element or it
    /// lacks the attribute.
    pub fn attr(&self, name: &str) -> Option<String> {
        self.elements.first()?.value().attr(name).map(str::to_owned)
    }

    /// `.text()`: each element's squashed text, joined with a space.
    pub fn text(&self) -> String {
        self.elements
            .iter()
            .map(|element| extract_text(*element))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The elements, for callers that read lxml's own `.text` / `.tail` model.
    pub fn elements(&self) -> &[ElementRef<'a>] {
        &self.elements
    }
}

// ---------- pyquery.text.extract_text ----------

/// pyquery's `INLINE_TAGS`.
const INLINE_TAGS: [&str; 32] = [
    "a", "abbr", "acronym", "b", "bdo", "big", "br", "button", "cite", "code", "dfn", "em", "i", "img", "input", "kbd",
    "label", "map", "object", "q", "samp", "script", "select", "small", "span", "strong", "sub", "sup", "textarea",
    "time", "tt", "var",
];

/// One entry of pyquery's text array: a string, an artificial newline (`None`) or a `<br>`
/// separator (`True`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    Text(String),
    Newline,
    Separator,
}

/// lxml's tag for the purposes of `extract_text`: the local name, or `{ns}name` for an XML
/// element in a namespace (which is never inline).
fn lxml_tag(element: ElementRef<'_>) -> String {
    element.value().name().to_owned()
}

fn extract_text_array(element: ElementRef<'_>, squash: bool, strip: bool) -> Vec<Part> {
    let tag = lxml_tag(element);
    let inline = INLINE_TAGS.contains(&tag.as_str());
    let mut parts = Vec::new();
    if tag == "br" {
        parts.push(Part::Separator);
    } else if !inline {
        parts.push(Part::Newline);
    }
    // `dom.text`, then each child's parts and its tail: in a node tree that is every text
    // child in order (a comment or processing instruction adds nothing but its tail).
    for child in element.children() {
        match child.value() {
            Node::Text(text) => parts.push(Part::Text(text.to_string())),
            Node::Element(_) => {
                if let Some(child) = ElementRef::wrap(child) {
                    parts.extend(extract_text_array(child, false, false));
                }
            }
            _ => {}
        }
    }
    if !inline && tag != "br" {
        parts.push(Part::Newline);
    }
    if squash {
        parts = squash_artificial_newlines(parts);
    }
    if strip {
        parts = strip_artificial_newlines(parts);
    }
    parts
}

fn squash_artificial_newlines(parts: Vec<Part>) -> Vec<Part> {
    let mut output = Vec::with_capacity(parts.len());
    let mut last_newline = false;
    for part in parts {
        if part == Part::Newline {
            if !last_newline {
                output.push(Part::Newline);
                last_newline = true;
            }
        } else {
            output.push(part);
            last_newline = false;
        }
    }
    output
}

/// `_strip_artifical_nl`, index arithmetic and all.
fn strip_artificial_newlines(parts: Vec<Part>) -> Vec<Part> {
    if parts.is_empty() {
        return parts;
    }
    let is_text = |part: &Part| matches!(part, Part::Text(_));
    // `for start_idx, pt in enumerate(parts): if str: break` leaves the last index when none is.
    let start = parts.iter().position(is_text).unwrap_or(parts.len() - 1);
    // `parts[:start_idx - 1 if start_idx > 0 else None:-1]`, enumerated from the end.
    let reversed: Vec<&Part> = if start > 0 {
        // A slice from the end down to (not including) start - 1.
        parts.iter().rev().take(parts.len() - start).collect()
    } else {
        parts.iter().rev().collect()
    };
    let mut end = 0;
    for (index, part) in reversed.iter().enumerate() {
        end = index;
        if is_text(part) {
            break;
        }
    }
    let stop = if end > 0 { parts.len() - end } else { parts.len() };
    if start >= stop {
        return Vec::new();
    }
    parts[start..stop].to_vec()
}

/// pyquery's `WHITESPACE_RE` (`[\x20\x09\x0C​\x0A\x0D]+`) replaced by one space.
fn squash_html_whitespace(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut in_space = false;
    for ch in text.chars() {
        if matches!(ch, ' ' | '\t' | '\x0c' | '\u{200b}' | '\n' | '\r') {
            if !in_space {
                output.push(' ');
                in_space = true;
            }
        } else {
            output.push(ch);
            in_space = false;
        }
    }
    output
}

fn merge_original_parts(parts: Vec<Part>) -> Vec<Part> {
    let mut output = Vec::new();
    let mut buffer = String::new();
    let mut buffered = false;
    let flush = |output: &mut Vec<Part>, buffer: &mut String, buffered: &mut bool| {
        if *buffered {
            let item = embyx_core::pytext::py_strip(&squash_html_whitespace(buffer)).to_owned();
            if !item.is_empty() {
                output.push(Part::Text(item));
            }
            buffer.clear();
            *buffered = false;
        }
    };
    for part in parts {
        match part {
            Part::Text(text) => {
                buffer.push_str(&text);
                buffered = true;
            }
            other => {
                flush(&mut output, &mut buffer, &mut buffered);
                output.push(other);
            }
        }
    }
    flush(&mut output, &mut buffer, &mut buffered);
    output
}

/// `pyquery.text.extract_text(element)` with its defaults (`squash_space=True`).
pub fn extract_text(element: ElementRef<'_>) -> String {
    let parts = extract_text_array(element, true, true);
    let parts = strip_artificial_newlines(squash_artificial_newlines(merge_original_parts(parts)));
    let joined: String = parts
        .iter()
        .map(|part| match part {
            Part::Text(text) => text.as_str(),
            Part::Newline | Part::Separator => "\n",
        })
        .collect();
    embyx_core::pytext::py_strip(&joined).to_owned()
}

/// lxml's `element.text`: the text before the first child element (comments are skipped,
/// so text on both sides of one is `text` and a comment's `tail`; nyaapy reads only `text`,
/// which stops at the first element or comment). `None` when there is none.
pub fn lxml_text(element: ElementRef<'_>) -> Option<String> {
    let mut text = String::new();
    for child in element.children() {
        match child.value() {
            Node::Text(part) => text.push_str(part),
            _ => break,
        }
    }
    (!text.is_empty()).then_some(text)
}

// ---------- the XML tree (etree.fromstring) ----------

/// The text as an XML tree, when lxml's XML parser would take it.
fn xml_tree(text: &str) -> Option<Html> {
    let options = roxmltree::ParsingOptions {
        allow_dtd: true,
        ..roxmltree::ParsingOptions::default()
    };
    let document = roxmltree::Document::parse_with_options(text, options).ok()?;
    let mut html = Html::new_document();
    let root = html.tree.root().id();
    append_xml(&mut html, root, document.root_element());
    Some(html)
}

/// lxml's name for an XML element or attribute: the local name, or `{namespace}name`. Kept
/// out of the HTML namespace so selectors match it the way lxml's `XPath` does (case-sensitive,
/// and a namespaced element is never `a`).
fn xml_name(namespace: Option<&str>, name: &str) -> QualName {
    let local = match namespace {
        Some(namespace) if !namespace.is_empty() => format!("{{{namespace}}}{name}"),
        _ => name.to_owned(),
    };
    QualName::new(None, Namespace::from(""), LocalName::from(local))
}

fn append_xml(html: &mut Html, parent: NodeId, node: roxmltree::Node<'_, '_>) {
    let value = match node.node_type() {
        roxmltree::NodeType::Element => {
            let attributes = node
                .attributes()
                .map(|attribute| Attribute {
                    name: xml_name(attribute.namespace(), attribute.name()),
                    value: StrTendril::from(attribute.value()),
                })
                .collect();
            let tag = node.tag_name();
            Node::Element(Element::new(xml_name(tag.namespace(), tag.name()), attributes))
        }
        roxmltree::NodeType::Text => Node::Text(Text {
            text: node.text().unwrap_or_default().into(),
        }),
        roxmltree::NodeType::Comment => Node::Comment(Comment {
            comment: node.text().unwrap_or_default().into(),
        }),
        roxmltree::NodeType::PI => {
            let pi = node.pi().expect("a processing instruction");
            Node::ProcessingInstruction(ProcessingInstruction {
                target: pi.target.into(),
                data: pi.value.unwrap_or_default().into(),
            })
        }
        roxmltree::NodeType::Root => return,
    };
    let id = {
        let mut parent = html.tree.get_mut(parent).expect("parent node");
        parent.append(value).id()
    };
    if node.is_element() {
        for child in node.children() {
            append_xml(html, id, child);
        }
    }
}

// ---------- the HTML tree (lxml.html.fromstring) ----------

/// Table parts html5ever ignores outside a table.
fn is_table_part(name: &LocalName) -> bool {
    matches!(
        *name,
        local_name!("tr")
            | local_name!("td")
            | local_name!("th")
            | local_name!("tbody")
            | local_name!("thead")
            | local_name!("tfoot")
            | local_name!("caption")
            | local_name!("col")
            | local_name!("colgroup")
    )
}

/// The tree html5ever builds, with the elements it implied unwrapped, and whether the text had
/// any start tag or non-blank text (else libxml2's document is empty).
fn html_tree(text: &str) -> (Html, bool) {
    let sink = TrackingSink {
        inner: HtmlTreeSink::new(Html::new_document()),
        current_start_tag: std::cell::RefCell::new(None),
        implied: std::cell::RefCell::new(Vec::new()),
    };
    let builder = TreeBuilder::new(sink, html5ever::tree_builder::TreeBuilderOpts::default());
    let tokenizer = Tokenizer::new(
        TokenWatcher {
            builder,
            content: std::cell::Cell::new(false),
            tables: std::cell::Cell::new(0),
            stray_table: std::cell::Cell::new(false),
            in_cell: std::cell::Cell::new(false),
        },
        html5ever::tokenizer::TokenizerOpts::default(),
    );
    let input = BufferQueue::default();
    input.push_back(StrTendril::from(text));
    while !matches!(tokenizer.feed(&input), TokenizerResult::Done) {}
    tokenizer.end();
    let has_content = tokenizer.sink.content.get();
    let TokenWatcher { builder, .. } = tokenizer.sink;
    let TrackingSink { inner, implied, .. } = builder.sink;
    let mut html = inner.0.into_inner();
    for id in implied.into_inner() {
        unwrap_node(&mut html, id);
    }
    (html, has_content)
}

/// Move a node's children into its place and drop it.
fn unwrap_node(html: &mut Html, id: NodeId) {
    let children: Vec<NodeId> = match html.tree.get(id) {
        Some(node) if node.parent().is_some() => node.children().map(|child| child.id()).collect(),
        _ => return,
    };
    for child in children {
        if let Some(mut node) = html.tree.get_mut(id) {
            node.insert_id_before(child);
        }
    }
    if let Some(mut node) = html.tree.get_mut(id) {
        node.detach();
    }
}

/// Passes tokens to the tree builder, noting the start tag being processed (for
/// [`TrackingSink`]) and whether libxml2 would see any content.
struct TokenWatcher {
    builder: TreeBuilder<NodeId, TrackingSink>,
    content: std::cell::Cell<bool>,
    /// `<table>` start tags of the text not yet closed.
    tables: std::cell::Cell<usize>,
    /// A table this opened for table parts outside any table is open.
    stray_table: std::cell::Cell<bool>,
    /// Inside a cell of that table, where any content belongs to the cell.
    in_cell: std::cell::Cell<bool>,
}

impl TokenWatcher {
    /// A tag of no start tag in the text: the element it makes counts as implied.
    fn synthetic(&self, kind: TagKind, line_number: u64) {
        let tag = html5ever::tokenizer::Tag {
            kind,
            name: local_name!("table"),
            self_closing: false,
            had_duplicate_attributes: false,
            attrs: Vec::new(),
        };
        let _ = self.builder.process_token(Token::TagToken(tag), line_number);
    }

    /// Open a table before a stray table part, close it before content that is not one.
    fn keep_stray_table_parts(&self, token: &Token, line_number: u64) {
        match token {
            Token::TagToken(tag) => {
                let part = is_table_part(&tag.name);
                let start = tag.kind == TagKind::StartTag;
                if self.stray_table.get() {
                    if tag.name == local_name!("table") && !self.in_cell.get() {
                        // A table of the text's own ends the stray one.
                        self.synthetic(TagKind::EndTag, line_number);
                        self.stray_table.set(false);
                    } else if part {
                        self.in_cell
                            .set(start && matches!(tag.name, local_name!("td") | local_name!("th")));
                    } else if !self.in_cell.get() {
                        self.synthetic(TagKind::EndTag, line_number);
                        self.stray_table.set(false);
                    }
                } else if part && start && self.tables.get() == 0 {
                    self.synthetic(TagKind::StartTag, line_number);
                    self.stray_table.set(true);
                    self.in_cell
                        .set(matches!(tag.name, local_name!("td") | local_name!("th")));
                }
                if tag.name == local_name!("table") {
                    let tables = self.tables.get();
                    self.tables
                        .set(if start { tables + 1 } else { tables.saturating_sub(1) });
                }
            }
            Token::CharacterTokens(text)
                if self.stray_table.get()
                    && !self.in_cell.get()
                    && text.chars().any(|ch| !matches!(ch, ' ' | '\t' | '\n' | '\r' | '\x0c')) =>
            {
                self.synthetic(TagKind::EndTag, line_number);
                self.stray_table.set(false);
            }
            _ => {}
        }
    }
}

impl TokenSink for TokenWatcher {
    type Handle = NodeId;

    fn process_token(&self, token: Token, line_number: u64) -> TokenSinkResult<NodeId> {
        let start = match &token {
            Token::TagToken(tag) if tag.kind == TagKind::StartTag => {
                self.content.set(true);
                Some(tag.name.clone())
            }
            // libxml2's IS_BLANK: space, tab, newline, carriage return.
            Token::CharacterTokens(text) if text.chars().any(|ch| !matches!(ch, ' ' | '\t' | '\n' | '\r')) => {
                self.content.set(true);
                None
            }
            _ => None,
        };
        self.keep_stray_table_parts(&token, line_number);
        *self.builder.sink.current_start_tag.borrow_mut() = start;
        let result = self.builder.process_token(token, line_number);
        *self.builder.sink.current_start_tag.borrow_mut() = None;
        result
    }

    fn end(&self) {
        self.builder.end();
    }

    fn adjusted_current_node_present_but_not_in_html_namespace(&self) -> bool {
        self.builder.adjusted_current_node_present_but_not_in_html_namespace()
    }
}

/// scraper's tree sink, recording the elements created without their own start tag.
struct TrackingSink {
    inner: HtmlTreeSink,
    current_start_tag: std::cell::RefCell<Option<LocalName>>,
    implied: std::cell::RefCell<Vec<NodeId>>,
}

impl TreeSink for TrackingSink {
    type Handle = NodeId;
    type Output = Html;
    type ElemName<'a> = std::cell::Ref<'a, QualName>;

    fn finish(self) -> Html {
        self.inner.finish()
    }

    fn parse_error(&self, msg: std::borrow::Cow<'static, str>) {
        self.inner.parse_error(msg);
    }

    fn get_document(&self) -> NodeId {
        self.inner.get_document()
    }

    fn elem_name<'a>(&'a self, target: &'a NodeId) -> Self::ElemName<'a> {
        self.inner.elem_name(target)
    }

    fn create_element(&self, name: QualName, attrs: Vec<Attribute>, flags: ElementFlags) -> NodeId {
        let structural = matches!(
            name.local,
            local_name!("html") | local_name!("head") | local_name!("body")
        );
        let own_tag = self.current_start_tag.borrow().as_ref() == Some(&name.local);
        let id = self.inner.create_element(name, attrs, flags);
        if !structural && !own_tag {
            self.implied.borrow_mut().push(id);
        }
        id
    }

    fn create_comment(&self, text: StrTendril) -> NodeId {
        self.inner.create_comment(text)
    }

    fn create_pi(&self, target: StrTendril, data: StrTendril) -> NodeId {
        self.inner.create_pi(target, data)
    }

    fn append(&self, parent: &NodeId, child: NodeOrText<NodeId>) {
        self.inner.append(parent, child);
    }

    fn append_based_on_parent_node(&self, element: &NodeId, prev_element: &NodeId, child: NodeOrText<NodeId>) {
        self.inner.append_based_on_parent_node(element, prev_element, child);
    }

    fn append_doctype_to_document(&self, name: StrTendril, public_id: StrTendril, system_id: StrTendril) {
        self.inner.append_doctype_to_document(name, public_id, system_id);
    }

    fn get_template_contents(&self, target: &NodeId) -> NodeId {
        self.inner.get_template_contents(target)
    }

    fn same_node(&self, x: &NodeId, y: &NodeId) -> bool {
        self.inner.same_node(x, y)
    }

    fn set_quirks_mode(&self, mode: QuirksMode) {
        self.inner.set_quirks_mode(mode);
    }

    fn append_before_sibling(&self, sibling: &NodeId, new_node: NodeOrText<NodeId>) {
        self.inner.append_before_sibling(sibling, new_node);
    }

    fn add_attrs_if_missing(&self, target: &NodeId, attrs: Vec<Attribute>) {
        self.inner.add_attrs_if_missing(target, attrs);
    }

    fn remove_from_parent(&self, target: &NodeId) {
        self.inner.remove_from_parent(target);
    }

    fn reparent_children(&self, node: &NodeId, new_parent: &NodeId) {
        self.inner.reparent_children(node, new_parent);
    }
}

#[cfg(test)]
mod tests;
