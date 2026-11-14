//! `mimetypes.guess_type` as Starlette's `FileResponse` calls it, on `CPython`'s built-in tables.
//!
//! Python origin: `mimetypes.py` of `CPython` 3.13 (`_types_map_default`, `_encodings_map_default`,
//! `_suffix_map_default`, `MimeTypes._guess_file_type`). Python also reads the system's
//! `mime.types` files when they exist; the image has none, so the built-in tables are what
//! it answers with (D53). `goldens/api/mimetypes.json` holds Python's tables and guesses.

/// `types_map[True]` (strict): lower-case extension to type, sorted for the binary search.
const TYPES: &[(&str, &str)] = &[
    (".3g2", "audio/3gpp2"),
    (".3gp", "audio/3gpp"),
    (".3gpp", "audio/3gpp"),
    (".3gpp2", "audio/3gpp2"),
    (".a", "application/octet-stream"),
    (".aac", "audio/aac"),
    (".adts", "audio/aac"),
    (".ai", "application/postscript"),
    (".aif", "audio/x-aiff"),
    (".aifc", "audio/x-aiff"),
    (".aiff", "audio/x-aiff"),
    (".ass", "audio/aac"),
    (".au", "audio/basic"),
    (".avi", "video/x-msvideo"),
    (".avif", "image/avif"),
    (".bat", "text/plain"),
    (".bcpio", "application/x-bcpio"),
    (".bin", "application/octet-stream"),
    (".bmp", "image/bmp"),
    (".c", "text/plain"),
    (".cdf", "application/x-netcdf"),
    (".cpio", "application/x-cpio"),
    (".csh", "application/x-csh"),
    (".css", "text/css"),
    (".csv", "text/csv"),
    (".dll", "application/octet-stream"),
    (".doc", "application/msword"),
    (".dot", "application/msword"),
    (".dvi", "application/x-dvi"),
    (".eml", "message/rfc822"),
    (".eps", "application/postscript"),
    (".etx", "text/x-setext"),
    (".exe", "application/octet-stream"),
    (".gif", "image/gif"),
    (".gtar", "application/x-gtar"),
    (".h", "text/plain"),
    (".h5", "application/x-hdf5"),
    (".hdf", "application/x-hdf"),
    (".heic", "image/heic"),
    (".heif", "image/heif"),
    (".htm", "text/html"),
    (".html", "text/html"),
    (".ico", "image/vnd.microsoft.icon"),
    (".ief", "image/ief"),
    (".jpe", "image/jpeg"),
    (".jpeg", "image/jpeg"),
    (".jpg", "image/jpeg"),
    (".js", "text/javascript"),
    (".json", "application/json"),
    (".ksh", "text/plain"),
    (".latex", "application/x-latex"),
    (".loas", "audio/aac"),
    (".m1v", "video/mpeg"),
    (".m3u", "application/vnd.apple.mpegurl"),
    (".m3u8", "application/vnd.apple.mpegurl"),
    (".man", "application/x-troff-man"),
    (".markdown", "text/markdown"),
    (".md", "text/markdown"),
    (".me", "application/x-troff-me"),
    (".mht", "message/rfc822"),
    (".mhtml", "message/rfc822"),
    (".mif", "application/x-mif"),
    (".mjs", "text/javascript"),
    (".mov", "video/quicktime"),
    (".movie", "video/x-sgi-movie"),
    (".mp2", "audio/mpeg"),
    (".mp3", "audio/mpeg"),
    (".mp4", "video/mp4"),
    (".mpa", "video/mpeg"),
    (".mpe", "video/mpeg"),
    (".mpeg", "video/mpeg"),
    (".mpg", "video/mpeg"),
    (".ms", "application/x-troff-ms"),
    (".n3", "text/n3"),
    (".nc", "application/x-netcdf"),
    (".nq", "application/n-quads"),
    (".nt", "application/n-triples"),
    (".nws", "message/rfc822"),
    (".o", "application/octet-stream"),
    (".obj", "application/octet-stream"),
    (".oda", "application/oda"),
    (".opus", "audio/opus"),
    (".p12", "application/x-pkcs12"),
    (".p7c", "application/pkcs7-mime"),
    (".pbm", "image/x-portable-bitmap"),
    (".pdf", "application/pdf"),
    (".pfx", "application/x-pkcs12"),
    (".pgm", "image/x-portable-graymap"),
    (".pl", "text/plain"),
    (".png", "image/png"),
    (".pnm", "image/x-portable-anymap"),
    (".pot", "application/vnd.ms-powerpoint"),
    (".ppa", "application/vnd.ms-powerpoint"),
    (".ppm", "image/x-portable-pixmap"),
    (".pps", "application/vnd.ms-powerpoint"),
    (".ppt", "application/vnd.ms-powerpoint"),
    (".ps", "application/postscript"),
    (".pwz", "application/vnd.ms-powerpoint"),
    (".py", "text/x-python"),
    (".pyc", "application/x-python-code"),
    (".pyo", "application/x-python-code"),
    (".qt", "video/quicktime"),
    (".ra", "audio/x-pn-realaudio"),
    (".ram", "application/x-pn-realaudio"),
    (".ras", "image/x-cmu-raster"),
    (".rdf", "application/xml"),
    (".rgb", "image/x-rgb"),
    (".roff", "application/x-troff"),
    (".rst", "text/x-rst"),
    (".rtf", "text/rtf"),
    (".rtx", "text/richtext"),
    (".sgm", "text/x-sgml"),
    (".sgml", "text/x-sgml"),
    (".sh", "application/x-sh"),
    (".shar", "application/x-shar"),
    (".snd", "audio/basic"),
    (".so", "application/octet-stream"),
    (".src", "application/x-wais-source"),
    (".srt", "text/plain"),
    (".sv4cpio", "application/x-sv4cpio"),
    (".sv4crc", "application/x-sv4crc"),
    (".svg", "image/svg+xml"),
    (".swf", "application/x-shockwave-flash"),
    (".t", "application/x-troff"),
    (".tar", "application/x-tar"),
    (".tcl", "application/x-tcl"),
    (".tex", "application/x-tex"),
    (".texi", "application/x-texinfo"),
    (".texinfo", "application/x-texinfo"),
    (".tif", "image/tiff"),
    (".tiff", "image/tiff"),
    (".tr", "application/x-troff"),
    (".trig", "application/trig"),
    (".tsv", "text/tab-separated-values"),
    (".txt", "text/plain"),
    (".ustar", "application/x-ustar"),
    (".vcf", "text/x-vcard"),
    (".vtt", "text/vtt"),
    (".wasm", "application/wasm"),
    (".wav", "audio/x-wav"),
    (".webm", "video/webm"),
    (".webmanifest", "application/manifest+json"),
    (".webp", "image/webp"),
    (".wiz", "application/msword"),
    (".wsdl", "application/xml"),
    (".xbm", "image/x-xbitmap"),
    (".xlb", "application/vnd.ms-excel"),
    (".xls", "application/vnd.ms-excel"),
    (".xml", "text/xml"),
    (".xpdl", "application/xml"),
    (".xpm", "image/x-xpixmap"),
    (".xsl", "application/xml"),
    (".xwd", "image/x-xwindowdump"),
    (".zip", "application/zip"),
];

/// `encodings_map`: matched case-sensitively.
const ENCODINGS: &[(&str, &str)] = &[
    (".Z", "compress"),
    (".br", "br"),
    (".bz2", "bzip2"),
    (".gz", "gzip"),
    (".xz", "xz"),
];

/// `suffix_map`: matched on the lower-cased extension.
const SUFFIXES: &[(&str, &str)] = &[
    (".svgz", ".svg.gz"),
    (".taz", ".tar.gz"),
    (".tbz2", ".tar.bz2"),
    (".tgz", ".tar.gz"),
    (".txz", ".tar.xz"),
    (".tz", ".tar.gz"),
];

fn lookup(table: &'static [(&'static str, &'static str)], key: &str) -> Option<&'static str> {
    table.iter().find(|(known, _)| *known == key).map(|(_, value)| *value)
}

/// `os.path.splitext(path)`: the extension starts at the last dot of the last component,
/// unless that component is all dots up to it (leading dots do not start an extension).
fn splitext(path: &str) -> (&str, &str) {
    let name_start = path.rfind('/').map_or(0, |slash| slash + 1);
    let Some(dot) = path[name_start..].rfind('.').map(|dot| name_start + dot) else {
        return (path, "");
    };
    if path[name_start..dot].bytes().all(|byte| byte == b'.') {
        return (path, "");
    }
    path.split_at(dot)
}

/// `mimetypes.guess_type(path)` for a file path: the type (if any) and the encoding.
pub fn guess_type(path: &str) -> (Option<&'static str>, Option<&'static str>) {
    let (mut base, mut ext) = {
        let (base, ext) = splitext(path);
        (base.to_owned(), ext.to_owned())
    };
    while let Some(replacement) = lookup(SUFFIXES, &ext.to_lowercase()) {
        let joined = format!("{base}{replacement}");
        let (new_base, new_ext) = splitext(&joined);
        (base, ext) = (new_base.to_owned(), new_ext.to_owned());
    }
    let encoding = lookup(ENCODINGS, &ext);
    if encoding.is_some() {
        let (new_base, new_ext) = splitext(&base);
        let (new_base, new_ext) = (new_base.to_owned(), new_ext.to_owned());
        (base, ext) = (new_base, new_ext);
    }
    let _ = base;
    (lookup(TYPES, &ext.to_lowercase()), encoding)
}

/// `FileResponse`'s media type: the guess, else `application/octet-stream`.
pub fn media_type(path: &str) -> &'static str {
    guess_type(path).0.unwrap_or("application/octet-stream")
}

#[cfg(test)]
mod tests {
    use embyx_core::golden;
    use embyx_core::pyjson::Value;

    use super::{ENCODINGS, SUFFIXES, TYPES, guess_type};

    fn table(entries: &[(&str, &str)]) -> Value {
        Value::Dict(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), Value::from(*value)))
                .collect(),
        )
    }

    #[test]
    fn tables_and_guesses_match_python() {
        let document = golden::load("api/mimetypes.json");
        let cases = golden::cases(&document);
        assert_eq!(cases[0].get("types_map"), Some(&table(TYPES)));
        assert_eq!(cases[1].get("encodings_map"), Some(&table(ENCODINGS)));
        assert_eq!(cases[2].get("suffix_map"), Some(&table(SUFFIXES)));
        for case in &cases[3..] {
            let path = case.get("path").and_then(Value::as_str).expect("a path");
            let (media_type, encoding) = guess_type(path);
            let text = |value: Option<&str>| value.map_or(Value::Null, Value::from);
            assert_eq!(Some(&text(media_type)), case.get("type"), "{path}");
            assert_eq!(Some(&text(encoding)), case.get("encoding"), "{path}");
        }
    }
}
