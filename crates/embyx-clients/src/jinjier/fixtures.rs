//! The ranking database the Python tests build (`tests/test_jinjier.py::make_database`), for
//! the jinjier tests here and the playlist tests of the monitor crate.

/// A slice of the real table: every family the rules have to tell apart.
pub const ROWS: &[(i64, i64, &str, &str)] = &[
    // kind, number, name, note
    (
        7,
        1,
        "SSNI-497 新任なのに常にパンスト挑発してくる小悪魔な美脚女教師 橋本ありな",
        "JavDB 有码 TOP250",
    ),
    (
        7,
        2,
        "IPX-811 -媚薬で翌朝まで覚醒絶頂- キメセク相部屋NTR姦",
        "JavDB 有码 TOP250",
    ),
    (7, 3, "ssni-497 same title, other spelling", "JavDB 有码 TOP250"),
    (7, 4, "ABP-984 中出し 射精執行官 05", "JavDB 有码 TOP250"),
    (6, 1, "LAFBD-41  無碼 ラフォーレ ガール Vol.41", "JavDB TOP250"),
    (6, 2, "SSNI-497 新任なのに", "JavDB TOP250"),
    (8, 1, "LAFBD-41  無碼 ラフォーレ ガール Vol.41", "JavDB 无码 TOP250"),
    (8, 2, "SKY-247 sky angel", "JavDB 无码 TOP250"),
    (
        8,
        3,
        "SERO-0127 sneaks into the censored year list untagged",
        "JavDB 无码 TOP250",
    ),
    (5, 1, "START-257 苦手な同僚と", "JavLibray TOP500"),
    (
        5,
        2,
        "SHKD-724 オフィスレディの湿ったパンスト 桜木凛",
        "JavLibray TOP500",
    ),
    (2024, 1, "MIAB-317 不登校オタク生徒を", "JavDB 2024 TOP250"),
    (2024, 2, "blacked.24.05.10 Our Little Secret", "JavDB 2024 TOP250"),
    (
        2024,
        3,
        "FC2-4025269 【ここに舞い降りし神秘の秘宝】",
        "JavDB 2024 TOP250",
    ),
    (2024, 4, "010115_001 caribbeancom", "JavDB 2024 TOP250"),
    (
        2024,
        5,
        "SERO-127 untagged but listed as uncensored",
        "JavDB 2024 TOP250",
    ),
    (2024, 6, "HEYZO-1031 heyzo", "JavDB 2024 TOP250"),
    (2024, 7, "SONE-308 美脚下半身が", "JavDB 2024 TOP250"),
    (2024, 8, "TEK-099 18Gold 金松季歩", "JavDB 2024 TOP250"),
    (
        2024,
        9,
        "TEK-099 18Gold 金松季歩 （ブルーレイディスク）",
        "JavDB 2024 TOP250",
    ),
    (4, 231, "MOON-018 いつも濡れすぎて", "第四届JAV金鸡儿奖 最佳故事片 提名"),
    (4, 232, "START-073 MINAMO ZOMBIE", "第四届JAV金鸡儿奖 最佳故事片 提名"),
    (4, 240, "きとるね川口", "第四届JAV金鸡儿奖 最佳导演 提名"),
    (4, 241, "木村浩之", "第四届JAV金鸡儿奖 最佳导演 提名"),
    (4, 250, "START-196 ある日", "第四届JAV金鸡儿奖 最佳故事片 获奖"),
    (2, 1, "FWAY-088 MONSTER 瀬戸環奈", "影片榜 - 2025年12月"),
    (3, 1, "TEK-099 18Gold", "影片榜 - 2024上半年"),
    (0, 1, "涼森れむ", "女优榜 - 2025年12月"),
    (9, 1, "BangBus.16.07.13 The Bus Gets Recognized", "JavDB 欧美 TOP250"),
    (10, 1, "FC2-3061625 人生初めての", "JavDB FC2 TOP250"),
];

/// The connection's main database as bytes.
pub fn serialize(connection: &rusqlite::Connection) -> Vec<u8> {
    connection.serialize(rusqlite::MAIN_DB).expect("serialize").to_vec()
}

/// `make_database(rows)`: the rows in a fresh SQLite `ranks` table, serialized.
pub fn make_database(rows: &[(i64, i64, &str, &str)]) -> Vec<u8> {
    let connection = rusqlite::Connection::open_in_memory().expect("open");
    connection
        .execute(
            "CREATE TABLE ranks (id INTEGER PRIMARY KEY, kind INTEGER, number INTEGER, name TEXT, date TEXT, \
             note TEXT, icon_url TEXT)",
            [],
        )
        .expect("create");
    for (kind, number, name, note) in rows {
        connection
            .execute(
                "INSERT INTO ranks (kind, number, name, date, note, icon_url) VALUES (?, ?, ?, ?, ?, ?)",
                rusqlite::params![kind, number, name, "2024-01-01", note, ""],
            )
            .expect("insert");
    }
    serialize(&connection)
}
