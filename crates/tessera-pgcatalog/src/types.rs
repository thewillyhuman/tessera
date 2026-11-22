// SPDX-License-Identifier: Apache-2.0

//! The types the catalogs name: what `pg_type` holds, and how a type
//! is written out for a reader.

/// One type as `pg_type` describes it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PgType {
    pub oid: i64,
    pub name: &'static str,
    /// Its length in bytes, or -1 for a type of varying length.
    pub len: i64,
    /// The category letter of `typcategory`.
    pub category: char,
    /// The type of its elements, for an array.
    pub elem: i64,
    /// The array type of it, when there is one.
    pub array: i64,
}

const fn base(oid: i64, name: &'static str, len: i64, category: char, array: i64) -> PgType {
    PgType {
        oid,
        name,
        len,
        category,
        elem: 0,
        array,
    }
}

const fn array(oid: i64, name: &'static str, elem: i64) -> PgType {
    PgType {
        oid,
        name,
        len: -1,
        category: 'A',
        elem,
        array: 0,
    }
}

/// The types of `pg_type`: the ones a column here can have, the ones
/// the catalogs' own columns have, and their arrays.
pub(crate) const TYPES: [PgType; 55] = [
    base(16, "bool", 1, 'B', 1000),
    base(17, "bytea", -1, 'U', 1001),
    base(18, "char", 1, 'Z', 1002),
    base(19, "name", 64, 'S', 1003),
    base(20, "int8", 8, 'N', 1016),
    base(21, "int2", 2, 'N', 1005),
    base(22, "int2vector", -1, 'A', 1006),
    base(23, "int4", 4, 'N', 1007),
    base(24, "regproc", 4, 'N', 1008),
    base(25, "text", -1, 'S', 1009),
    base(26, "oid", 4, 'N', 1028),
    base(27, "tid", 6, 'U', 1010),
    base(28, "xid", 4, 'U', 1011),
    base(29, "cid", 4, 'U', 1012),
    base(30, "oidvector", -1, 'A', 1013),
    base(114, "json", -1, 'U', 199),
    base(142, "xml", -1, 'U', 143),
    base(700, "float4", 4, 'N', 1021),
    base(701, "float8", 8, 'N', 1022),
    base(705, "unknown", -2, 'X', 0),
    base(1033, "aclitem", 16, 'U', 1034),
    base(1042, "bpchar", -1, 'S', 1014),
    base(1043, "varchar", -1, 'S', 1015),
    base(1082, "date", 4, 'D', 1182),
    base(1083, "time", 8, 'D', 1183),
    base(1114, "timestamp", 8, 'D', 1115),
    base(1184, "timestamptz", 8, 'D', 1185),
    base(1186, "interval", 16, 'T', 1187),
    base(1700, "numeric", -1, 'N', 1231),
    base(2205, "regclass", 4, 'N', 2210),
    base(2206, "regtype", 4, 'N', 2211),
    base(2249, "record", -1, 'P', 2287),
    base(2278, "void", 4, 'P', 0),
    base(2950, "uuid", 16, 'U', 2951),
    base(3802, "jsonb", -1, 'U', 3807),
    array(199, "_json", 114),
    array(1000, "_bool", 16),
    array(1001, "_bytea", 17),
    array(1002, "_char", 18),
    array(1003, "_name", 19),
    array(1005, "_int2", 21),
    array(1007, "_int4", 23),
    array(1009, "_text", 25),
    array(1014, "_bpchar", 1042),
    array(1015, "_varchar", 1043),
    array(1016, "_int8", 20),
    array(1021, "_float4", 700),
    array(1022, "_float8", 701),
    array(1028, "_oid", 26),
    array(1034, "_aclitem", 1033),
    array(1115, "_timestamp", 1114),
    array(1185, "_timestamptz", 1184),
    array(1231, "_numeric", 1700),
    array(2951, "_uuid", 2950),
    array(3807, "_jsonb", 3802),
];

/// The type with `oid`, if `pg_type` has it.
pub(crate) fn by_oid(oid: i64) -> Option<&'static PgType> {
    TYPES.iter().find(|t| t.oid == oid)
}

/// The oid of the type named `name`, by its catalog name or as SQL
/// writes it.
pub(crate) fn by_name(name: &str) -> Option<i64> {
    let name = name.trim().to_ascii_lowercase();
    let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
    let catalog = match name {
        "bigint" => "int8",
        "integer" | "int" => "int4",
        "smallint" => "int2",
        "boolean" => "bool",
        "double precision" => "float8",
        "real" => "float4",
        "character varying" => "varchar",
        "character" => "bpchar",
        "timestamp with time zone" => "timestamptz",
        "timestamp without time zone" => "timestamp",
        other => other,
    };
    TYPES.iter().find(|t| t.name == catalog).map(|t| t.oid)
}

/// The type with `oid` as SQL writes it, which is what `format_type`
/// answers.
pub(crate) fn display(oid: i64) -> Option<String> {
    let found = by_oid(oid)?;
    if found.elem != 0 && found.category == 'A' && found.name.starts_with('_') {
        return Some(format!("{}[]", display(found.elem)?));
    }
    Some(
        match found.name {
            "int8" => "bigint",
            "int4" => "integer",
            "int2" => "smallint",
            "bool" => "boolean",
            "float8" => "double precision",
            "float4" => "real",
            "varchar" => "character varying",
            "bpchar" => "character",
            "timestamptz" => "timestamp with time zone",
            "timestamp" => "timestamp without time zone",
            "char" => "\"char\"",
            other => other,
        }
        .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_type_is_found_by_oid_and_by_either_name() {
        assert_eq!(by_oid(20).map(|t| t.name), Some("int8"));
        assert_eq!(by_name("bigint"), Some(20));
        assert_eq!(by_name("pg_catalog.int8"), Some(20));
        assert_eq!(by_name("Timestamp With Time Zone"), Some(1184));
        assert_eq!(by_name("no such type"), None);
        assert_eq!(display(1184).as_deref(), Some("timestamp with time zone"));
        assert_eq!(display(1009).as_deref(), Some("text[]"));
        assert_eq!(display(9), None);
    }

    #[test]
    fn every_array_names_an_element_that_names_it_back() {
        for found in TYPES.iter().filter(|t| t.name.starts_with('_')) {
            let elem = by_oid(found.elem).unwrap_or_else(|| panic!("{}", found.name));
            assert_eq!(elem.array, found.oid, "{}", found.name);
        }
        let mut oids: Vec<i64> = TYPES.iter().map(|t| t.oid).collect();
        oids.sort_unstable();
        oids.dedup();
        assert_eq!(oids.len(), TYPES.len());
    }
}
