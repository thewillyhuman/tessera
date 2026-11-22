-- SQLAlchemy 2 over psycopg2 (its reflection of schemas, tables, columns, keys and indexes), psycopg 3 and asyncpg.
-- Recorded from the client against a node; statements are separated by a line of three semicolons.
SELECT t.oid, typarray
FROM pg_type t JOIN pg_namespace ns
    ON typnamespace = ns.oid
WHERE typname = 'hstore'
;;;
select pg_catalog.version()
;;;
select current_schema()
;;;
show transaction isolation level
;;;
show standard_conforming_strings
;;;
select version()
;;;
SELECT pg_catalog.pg_namespace.nspname 
FROM pg_catalog.pg_namespace 
WHERE (pg_catalog.pg_namespace.nspname NOT LIKE 'pg/_' || '%' ESCAPE '/') ORDER BY pg_catalog.pg_namespace.nspname
;;;
SELECT pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p']) AND pg_catalog.pg_class.relpersistence != 't' AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
;;;
SELECT pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p']) AND pg_catalog.pg_class.relpersistence != 't' AND pg_catalog.pg_namespace.nspname = 'system'
;;;
SELECT pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['v']) AND pg_catalog.pg_class.relpersistence != 't' AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
;;;
SELECT pg_catalog.pg_attribute.attname AS name, pg_catalog.format_type(pg_catalog.pg_attribute.atttypid, pg_catalog.pg_attribute.atttypmod) AS format_type, CASE WHEN pg_catalog.pg_attribute.atthasdef THEN (SELECT pg_catalog.pg_get_expr(pg_catalog.pg_attrdef.adbin, pg_catalog.pg_attrdef.adrelid) AS pg_get_expr_1 
FROM pg_catalog.pg_attrdef 
WHERE pg_catalog.pg_attribute.atthasdef AND pg_catalog.pg_attrdef.adrelid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_attrdef.adnum = pg_catalog.pg_attribute.attnum) ELSE NULL END AS "default", pg_catalog.pg_attribute.attnotnull AS not_null, pg_catalog.pg_class.relname AS table_name, pg_catalog.pg_description.description AS comment, pg_catalog.pg_attribute.attgenerated AS generated, CASE WHEN (pg_catalog.pg_attribute.attidentity != '') THEN (SELECT json_build_object('always', pg_catalog.pg_attribute.attidentity = 'a', 'start', pg_catalog.pg_sequence.seqstart, 'increment', pg_catalog.pg_sequence.seqincrement, 'minvalue', pg_catalog.pg_sequence.seqmin, 'maxvalue', pg_catalog.pg_sequence.seqmax, 'cache', pg_catalog.pg_sequence.seqcache, 'cycle', pg_catalog.pg_sequence.seqcycle) AS json_build_object_1 
FROM pg_catalog.pg_sequence 
WHERE pg_catalog.pg_attribute.attidentity != '' AND pg_catalog.pg_sequence.seqrelid = CAST(CAST(pg_catalog.pg_get_serial_sequence(CAST(CAST(pg_catalog.pg_attribute.attrelid AS REGCLASS) AS TEXT), pg_catalog.pg_attribute.attname) AS REGCLASS) AS OID)) ELSE NULL END AS identity_options, CASE WHEN (pg_catalog.pg_attribute.attcollation != 0 AND (SELECT pg_catalog.pg_type.typcollation 
FROM pg_catalog.pg_type 
WHERE pg_catalog.pg_type.oid = pg_catalog.pg_attribute.atttypid) != pg_catalog.pg_attribute.attcollation) THEN (SELECT json_build_object('name', pg_catalog.pg_collation.collname, 'schema', CASE WHEN pg_catalog.pg_collation_is_visible(pg_catalog.pg_collation.oid) THEN NULL ELSE pg_catalog.pg_namespace.nspname END) AS json_build_object_9 
FROM pg_catalog.pg_collation JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_collation.collnamespace 
WHERE pg_catalog.pg_collation.oid = pg_catalog.pg_attribute.attcollation) ELSE NULL END AS collation 
FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_attribute ON pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_attribute.attnum > 0 AND NOT pg_catalog.pg_attribute.attisdropped LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_description.objsubid = pg_catalog.pg_attribute.attnum JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('logs') ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_attribute.attnum
;;;
SELECT pg_catalog.pg_class.oid, pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('logs')
;;;
SELECT attr.conrelid, array_agg(CAST(attr.attname AS TEXT) ORDER BY attr.ord) AS cols, attr.conname, min(attr.description) AS description, min(attr.indnkeyatts) AS indnkeyatts, bool_and(attr.indnullsnotdistinct) AS indnullsnotdistinct 
FROM (SELECT con.conrelid AS conrelid, con.conname AS conname, con.description AS description, con.ord AS ord, con.indnkeyatts AS indnkeyatts, con.indnullsnotdistinct AS indnullsnotdistinct, pg_catalog.pg_attribute.attname AS attname 
FROM pg_catalog.pg_attribute JOIN (SELECT pg_catalog.pg_constraint.conrelid AS conrelid, pg_catalog.pg_constraint.conname AS conname, unnest(pg_catalog.pg_index.indkey) AS attnum, generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord, pg_catalog.pg_index.indnkeyatts AS indnkeyatts, pg_catalog.pg_index.indnullsnotdistinct AS indnullsnotdistinct, pg_catalog.pg_description.description AS description 
FROM pg_catalog.pg_constraint JOIN pg_catalog.pg_index ON pg_catalog.pg_constraint.conindid = pg_catalog.pg_index.indexrelid LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_constraint' AS REGCLASS) 
WHERE pg_catalog.pg_constraint.contype = 'p' AND pg_catalog.pg_constraint.conrelid IN (40000)) AS con ON pg_catalog.pg_attribute.attnum = con.attnum AND pg_catalog.pg_attribute.attrelid = con.conrelid 
WHERE con.conrelid IN (40000)) AS attr GROUP BY attr.conrelid, attr.conname ORDER BY attr.conrelid, attr.conname
;;;
SELECT pg_catalog.pg_class.relname, pg_catalog.pg_constraint.conname, CASE WHEN (pg_catalog.pg_constraint.oid IS NOT NULL) THEN pg_catalog.pg_get_constraintdef(pg_catalog.pg_constraint.oid, true) END AS anon_1, nsp_ref.nspname, pg_catalog.pg_description.description 
FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_constraint ON pg_catalog.pg_class.oid = pg_catalog.pg_constraint.conrelid AND pg_catalog.pg_constraint.contype = 'f' LEFT OUTER JOIN pg_catalog.pg_class AS cls_ref ON cls_ref.oid = pg_catalog.pg_constraint.confrelid LEFT OUTER JOIN pg_catalog.pg_namespace AS nsp_ref ON cls_ref.relnamespace = nsp_ref.oid LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_constraint' AS REGCLASS) JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('logs') ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_constraint.conname
;;;
SELECT pg_catalog.pg_am.oid, pg_catalog.pg_am.amname 
FROM pg_catalog.pg_am 
WHERE pg_catalog.pg_am.amname = 'btree'
;;;
SELECT pg_catalog.pg_opclass.oid, pg_catalog.pg_opclass.opcname 
FROM pg_catalog.pg_opclass 
WHERE NOT pg_catalog.pg_opclass.opcdefault
;;;
SELECT pg_catalog.pg_index.indrelid, pg_catalog.pg_class.relname, pg_catalog.pg_index.indisunique, pg_catalog.pg_index.indisvalid, pg_catalog.pg_constraint.conrelid IS NOT NULL AS has_constraint, pg_catalog.pg_index.indoption, pg_catalog.pg_class.reloptions, pg_catalog.pg_class.relam, CASE WHEN (pg_catalog.pg_index.indpred IS NOT NULL) THEN pg_catalog.pg_get_expr(pg_catalog.pg_index.indpred, pg_catalog.pg_index.indrelid) END AS filter_definition, pg_catalog.pg_index.indnkeyatts, pg_catalog.pg_index.indnullsnotdistinct, idx_cols.elements, idx_cols.elements_is_expr, idx_cols.elements_opclass 
FROM pg_catalog.pg_index JOIN pg_catalog.pg_class ON pg_catalog.pg_index.indexrelid = pg_catalog.pg_class.oid LEFT OUTER JOIN (SELECT idx_attr.indexrelid AS indexrelid, min(idx_attr.indrelid) AS min_1, array_agg(idx_attr.element ORDER BY idx_attr.ord) AS elements, array_agg(idx_attr.is_expr ORDER BY idx_attr.ord) AS elements_is_expr, array_agg(idx_attr.att_opclass ORDER BY idx_attr.ord) AS elements_opclass 
FROM (SELECT idx.indexrelid AS indexrelid, idx.indrelid AS indrelid, idx.ord AS ord, CASE WHEN (idx.attnum = 0) THEN pg_catalog.pg_get_indexdef(idx.indexrelid, idx.ord + 1, true) ELSE CAST(pg_catalog.pg_attribute.attname AS TEXT) END AS element, idx.attnum = 0 AS is_expr, CAST(idx.att_opclass AS BIGINT) AS att_opclass 
FROM (SELECT pg_catalog.pg_index.indexrelid AS indexrelid, pg_catalog.pg_index.indrelid AS indrelid, unnest(pg_catalog.pg_index.indkey) AS attnum, unnest(pg_catalog.pg_index.indclass) AS att_opclass, generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord 
FROM pg_catalog.pg_index 
WHERE NOT pg_catalog.pg_index.indisprimary AND pg_catalog.pg_index.indrelid IN (40000)) AS idx LEFT OUTER JOIN pg_catalog.pg_attribute ON pg_catalog.pg_attribute.attnum = idx.attnum AND pg_catalog.pg_attribute.attrelid = idx.indrelid 
WHERE idx.indrelid IN (40000)) AS idx_attr GROUP BY idx_attr.indexrelid) AS idx_cols ON pg_catalog.pg_index.indexrelid = idx_cols.indexrelid LEFT OUTER JOIN pg_catalog.pg_constraint ON pg_catalog.pg_index.indrelid = pg_catalog.pg_constraint.conrelid AND pg_catalog.pg_index.indexrelid = pg_catalog.pg_constraint.conindid AND pg_catalog.pg_constraint.contype = ANY (ARRAY['p', 'u', 'x']) 
WHERE pg_catalog.pg_index.indrelid IN (40000) AND NOT pg_catalog.pg_index.indisprimary ORDER BY pg_catalog.pg_index.indrelid, pg_catalog.pg_class.relname
;;;
SELECT attr.conrelid, array_agg(CAST(attr.attname AS TEXT) ORDER BY attr.ord) AS cols, attr.conname, min(attr.description) AS description, min(attr.indnkeyatts) AS indnkeyatts, bool_and(attr.indnullsnotdistinct) AS indnullsnotdistinct 
FROM (SELECT con.conrelid AS conrelid, con.conname AS conname, con.description AS description, con.ord AS ord, con.indnkeyatts AS indnkeyatts, con.indnullsnotdistinct AS indnullsnotdistinct, pg_catalog.pg_attribute.attname AS attname 
FROM pg_catalog.pg_attribute JOIN (SELECT pg_catalog.pg_constraint.conrelid AS conrelid, pg_catalog.pg_constraint.conname AS conname, unnest(pg_catalog.pg_index.indkey) AS attnum, generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord, pg_catalog.pg_index.indnkeyatts AS indnkeyatts, pg_catalog.pg_index.indnullsnotdistinct AS indnullsnotdistinct, pg_catalog.pg_description.description AS description 
FROM pg_catalog.pg_constraint JOIN pg_catalog.pg_index ON pg_catalog.pg_constraint.conindid = pg_catalog.pg_index.indexrelid LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_constraint' AS REGCLASS) 
WHERE pg_catalog.pg_constraint.contype = 'u' AND pg_catalog.pg_constraint.conrelid IN (40000)) AS con ON pg_catalog.pg_attribute.attnum = con.attnum AND pg_catalog.pg_attribute.attrelid = con.conrelid 
WHERE con.conrelid IN (40000)) AS attr GROUP BY attr.conrelid, attr.conname ORDER BY attr.conrelid, attr.conname
;;;
SELECT pg_catalog.pg_class.relname, pg_catalog.pg_description.description 
FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_class.oid = pg_catalog.pg_description.objoid AND pg_catalog.pg_description.objsubid = 0 AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_class' AS REGCLASS) JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('logs')
;;;
SELECT pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relname IN ('logs') AND pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
;;;
SELECT pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relname IN ('nope') AND pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog'
;;;
SELECT pg_catalog.pg_attribute.attname AS name, pg_catalog.format_type(pg_catalog.pg_attribute.atttypid, pg_catalog.pg_attribute.atttypmod) AS format_type, CASE WHEN pg_catalog.pg_attribute.atthasdef THEN (SELECT pg_catalog.pg_get_expr(pg_catalog.pg_attrdef.adbin, pg_catalog.pg_attrdef.adrelid) AS pg_get_expr_1 
FROM pg_catalog.pg_attrdef 
WHERE pg_catalog.pg_attribute.atthasdef AND pg_catalog.pg_attrdef.adrelid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_attrdef.adnum = pg_catalog.pg_attribute.attnum) ELSE NULL END AS "default", pg_catalog.pg_attribute.attnotnull AS not_null, pg_catalog.pg_class.relname AS table_name, pg_catalog.pg_description.description AS comment, pg_catalog.pg_attribute.attgenerated AS generated, CASE WHEN (pg_catalog.pg_attribute.attidentity != '') THEN (SELECT json_build_object('always', pg_catalog.pg_attribute.attidentity = 'a', 'start', pg_catalog.pg_sequence.seqstart, 'increment', pg_catalog.pg_sequence.seqincrement, 'minvalue', pg_catalog.pg_sequence.seqmin, 'maxvalue', pg_catalog.pg_sequence.seqmax, 'cache', pg_catalog.pg_sequence.seqcache, 'cycle', pg_catalog.pg_sequence.seqcycle) AS json_build_object_1 
FROM pg_catalog.pg_sequence 
WHERE pg_catalog.pg_attribute.attidentity != '' AND pg_catalog.pg_sequence.seqrelid = CAST(CAST(pg_catalog.pg_get_serial_sequence(CAST(CAST(pg_catalog.pg_attribute.attrelid AS REGCLASS) AS TEXT), pg_catalog.pg_attribute.attname) AS REGCLASS) AS OID)) ELSE NULL END AS identity_options, CASE WHEN (pg_catalog.pg_attribute.attcollation != 0 AND (SELECT pg_catalog.pg_type.typcollation 
FROM pg_catalog.pg_type 
WHERE pg_catalog.pg_type.oid = pg_catalog.pg_attribute.atttypid) != pg_catalog.pg_attribute.attcollation) THEN (SELECT json_build_object('name', pg_catalog.pg_collation.collname, 'schema', CASE WHEN pg_catalog.pg_collation_is_visible(pg_catalog.pg_collation.oid) THEN NULL ELSE pg_catalog.pg_namespace.nspname END) AS json_build_object_9 
FROM pg_catalog.pg_collation JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_collation.collnamespace 
WHERE pg_catalog.pg_collation.oid = pg_catalog.pg_attribute.attcollation) ELSE NULL END AS collation 
FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_attribute ON pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_attribute.attnum > 0 AND NOT pg_catalog.pg_attribute.attisdropped LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_attribute.attrelid AND pg_catalog.pg_description.objsubid = pg_catalog.pg_attribute.attnum JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('metrics') ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_attribute.attnum
;;;
SELECT pg_catalog.pg_class.oid, pg_catalog.pg_class.relname 
FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('metrics')
;;;
SELECT attr.conrelid, array_agg(CAST(attr.attname AS TEXT) ORDER BY attr.ord) AS cols, attr.conname, min(attr.description) AS description, min(attr.indnkeyatts) AS indnkeyatts, bool_and(attr.indnullsnotdistinct) AS indnullsnotdistinct 
FROM (SELECT con.conrelid AS conrelid, con.conname AS conname, con.description AS description, con.ord AS ord, con.indnkeyatts AS indnkeyatts, con.indnullsnotdistinct AS indnullsnotdistinct, pg_catalog.pg_attribute.attname AS attname 
FROM pg_catalog.pg_attribute JOIN (SELECT pg_catalog.pg_constraint.conrelid AS conrelid, pg_catalog.pg_constraint.conname AS conname, unnest(pg_catalog.pg_index.indkey) AS attnum, generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord, pg_catalog.pg_index.indnkeyatts AS indnkeyatts, pg_catalog.pg_index.indnullsnotdistinct AS indnullsnotdistinct, pg_catalog.pg_description.description AS description 
FROM pg_catalog.pg_constraint JOIN pg_catalog.pg_index ON pg_catalog.pg_constraint.conindid = pg_catalog.pg_index.indexrelid LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_constraint' AS REGCLASS) 
WHERE pg_catalog.pg_constraint.contype = 'p' AND pg_catalog.pg_constraint.conrelid IN (40002)) AS con ON pg_catalog.pg_attribute.attnum = con.attnum AND pg_catalog.pg_attribute.attrelid = con.conrelid 
WHERE con.conrelid IN (40002)) AS attr GROUP BY attr.conrelid, attr.conname ORDER BY attr.conrelid, attr.conname
;;;
SELECT pg_catalog.pg_class.relname, pg_catalog.pg_constraint.conname, CASE WHEN (pg_catalog.pg_constraint.oid IS NOT NULL) THEN pg_catalog.pg_get_constraintdef(pg_catalog.pg_constraint.oid, true) END AS anon_1, nsp_ref.nspname, pg_catalog.pg_description.description 
FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_constraint ON pg_catalog.pg_class.oid = pg_catalog.pg_constraint.conrelid AND pg_catalog.pg_constraint.contype = 'f' LEFT OUTER JOIN pg_catalog.pg_class AS cls_ref ON cls_ref.oid = pg_catalog.pg_constraint.confrelid LEFT OUTER JOIN pg_catalog.pg_namespace AS nsp_ref ON cls_ref.relnamespace = nsp_ref.oid LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_constraint' AS REGCLASS) JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('metrics') ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_constraint.conname
;;;
SELECT pg_catalog.pg_index.indrelid, pg_catalog.pg_class.relname, pg_catalog.pg_index.indisunique, pg_catalog.pg_index.indisvalid, pg_catalog.pg_constraint.conrelid IS NOT NULL AS has_constraint, pg_catalog.pg_index.indoption, pg_catalog.pg_class.reloptions, pg_catalog.pg_class.relam, CASE WHEN (pg_catalog.pg_index.indpred IS NOT NULL) THEN pg_catalog.pg_get_expr(pg_catalog.pg_index.indpred, pg_catalog.pg_index.indrelid) END AS filter_definition, pg_catalog.pg_index.indnkeyatts, pg_catalog.pg_index.indnullsnotdistinct, idx_cols.elements, idx_cols.elements_is_expr, idx_cols.elements_opclass 
FROM pg_catalog.pg_index JOIN pg_catalog.pg_class ON pg_catalog.pg_index.indexrelid = pg_catalog.pg_class.oid LEFT OUTER JOIN (SELECT idx_attr.indexrelid AS indexrelid, min(idx_attr.indrelid) AS min_1, array_agg(idx_attr.element ORDER BY idx_attr.ord) AS elements, array_agg(idx_attr.is_expr ORDER BY idx_attr.ord) AS elements_is_expr, array_agg(idx_attr.att_opclass ORDER BY idx_attr.ord) AS elements_opclass 
FROM (SELECT idx.indexrelid AS indexrelid, idx.indrelid AS indrelid, idx.ord AS ord, CASE WHEN (idx.attnum = 0) THEN pg_catalog.pg_get_indexdef(idx.indexrelid, idx.ord + 1, true) ELSE CAST(pg_catalog.pg_attribute.attname AS TEXT) END AS element, idx.attnum = 0 AS is_expr, CAST(idx.att_opclass AS BIGINT) AS att_opclass 
FROM (SELECT pg_catalog.pg_index.indexrelid AS indexrelid, pg_catalog.pg_index.indrelid AS indrelid, unnest(pg_catalog.pg_index.indkey) AS attnum, unnest(pg_catalog.pg_index.indclass) AS att_opclass, generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord 
FROM pg_catalog.pg_index 
WHERE NOT pg_catalog.pg_index.indisprimary AND pg_catalog.pg_index.indrelid IN (40002)) AS idx LEFT OUTER JOIN pg_catalog.pg_attribute ON pg_catalog.pg_attribute.attnum = idx.attnum AND pg_catalog.pg_attribute.attrelid = idx.indrelid 
WHERE idx.indrelid IN (40002)) AS idx_attr GROUP BY idx_attr.indexrelid) AS idx_cols ON pg_catalog.pg_index.indexrelid = idx_cols.indexrelid LEFT OUTER JOIN pg_catalog.pg_constraint ON pg_catalog.pg_index.indrelid = pg_catalog.pg_constraint.conrelid AND pg_catalog.pg_index.indexrelid = pg_catalog.pg_constraint.conindid AND pg_catalog.pg_constraint.contype = ANY (ARRAY['p', 'u', 'x']) 
WHERE pg_catalog.pg_index.indrelid IN (40002) AND NOT pg_catalog.pg_index.indisprimary ORDER BY pg_catalog.pg_index.indrelid, pg_catalog.pg_class.relname
;;;
SELECT attr.conrelid, array_agg(CAST(attr.attname AS TEXT) ORDER BY attr.ord) AS cols, attr.conname, min(attr.description) AS description, min(attr.indnkeyatts) AS indnkeyatts, bool_and(attr.indnullsnotdistinct) AS indnullsnotdistinct 
FROM (SELECT con.conrelid AS conrelid, con.conname AS conname, con.description AS description, con.ord AS ord, con.indnkeyatts AS indnkeyatts, con.indnullsnotdistinct AS indnullsnotdistinct, pg_catalog.pg_attribute.attname AS attname 
FROM pg_catalog.pg_attribute JOIN (SELECT pg_catalog.pg_constraint.conrelid AS conrelid, pg_catalog.pg_constraint.conname AS conname, unnest(pg_catalog.pg_index.indkey) AS attnum, generate_subscripts(pg_catalog.pg_index.indkey, 1) AS ord, pg_catalog.pg_index.indnkeyatts AS indnkeyatts, pg_catalog.pg_index.indnullsnotdistinct AS indnullsnotdistinct, pg_catalog.pg_description.description AS description 
FROM pg_catalog.pg_constraint JOIN pg_catalog.pg_index ON pg_catalog.pg_constraint.conindid = pg_catalog.pg_index.indexrelid LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_constraint' AS REGCLASS) 
WHERE pg_catalog.pg_constraint.contype = 'u' AND pg_catalog.pg_constraint.conrelid IN (40002)) AS con ON pg_catalog.pg_attribute.attnum = con.attnum AND pg_catalog.pg_attribute.attrelid = con.conrelid 
WHERE con.conrelid IN (40002)) AS attr GROUP BY attr.conrelid, attr.conname ORDER BY attr.conrelid, attr.conname
;;;
SELECT pg_catalog.pg_class.relname, pg_catalog.pg_description.description 
FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_class.oid = pg_catalog.pg_description.objoid AND pg_catalog.pg_description.objsubid = 0 AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_class' AS REGCLASS) JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('metrics')
;;;
SELECT pg_catalog.pg_class.relname, pg_catalog.pg_constraint.conname, CASE WHEN (pg_catalog.pg_constraint.oid IS NOT NULL) THEN pg_catalog.pg_get_constraintdef(pg_catalog.pg_constraint.oid, true) END AS anon_1, pg_catalog.pg_description.description 
FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_constraint ON pg_catalog.pg_class.oid = pg_catalog.pg_constraint.conrelid AND pg_catalog.pg_constraint.contype = 'c' LEFT OUTER JOIN pg_catalog.pg_description ON pg_catalog.pg_description.objoid = pg_catalog.pg_constraint.oid AND pg_catalog.pg_description.classoid = CAST('pg_catalog.pg_constraint' AS REGCLASS) JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('metrics') ORDER BY pg_catalog.pg_class.relname, pg_catalog.pg_constraint.conname
;;;
SELECT pg_catalog.pg_class.oid, pg_catalog.pg_class.relname, pg_catalog.pg_class.reloptions, NULL AS relhasoids, CASE WHEN (pg_catalog.pg_am.amname IS NOT NULL AND pg_catalog.pg_am.amname != current_setting('default_table_access_method')) THEN pg_catalog.pg_am.amname ELSE NULL END AS access_method_name, pg_catalog.pg_tablespace.spcname AS tablespace_name, inherits.parent_table_names 
FROM pg_catalog.pg_class LEFT OUTER JOIN pg_catalog.pg_am ON pg_catalog.pg_class.relam = pg_catalog.pg_am.oid LEFT OUTER JOIN (SELECT pg_catalog.pg_inherits.inhrelid AS inhrelid, array_agg(pg_catalog.pg_class.relname ORDER BY pg_catalog.pg_inherits.inhseqno) AS parent_table_names 
FROM pg_catalog.pg_inherits JOIN pg_catalog.pg_class ON pg_catalog.pg_inherits.inhparent = pg_catalog.pg_class.oid GROUP BY pg_catalog.pg_inherits.inhrelid) AS inherits ON pg_catalog.pg_class.oid = inherits.inhrelid LEFT OUTER JOIN pg_catalog.pg_tablespace ON pg_catalog.pg_tablespace.oid = pg_catalog.pg_class.reltablespace JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
WHERE pg_catalog.pg_class.relkind = ANY (ARRAY['r', 'p', 'f', 'v', 'm']) AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) AND pg_catalog.pg_namespace.nspname != 'pg_catalog' AND pg_catalog.pg_class.relname IN ('metrics')
;;;
select nspname from pg_namespace where oid = $1
;;;
select table_name from information_schema.tables where table_schema = $1
