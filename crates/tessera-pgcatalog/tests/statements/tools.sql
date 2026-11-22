-- What Grafana's PostgreSQL data source asks to fill its query builder, and what DBeaver asks to draw its
-- navigator. Written from what the two are known to send, not recorded from them: neither was run.
-- Statements are separated by a line of three semicolons.
SELECT current_setting('server_version_num')::int/100 as version
;;;
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'
;;;
SELECT quote_ident(table_name) as "table" FROM information_schema.tables WHERE quote_ident(table_schema) NOT IN ('information_schema', 'pg_catalog', '_timescaledb_cache', '_timescaledb_catalog', '_timescaledb_internal', '_timescaledb_config', 'timescaledb_information', 'timescaledb_experimental') AND table_type = 'BASE TABLE' AND quote_ident(table_schema) IN (SELECT CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END FROM generate_series(array_lower(string_to_array(current_setting('search_path'),','),1), array_upper(string_to_array(current_setting('search_path'),','),1)) as i, string_to_array(current_setting('search_path'),',') s)
;;;
SELECT quote_ident(column_name) AS "column", data_type AS "type" FROM information_schema.columns WHERE CASE WHEN array_length(parse_ident('logs'),1) = 2 THEN quote_ident(table_schema) = (parse_ident('logs'))[1] AND quote_ident(table_name) = (parse_ident('logs'))[2] ELSE quote_ident(table_name) = 'logs' AND quote_ident(table_schema) IN (SELECT CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END FROM generate_series(array_lower(string_to_array(current_setting('search_path'),','),1), array_upper(string_to_array(current_setting('search_path'),','),1)) as i, string_to_array(current_setting('search_path'),',') s) END LIMIT 4
;;;
SELECT current_schema(),session_user
;;;
SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE 1 = 1 AND datallowconn AND NOT datistemplate ORDER BY db.datname
;;;
select * from pg_catalog.pg_settings where name='standard_conforming_strings'
;;;
SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname
;;;
SELECT count(*) FROM (SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description FROM pg_catalog.pg_type t LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem LEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid LEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid WHERE t.typname IS NOT NULL AND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')) x
;;;
SELECT c.oid,c.relname,c.relkind,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key FROM pg_catalog.pg_class c LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass WHERE c.relnamespace=2200 AND c.relkind not in ('i','I','c')
;;;
SELECT c.relname,a.attname,a.atttypid,a.attnotnull,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description,dep.objid FROM pg_catalog.pg_attribute a INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid) LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum) LEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) LEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum and dep.classid = dep.refclassid WHERE NOT a.attisdropped AND c.relkind not in ('i','I','c') AND c.oid=40001 ORDER BY a.attnum LIMIT 3
;;;
SELECT a.oid,a.*,pd.description FROM pg_catalog.pg_roles a left join pg_catalog.pg_shdescription pd on a.oid = pd.objoid ORDER BY a.rolname
;;;
SELECT i.*,i.indkey as keys,c.relname,c.relnamespace,c.relam,c.reltablespace,tc.relname as tabrelname,dsc.description,pg_catalog.pg_get_expr(i.indpred, i.indrelid) as pred_expr,pg_catalog.pg_get_expr(i.indexprs, i.indrelid, true) as expr,pg_catalog.pg_relation_size(i.indexrelid) as index_rel_size,pg_catalog.pg_stat_get_numscans(i.indexrelid) as index_num_scans FROM pg_catalog.pg_index i INNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid INNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid WHERE  i.indrelid=40001 ORDER BY tabrelname, c.relname
;;;
SELECT c.oid,c.*,t.relname as tabrelname,rt.relnamespace as refnamespace,d.description, case when c.contype='c' then "substring"(pg_get_constraintdef(c.oid), 7) else null end consrc_copy FROM pg_catalog.pg_constraint c INNER JOIN pg_catalog.pg_class t ON t.oid=c.conrelid LEFT OUTER JOIN pg_catalog.pg_class rt ON rt.oid=c.confrelid LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_constraint'::regclass WHERE c.conrelid=40001 ORDER BY c.oid
;;;
SELECT am.oid,am.* FROM pg_catalog.pg_am am ORDER BY am.oid
;;;
SELECT t.oid,t.* FROM pg_catalog.pg_tablespace t ORDER BY t.oid
;;;
SELECT l.oid,l.* FROM pg_catalog.pg_language l ORDER BY l.oid
;;;
SELECT count(*) FROM (SELECT enc.encid as encid, pg_catalog.pg_encoding_to_char(enc.encid) encname FROM (SELECT generate_series(0, 100) AS encid) as enc) x
;;;
SELECT e.oid,cfg.tbls,n.nspname as schema_name,e.* FROM pg_catalog.pg_extension e join pg_namespace n on n.oid =e.extnamespace left join (select ARRAY_AGG(ARRAY[n.nspname, c.relname]) tbls, oid_ext from (select unnest(e1.extconfig) oid , e1.oid oid_ext from pg_catalog.pg_extension e1 ) c0 join pg_class c on c.oid =c0.oid join pg_namespace n on n.oid =c.relnamespace group by oid_ext) cfg on cfg.oid_ext=e.oid ORDER BY e.oid
;;;
SELECT x.oid,x.* FROM pg_catalog.pg_event_trigger x
;;;
SELECT sp.oid,sp.* FROM pg_catalog.pg_proc sp WHERE sp.pronamespace=2200
