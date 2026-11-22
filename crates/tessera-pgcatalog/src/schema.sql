-- SPDX-License-Identifier: Apache-2.0
--
-- The system catalogs as PostgreSQL 16 lays them out, for the store
-- that answers catalog queries. A column's declared type is the type
-- a client is told the column has; arrays and the types with no
-- counterpart here are text.

CREATE TABLE pg_catalog.pg_namespace (
    oid OID, nspname NAME, nspowner OID, nspacl TEXT);

CREATE TABLE pg_catalog.pg_class (
    oid OID, relname NAME, relnamespace OID, reltype OID, reloftype OID,
    relowner OID, relam OID, relfilenode OID, reltablespace OID,
    relpages INT4, reltuples FLOAT4, relallvisible INT4, reltoastrelid OID,
    relhasindex BOOL, relisshared BOOL, relpersistence CHAR, relkind CHAR,
    relnatts INT2, relchecks INT2, relhasrules BOOL, relhastriggers BOOL,
    relhassubclass BOOL, relrowsecurity BOOL, relforcerowsecurity BOOL,
    relispopulated BOOL, relreplident CHAR, relispartition BOOL,
    relrewrite OID, relfrozenxid TEXT, relminmxid TEXT, relacl TEXT,
    reloptions TEXT, relpartbound TEXT);

CREATE TABLE pg_catalog.pg_attribute (
    attrelid OID, attname NAME, atttypid OID, attstattarget INT4, attlen INT2,
    attnum INT2, attndims INT4, attcacheoff INT4, atttypmod INT4,
    attbyval BOOL, attalign CHAR, attstorage CHAR, attcompression CHAR,
    attnotnull BOOL, atthasdef BOOL, atthasmissing BOOL, attidentity CHAR,
    attgenerated CHAR, attisdropped BOOL, attislocal BOOL, attinhcount INT4,
    attcollation OID, attacl TEXT, attoptions TEXT, attfdwoptions TEXT,
    attmissingval TEXT);

CREATE TABLE pg_catalog.pg_type (
    oid OID, typname NAME, typnamespace OID, typowner OID, typlen INT2,
    typbyval BOOL, typtype CHAR, typcategory CHAR, typispreferred BOOL,
    typisdefined BOOL, typdelim CHAR, typrelid OID, typsubscript TEXT,
    typelem OID, typarray OID, typinput TEXT, typoutput TEXT,
    typreceive TEXT, typsend TEXT, typmodin TEXT, typmodout TEXT,
    typanalyze TEXT, typalign CHAR, typstorage CHAR, typnotnull BOOL,
    typbasetype OID, typtypmod INT4, typndims INT4, typcollation OID,
    typdefaultbin TEXT, typdefault TEXT, typacl TEXT);

CREATE TABLE pg_catalog.pg_database (
    oid OID, datname NAME, datdba OID, encoding INT4, datlocprovider CHAR,
    datistemplate BOOL, datallowconn BOOL, datconnlimit INT4,
    datfrozenxid TEXT, datminmxid TEXT, dattablespace OID, datcollate TEXT,
    datctype TEXT, daticulocale TEXT, daticurules TEXT, datcollversion TEXT,
    datacl TEXT);

CREATE TABLE pg_catalog.pg_roles (
    rolname NAME, rolsuper BOOL, rolinherit BOOL, rolcreaterole BOOL,
    rolcreatedb BOOL, rolcanlogin BOOL, rolreplication BOOL,
    rolconnlimit INT4, rolpassword TEXT, rolvaliduntil TEXT,
    rolbypassrls BOOL, rolconfig TEXT, oid OID);
CREATE TABLE pg_catalog.pg_authid (
    oid OID, rolname NAME, rolsuper BOOL, rolinherit BOOL, rolcreaterole BOOL,
    rolcreatedb BOOL, rolcanlogin BOOL, rolreplication BOOL,
    rolbypassrls BOOL, rolconnlimit INT4, rolpassword TEXT,
    rolvaliduntil TEXT);
CREATE TABLE pg_catalog.pg_user (
    usename NAME, usesysid OID, usecreatedb BOOL, usesuper BOOL,
    userepl BOOL, usebypassrls BOOL, passwd TEXT, valuntil TEXT,
    useconfig TEXT);
CREATE TABLE pg_catalog.pg_shadow (
    usename NAME, usesysid OID, usecreatedb BOOL, usesuper BOOL,
    userepl BOOL, usebypassrls BOOL, passwd TEXT, valuntil TEXT,
    useconfig TEXT);
CREATE TABLE pg_catalog.pg_group (groname NAME, grosysid OID, grolist TEXT);
CREATE TABLE pg_catalog.pg_auth_members (
    oid OID, roleid OID, member OID, grantor OID, admin_option BOOL,
    inherit_option BOOL, set_option BOOL);

CREATE TABLE pg_catalog.pg_settings (
    name TEXT, setting TEXT, unit TEXT, category TEXT, short_desc TEXT,
    extra_desc TEXT, context TEXT, vartype TEXT, source TEXT, min_val TEXT,
    max_val TEXT, enumvals TEXT, boot_val TEXT, reset_val TEXT,
    sourcefile TEXT, sourceline INT4, pending_restart BOOL);

CREATE TABLE pg_catalog.pg_am (
    oid OID, amname NAME, amhandler TEXT, amtype CHAR);
CREATE TABLE pg_catalog.pg_tablespace (
    oid OID, spcname NAME, spcowner OID, spcacl TEXT, spcoptions TEXT);
CREATE TABLE pg_catalog.pg_description (
    objoid OID, classoid OID, objsubid INT4, description TEXT);
CREATE TABLE pg_catalog.pg_shdescription (
    objoid OID, classoid OID, description TEXT);
CREATE TABLE pg_catalog.pg_attrdef (
    oid OID, adrelid OID, adnum INT2, adbin TEXT);

CREATE TABLE pg_catalog.pg_constraint (
    oid OID, conname NAME, connamespace OID, contype CHAR,
    condeferrable BOOL, condeferred BOOL, convalidated BOOL, conrelid OID,
    contypid OID, conindid OID, conparentid OID, confrelid OID,
    confupdtype CHAR, confdeltype CHAR, confmatchtype CHAR,
    conislocal BOOL, coninhcount INT4, connoinherit BOOL, conkey TEXT,
    confkey TEXT, conpfeqop TEXT, conppeqop TEXT, conffeqop TEXT,
    confdelsetcols TEXT, conexclop TEXT, conbin TEXT);
CREATE TABLE pg_catalog.pg_index (
    indexrelid OID, indrelid OID, indnatts INT2, indnkeyatts INT2,
    indisunique BOOL, indnullsnotdistinct BOOL, indisprimary BOOL,
    indisexclusion BOOL, indimmediate BOOL, indisclustered BOOL,
    indisvalid BOOL, indcheckxmin BOOL, indisready BOOL, indislive BOOL,
    indisreplident BOOL, indkey TEXT, indcollation TEXT, indclass TEXT,
    indoption TEXT, indexprs TEXT, indpred TEXT);
CREATE TABLE pg_catalog.pg_inherits (
    inhrelid OID, inhparent OID, inhseqno INT4, inhdetachpending BOOL);
CREATE TABLE pg_catalog.pg_partitioned_table (
    partrelid OID, partstrat CHAR, partnatts INT2, partdefid OID,
    partattrs TEXT, partclass TEXT, partcollation TEXT, partexprs TEXT);
CREATE TABLE pg_catalog.pg_trigger (
    oid OID, tgrelid OID, tgparentid OID, tgname NAME, tgfoid OID,
    tgtype INT2, tgenabled CHAR, tgisinternal BOOL, tgconstrrelid OID,
    tgconstrindid OID, tgconstraint OID, tgdeferrable BOOL,
    tginitdeferred BOOL, tgnargs INT2, tgattr TEXT, tgargs TEXT,
    tgqual TEXT, tgoldtable NAME, tgnewtable NAME);
CREATE TABLE pg_catalog.pg_rewrite (
    oid OID, rulename NAME, ev_class OID, ev_type CHAR, ev_enabled CHAR,
    is_instead BOOL, ev_qual TEXT, ev_action TEXT);
CREATE TABLE pg_catalog.pg_policy (
    oid OID, polname NAME, polrelid OID, polcmd CHAR, polpermissive BOOL,
    polroles TEXT, polqual TEXT, polwithcheck TEXT);
CREATE TABLE pg_catalog.pg_depend (
    classid OID, objid OID, objsubid INT4, refclassid OID, refobjid OID,
    refobjsubid INT4, deptype CHAR);
CREATE TABLE pg_catalog.pg_proc (
    oid OID, proname NAME, pronamespace OID, proowner OID, prolang OID,
    procost FLOAT4, prorows FLOAT4, provariadic OID, prosupport TEXT,
    prokind CHAR, prosecdef BOOL, proleakproof BOOL, proisstrict BOOL,
    proretset BOOL, provolatile CHAR, proparallel CHAR, pronargs INT2,
    pronargdefaults INT2, prorettype OID, proargtypes TEXT,
    proallargtypes TEXT, proargmodes TEXT, proargnames TEXT,
    proargdefaults TEXT, protrftypes TEXT, prosrc TEXT, probin TEXT,
    prosqlbody TEXT, proconfig TEXT, proacl TEXT);
CREATE TABLE pg_catalog.pg_aggregate (
    aggfnoid OID, aggkind CHAR, aggnumdirectargs INT2, aggtransfn TEXT,
    aggfinalfn TEXT, aggtranstype OID);
CREATE TABLE pg_catalog.pg_language (
    oid OID, lanname NAME, lanowner OID, lanispl BOOL, lanpltrusted BOOL,
    lanplcallfoid OID, laninline OID, lanvalidator OID, lanacl TEXT);
CREATE TABLE pg_catalog.pg_operator (
    oid OID, oprname NAME, oprnamespace OID, oprowner OID, oprkind CHAR,
    oprcanmerge BOOL, oprcanhash BOOL, oprleft OID, oprright OID,
    oprresult OID, oprcom OID, oprnegate OID, oprcode TEXT, oprrest TEXT,
    oprjoin TEXT);
CREATE TABLE pg_catalog.pg_opclass (
    oid OID, opcmethod OID, opcname NAME, opcnamespace OID, opcowner OID,
    opcfamily OID, opcintype OID, opcdefault BOOL, opckeytype OID);
CREATE TABLE pg_catalog.pg_opfamily (
    oid OID, opfmethod OID, opfname NAME, opfnamespace OID, opfowner OID);
CREATE TABLE pg_catalog.pg_amop (
    oid OID, amopfamily OID, amoplefttype OID, amoprighttype OID,
    amopstrategy INT2, amoppurpose CHAR, amopopr OID, amopmethod OID,
    amopsortfamily OID);
CREATE TABLE pg_catalog.pg_amproc (
    oid OID, amprocfamily OID, amproclefttype OID, amprocrighttype OID,
    amprocnum INT2, amproc TEXT);
CREATE TABLE pg_catalog.pg_ts_config (
    oid OID, cfgname NAME, cfgnamespace OID, cfgowner OID, cfgparser OID);
CREATE TABLE pg_catalog.pg_ts_dict (
    oid OID, dictname NAME, dictnamespace OID, dictowner OID,
    dicttemplate OID, dictinitoption TEXT);
CREATE TABLE pg_catalog.pg_ts_parser (
    oid OID, prsname NAME, prsnamespace OID);
CREATE TABLE pg_catalog.pg_ts_template (
    oid OID, tmplname NAME, tmplnamespace OID);
CREATE TABLE pg_catalog.pg_cast (
    oid OID, castsource OID, casttarget OID, castfunc OID,
    castcontext CHAR, castmethod CHAR);
CREATE TABLE pg_catalog.pg_collation (
    oid OID, collname NAME, collnamespace OID, collowner OID,
    collprovider CHAR, collisdeterministic BOOL, collencoding INT4,
    collcollate TEXT, collctype TEXT, colliculocale TEXT,
    collicurules TEXT, collversion TEXT);
CREATE TABLE pg_catalog.pg_conversion (
    oid OID, conname NAME, connamespace OID, conowner OID,
    conforencoding INT4, contoencoding INT4, conproc TEXT, condefault BOOL);
CREATE TABLE pg_catalog.pg_enum (
    oid OID, enumtypid OID, enumsortorder FLOAT4, enumlabel NAME);
CREATE TABLE pg_catalog.pg_range (
    rngtypid OID, rngsubtype OID, rngmultitypid OID, rngcollation OID,
    rngsubopc OID, rngcanonical TEXT, rngsubdiff TEXT);
CREATE TABLE pg_catalog.pg_extension (
    oid OID, extname NAME, extowner OID, extnamespace OID,
    extrelocatable BOOL, extversion TEXT, extconfig TEXT, extcondition TEXT);
CREATE TABLE pg_catalog.pg_available_extensions (
    name NAME, default_version TEXT, installed_version TEXT, comment TEXT);
CREATE TABLE pg_catalog.pg_event_trigger (
    oid OID, evtname NAME, evtevent NAME, evtowner OID, evtfoid OID,
    evtenabled CHAR, evttags TEXT);
CREATE TABLE pg_catalog.pg_foreign_data_wrapper (
    oid OID, fdwname NAME, fdwowner OID, fdwhandler OID, fdwvalidator OID,
    fdwacl TEXT, fdwoptions TEXT);
CREATE TABLE pg_catalog.pg_foreign_server (
    oid OID, srvname NAME, srvowner OID, srvfdw OID, srvtype TEXT,
    srvversion TEXT, srvacl TEXT, srvoptions TEXT);
CREATE TABLE pg_catalog.pg_foreign_table (
    ftrelid OID, ftserver OID, ftoptions TEXT);
CREATE TABLE pg_catalog.pg_user_mapping (
    oid OID, umuser OID, umserver OID, umoptions TEXT);
CREATE TABLE pg_catalog.pg_sequence (
    seqrelid OID, seqtypid OID, seqstart INT8, seqincrement INT8,
    seqmax INT8, seqmin INT8, seqcache INT8, seqcycle BOOL);
CREATE TABLE pg_catalog.pg_publication (
    oid OID, pubname NAME, pubowner OID, puballtables BOOL, pubinsert BOOL,
    pubupdate BOOL, pubdelete BOOL, pubtruncate BOOL, pubviaroot BOOL);
CREATE TABLE pg_catalog.pg_publication_rel (
    oid OID, prpubid OID, prrelid OID, prqual TEXT, prattrs TEXT);
CREATE TABLE pg_catalog.pg_publication_namespace (
    oid OID, pnpubid OID, pnnspid OID);
CREATE TABLE pg_catalog.pg_subscription (
    oid OID, subdbid OID, subname NAME, subowner OID, subenabled BOOL);
CREATE TABLE pg_catalog.pg_statistic_ext (
    oid OID, stxrelid OID, stxname NAME, stxnamespace OID, stxowner OID,
    stxstattarget INT4, stxkeys TEXT, stxkind TEXT, stxexprs TEXT);
CREATE TABLE pg_catalog.pg_default_acl (
    oid OID, defaclrole OID, defaclnamespace OID, defaclobjtype CHAR,
    defaclacl TEXT);
CREATE TABLE pg_catalog.pg_db_role_setting (
    setdatabase OID, setrole OID, setconfig TEXT);
CREATE TABLE pg_catalog.pg_seclabel (
    objoid OID, classoid OID, objsubid INT4, provider TEXT, label TEXT);
CREATE TABLE pg_catalog.pg_stat_activity (
    datid OID, datname NAME, pid INT4, leader_pid INT4, usesysid OID,
    usename NAME, application_name TEXT, client_addr TEXT,
    client_hostname TEXT, client_port INT4, backend_start TEXT,
    xact_start TEXT, query_start TEXT, state_change TEXT,
    wait_event_type TEXT, wait_event TEXT, state TEXT, backend_xid TEXT,
    backend_xmin TEXT, query_id INT8, query TEXT, backend_type TEXT);
CREATE TABLE pg_catalog.pg_locks (
    locktype TEXT, database OID, relation OID, page INT4, tuple INT2,
    virtualxid TEXT, transactionid TEXT, classid OID, objid OID,
    objsubid INT2, virtualtransaction TEXT, pid INT4, mode TEXT,
    granted BOOL, fastpath BOOL, waitstart TEXT);
CREATE TABLE pg_catalog.pg_stat_user_tables (
    relid OID, schemaname NAME, relname NAME, seq_scan INT8,
    seq_tup_read INT8, idx_scan INT8, idx_tup_fetch INT8, n_tup_ins INT8,
    n_tup_upd INT8, n_tup_del INT8, n_live_tup INT8, n_dead_tup INT8);
CREATE TABLE pg_catalog.pg_stat_all_tables (
    relid OID, schemaname NAME, relname NAME, seq_scan INT8,
    seq_tup_read INT8, idx_scan INT8, idx_tup_fetch INT8, n_tup_ins INT8,
    n_tup_upd INT8, n_tup_del INT8, n_live_tup INT8, n_dead_tup INT8);
CREATE TABLE pg_catalog.pg_statio_user_tables (
    relid OID, schemaname NAME, relname NAME, heap_blks_read INT8,
    heap_blks_hit INT8);

CREATE TABLE pg_catalog.pg_tables (
    schemaname NAME, tablename NAME, tableowner NAME, tablespace NAME,
    hasindexes BOOL, hasrules BOOL, hastriggers BOOL, rowsecurity BOOL);
CREATE TABLE pg_catalog.pg_views (
    schemaname NAME, viewname NAME, viewowner NAME, definition TEXT);
CREATE TABLE pg_catalog.pg_matviews (
    schemaname NAME, matviewname NAME, matviewowner NAME, tablespace NAME,
    hasindexes BOOL, ispopulated BOOL, definition TEXT);
CREATE TABLE pg_catalog.pg_indexes (
    schemaname NAME, tablename NAME, indexname NAME, tablespace NAME,
    indexdef TEXT);
CREATE TABLE pg_catalog.pg_sequences (
    schemaname NAME, sequencename NAME, sequenceowner NAME, data_type TEXT,
    start_value INT8, min_value INT8, max_value INT8, increment_by INT8,
    cycle BOOL, cache_size INT8, last_value INT8);
CREATE TABLE pg_catalog.pg_get_keywords (
    word TEXT, catcode CHAR, barelabel BOOL, catdesc TEXT, baredesc TEXT);
CREATE TABLE pg_catalog.pg_timezone_names (
    name TEXT, abbrev TEXT, utc_offset TEXT, is_dst BOOL);

CREATE TABLE information_schema.schemata (
    catalog_name NAME, schema_name NAME, schema_owner NAME,
    default_character_set_catalog NAME, default_character_set_schema NAME,
    default_character_set_name NAME, sql_path TEXT);
CREATE TABLE information_schema.tables (
    table_catalog NAME, table_schema NAME, table_name NAME, table_type TEXT,
    self_referencing_column_name NAME, reference_generation TEXT,
    user_defined_type_catalog NAME, user_defined_type_schema NAME,
    user_defined_type_name NAME, is_insertable_into TEXT, is_typed TEXT,
    commit_action TEXT);
CREATE TABLE information_schema.columns (
    table_catalog NAME, table_schema NAME, table_name NAME,
    column_name NAME, ordinal_position INT4, column_default TEXT,
    is_nullable TEXT, data_type TEXT, character_maximum_length INT4,
    character_octet_length INT4, numeric_precision INT4,
    numeric_precision_radix INT4, numeric_scale INT4,
    datetime_precision INT4, interval_type TEXT, interval_precision INT4,
    character_set_catalog NAME, character_set_schema NAME,
    character_set_name NAME, collation_catalog NAME, collation_schema NAME,
    collation_name NAME, domain_catalog NAME, domain_schema NAME,
    domain_name NAME, udt_catalog NAME, udt_schema NAME, udt_name NAME,
    scope_catalog NAME, scope_schema NAME, scope_name NAME,
    maximum_cardinality INT4, dtd_identifier NAME, is_self_referencing TEXT,
    is_identity TEXT, identity_generation TEXT, identity_start TEXT,
    identity_increment TEXT, identity_maximum TEXT, identity_minimum TEXT,
    identity_cycle TEXT, is_generated TEXT, generation_expression TEXT,
    is_updatable TEXT);
CREATE TABLE information_schema.views (
    table_catalog NAME, table_schema NAME, table_name NAME,
    view_definition TEXT, check_option TEXT, is_updatable TEXT,
    is_insertable_into TEXT, is_trigger_updatable TEXT,
    is_trigger_deletable TEXT, is_trigger_insertable_into TEXT);
CREATE TABLE information_schema.table_constraints (
    constraint_catalog NAME, constraint_schema NAME, constraint_name NAME,
    table_catalog NAME, table_schema NAME, table_name NAME,
    constraint_type TEXT, is_deferrable TEXT, initially_deferred TEXT,
    enforced TEXT, nulls_distinct TEXT);
CREATE TABLE information_schema.key_column_usage (
    constraint_catalog NAME, constraint_schema NAME, constraint_name NAME,
    table_catalog NAME, table_schema NAME, table_name NAME,
    column_name NAME, ordinal_position INT4,
    position_in_unique_constraint INT4);
CREATE TABLE information_schema.constraint_column_usage (
    table_catalog NAME, table_schema NAME, table_name NAME,
    column_name NAME, constraint_catalog NAME, constraint_schema NAME,
    constraint_name NAME);
CREATE TABLE information_schema.referential_constraints (
    constraint_catalog NAME, constraint_schema NAME, constraint_name NAME,
    unique_constraint_catalog NAME, unique_constraint_schema NAME,
    unique_constraint_name NAME, match_option TEXT, update_rule TEXT,
    delete_rule TEXT);
CREATE TABLE information_schema.routines (
    specific_catalog NAME, specific_schema NAME, specific_name NAME,
    routine_catalog NAME, routine_schema NAME, routine_name NAME,
    routine_type TEXT, data_type TEXT, routine_definition TEXT);
CREATE TABLE information_schema.sequences (
    sequence_catalog NAME, sequence_schema NAME, sequence_name NAME,
    data_type TEXT, start_value TEXT, minimum_value TEXT,
    maximum_value TEXT, increment TEXT, cycle_option TEXT);
CREATE TABLE information_schema.character_sets (
    character_set_catalog NAME, character_set_schema NAME,
    character_set_name NAME, character_repertoire TEXT, form_of_use TEXT,
    default_collate_catalog NAME, default_collate_schema NAME,
    default_collate_name NAME);
CREATE TABLE information_schema.table_privileges (
    grantor NAME, grantee NAME, table_catalog NAME, table_schema NAME,
    table_name NAME, privilege_type TEXT, is_grantable TEXT,
    with_hierarchy TEXT);
CREATE TABLE information_schema.triggers (
    trigger_catalog NAME, trigger_schema NAME, trigger_name NAME,
    event_manipulation TEXT, event_object_catalog NAME,
    event_object_schema NAME, event_object_table NAME,
    action_statement TEXT, action_timing TEXT);
