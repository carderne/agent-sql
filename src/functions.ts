import type { SelectStatement, WhereExpr, WhereValue, FuncCall } from "./ast";
import { SanitiseError } from "./errors";
import { Err, Ok, type Result } from "./result";

export type DbType = "postgres" | "pglite" | "sqlite" | "clickhouse";

export const DEFAULT_DB = "postgres";

/*
 * Standard SQL / common aggregate & scalar functions shared across all engines.
 */
const COMMON_FUNCTIONS: readonly string[] = [
  // Aggregates
  "count",
  "sum",
  "avg",
  "min",
  "max",

  // Conditionals / nulls
  "coalesce",
  "nullif",
  "greatest",
  "least",

  // String
  "lower",
  "upper",
  "trim",
  "ltrim",
  "rtrim",
  "length",
  "replace",
  "substr",
  "substring",
  "concat",
  "reverse",
  "repeat",
  "position",
  "left",
  "right",
  "lpad",
  "rpad",
  "translate",
  "char_length",
  "character_length",
  "octet_length",
  "overlay",
  "ascii",
  "chr",
  "starts_with",

  // Math
  "abs",
  "ceil",
  "ceiling",
  "floor",
  "round",
  "trunc",
  "truncate",
  "mod",
  "power",
  "sqrt",
  "cbrt",
  "log",
  "ln",
  "exp",
  "sign",
  "random",
  "pi",
  "degrees",
  "radians",
  "div",
  "gcd",
  "lcm",

  // Casting / type
  "cast",
];

/*
 * pgvector extension functions.
 */
const PGVECTOR_FUNCTIONS: readonly string[] = [
  "l2_distance",
  "inner_product",
  "cosine_distance",
  "l1_distance",
  "vector_dims",
  "vector_norm",
  "l2_normalize",
  "binary_quantize",
  "subvector",
  "vector_avg",
];

/*
 * PostGIS extension functions.
 *
 * This covers the most commonly used subset — geometry constructors,
 * accessors, spatial relationships, measurements, transformations,
 * and indexing helpers. Raster, topology, and network functions are
 * omitted for now; users can add them via allowExtraFunctions.
 */
const POSTGIS_FUNCTIONS: readonly string[] = [
  // Constructors
  "st_geomfromtext",
  "st_geomfromwkb",
  "st_geomfromewkt",
  "st_geomfromewkb",
  "st_geomfromgeojson",
  "st_geogfromtext",
  "st_geogfromwkb",
  "st_makepoint",
  "st_makepoint",
  "st_makepointm",
  "st_makeenvelope",
  "st_makeline",
  "st_makepolygon",
  "st_makebox2d",
  "st_point",
  "st_pointz",
  "st_pointm",
  "st_pointzm",
  "st_polygon",
  "st_linefrommultipoint",
  "st_tileenvelope",
  "st_hexagongrid",
  "st_squaregrid",
  "st_letters",
  "st_collect",
  "st_linemerge",
  "st_buildarea",
  "st_polygonize",
  "st_unaryunion",

  // Output / serialisation
  "st_astext",
  "st_asewkt",
  "st_asbinary",
  "st_asewkb",
  "st_asgeojson",
  "st_asgml",
  "st_askml",
  "st_assvg",
  "st_astwkb",
  "st_asmvtgeom",
  "st_asmvt",
  "st_asencodedpolyline",
  "st_ashexewkb",
  "st_aslatlontext",

  // Accessors
  "st_x",
  "st_y",
  "st_z",
  "st_m",
  "st_geometrytype",
  "st_srid",
  "st_dimension",
  "st_coorddim",
  "st_numgeometries",
  "st_numpoints",
  "st_npoints",
  "st_nrings",
  "st_numinteriorrings",
  "st_numinteriorring",
  "st_exteriorring",
  "st_interiorringn",
  "st_geometryn",
  "st_pointn",
  "st_startpoint",
  "st_endpoint",
  "st_envelope",
  "st_boundingdiagonal",
  "st_xmin",
  "st_xmax",
  "st_ymin",
  "st_ymax",
  "st_zmin",
  "st_zmax",
  "st_isempty",
  "st_isclosed",
  "st_isring",
  "st_issimple",
  "st_isvalid",
  "st_isvalidreason",
  "st_isvaliddetail",
  "st_hasm",
  "st_hasz",
  "st_ismeasured",

  // Spatial relationships (predicates)
  "st_intersects",
  "st_disjoint",
  "st_contains",
  "st_within",
  "st_covers",
  "st_coveredby",
  "st_crosses",
  "st_overlaps",
  "st_touches",
  "st_equals",
  "st_relate",
  "st_containsproperly",
  "st_dwithin",
  "st_3dintersects",
  "st_3ddwithin",
  "st_orderingequals",

  // Measurements
  "st_distance",
  "st_3ddistance",
  "st_maxdistance",
  "st_area",
  "st_length",
  "st_length2d",
  "st_3dlength",
  "st_perimeter",
  "st_azimuth",
  "st_angle",
  "st_hausdorffdistance",
  "st_frechetdistance",
  "st_longestline",
  "st_shortestline",

  // Transformations
  "st_transform",
  "st_setsrid",
  "st_force2d",
  "st_force3d",
  "st_force3dz",
  "st_force3dm",
  "st_force4d",
  "st_forcecollection",
  "st_forcepolygoncw",
  "st_forcepolygonccw",
  "st_forcecurve",
  "st_forcesfs",
  "st_multi",
  "st_normalize",
  "st_flipcoordinates",
  "st_translate",
  "st_scale",
  "st_rotate",
  "st_rotatex",
  "st_rotatey",
  "st_rotatez",
  "st_affine",
  "st_transscale",
  "st_snap",
  "st_snaptogrid",
  "st_segmentize",
  "st_simplify",
  "st_simplifypreservetopology",
  "st_simplifyvw",
  "st_chaikinsmoothing",
  "st_seteffectivearea",
  "st_filterbym",
  "st_locatebetween",
  "st_locatebetweenelevations",
  "st_offsetcurve",

  // Processing / overlay
  "st_intersection",
  "st_union",
  "st_difference",
  "st_symdifference",
  "st_buffer",
  "st_convexhull",
  "st_concavehull",
  "st_minimumboundingcircle",
  "st_minimumboundingradius",
  "st_orientedenvelope",
  "st_centroid",
  "st_pointonsurface",
  "st_geometricmedian",
  "st_voronoipolygons",
  "st_voronoilines",
  "st_delaunaytriangles",
  "st_subdivide",
  "st_split",
  "st_sharedpaths",
  "st_node",
  "st_clusterdbscan",
  "st_clusterkmeans",
  "st_clusterintersecting",
  "st_clusterwithin",
  "st_makevalid",

  // Linear referencing
  "st_lineinterpolatepoint",
  "st_lineinterpolatepoints",
  "st_linelocatepoint",
  "st_linesubstring",
  "st_addmeasure",
  "st_closestpoint",
  "st_linefromencodedpolyline",

  // Bounding box
  "box2d",
  "box3d",
  "st_expand",
  "st_estimatedextent",
  "st_extent",
  "st_3dextent",
  "st_memsize",

  // Geography-specific
  "st_distancesphere",
  "st_distancespheroid",
  "st_project",

  // Aggregate
  "st_memunion",
  "st_polygonize",

  // Misc
  "st_nband",
  "st_numbands",
  "st_summary",
  "st_dump",
  "st_dumppoints",
  "st_dumprings",
  "postgis_version",
  "postgis_full_version",
  "postgis_geos_version",
  "postgis_proj_version",
  "postgis_lib_version",
  "postgis_scripts_installed",
  "postgis_scripts_released",
  "postgis_type_name",
  "populate_geometry_columns",
  "find_srid",
  "updategeometrysrid",
  "addgeometrycolumn",
  "dropgeometrycolumn",
  "geography",
  "geometry",
];

/*
 * PostgreSQL (and PGlite) built-in & common-extension functions.
 *
 * Dangerous PG functions that are deliberately excluded:
 *   pg_read_file, pg_read_binary_file, pg_write_file,
 *   lo_import, lo_export, lo_from_bytea,
 *   pg_execute_server_program,
 *   dblink, dblink_exec, dblink_connect,
 *   copy_to, copy_from,
 *   pg_notify, pg_terminate_backend, pg_cancel_backend,
 *   set_config, pg_reload_conf,
 *   pg_ls_dir, pg_stat_file,
 *   current_setting (can reveal server config but is commonly used —
 *     included because it's read-only and widely needed)
 */
const POSTGRES_FUNCTIONS: readonly string[] = [
  ...COMMON_FUNCTIONS,

  // Additional aggregates
  "array_agg",
  "string_agg",
  "json_agg",
  "jsonb_agg",
  "json_object_agg",
  "jsonb_object_agg",
  "bool_and",
  "bool_or",
  "every",
  "bit_and",
  "bit_or",
  "bit_xor",
  "corr",
  "covar_pop",
  "covar_samp",
  "regr_avgx",
  "regr_avgy",
  "regr_count",
  "regr_intercept",
  "regr_r2",
  "regr_slope",
  "regr_sxx",
  "regr_sxy",
  "regr_syy",
  "stddev",
  "stddev_pop",
  "stddev_samp",
  "variance",
  "var_pop",
  "var_samp",
  "percentile_cont",
  "percentile_disc",
  "mode",
  "rank",
  "dense_rank",
  "percent_rank",
  "cume_dist",
  "ntile",
  "lag",
  "lead",
  "first_value",
  "last_value",
  "nth_value",
  "row_number",

  // String (PG extras)
  "initcap",
  "strpos",
  "encode",
  "decode",
  "md5",
  "sha256",
  "sha224",
  "sha384",
  "sha512",
  "format",
  "concat_ws",
  "regexp_replace",
  "regexp_match",
  "regexp_matches",
  "regexp_split_to_array",
  "regexp_split_to_table",
  "split_part",
  "btrim",
  "bit_length",
  "quote_ident",
  "quote_literal",
  "quote_nullable",
  "to_hex",
  "convert",
  "convert_from",
  "convert_to",
  "string_to_array",
  "array_to_string",

  // Date / time
  "now",
  "current_timestamp",
  "current_date",
  "current_time",
  "localtime",
  "localtimestamp",
  "clock_timestamp",
  "statement_timestamp",
  "transaction_timestamp",
  "timeofday",
  "date_trunc",
  "date_part",
  "extract",
  "age",
  "to_char",
  "to_date",
  "to_timestamp",
  "to_number",
  "make_date",
  "make_time",
  "make_timestamp",
  "make_timestamptz",
  "make_interval",
  "justify_days",
  "justify_hours",
  "justify_interval",
  "isfinite",

  // JSON / JSONB
  "json_extract_path",
  "json_extract_path_text",
  "jsonb_extract_path",
  "jsonb_extract_path_text",
  "json_array_length",
  "jsonb_array_length",
  "json_typeof",
  "jsonb_typeof",
  "json_build_object",
  "jsonb_build_object",
  "json_build_array",
  "jsonb_build_array",
  "to_json",
  "to_jsonb",
  "row_to_json",
  "json_each",
  "json_each_text",
  "jsonb_each",
  "jsonb_each_text",
  "json_object_keys",
  "jsonb_object_keys",
  "json_populate_record",
  "jsonb_populate_record",
  "json_populate_recordset",
  "jsonb_populate_recordset",
  "json_to_record",
  "jsonb_to_record",
  "json_to_recordset",
  "jsonb_to_recordset",
  "json_array_elements",
  "jsonb_array_elements",
  "json_array_elements_text",
  "jsonb_array_elements_text",
  "jsonb_set",
  "jsonb_set_lax",
  "jsonb_insert",
  "jsonb_path_query",
  "jsonb_path_query_array",
  "jsonb_path_query_first",
  "jsonb_path_exists",
  "jsonb_path_match",
  "jsonb_strip_nulls",
  "jsonb_pretty",
  "json_strip_nulls",

  // Text search
  "to_tsvector",
  "to_tsquery",
  "plainto_tsquery",
  "phraseto_tsquery",
  "websearch_to_tsquery",
  "ts_rank",
  "ts_rank_cd",
  "ts_headline",
  "tsvector_to_array",
  "array_to_tsvector",
  "numnode",
  "querytree",
  "ts_rewrite",
  "setweight",
  "strip",
  "ts_debug",
  "ts_lexize",
  "ts_parse",
  "ts_token_type",
  "get_current_ts_config",

  // Array
  "array_append",
  "array_cat",
  "array_dims",
  "array_fill",
  "array_length",
  "array_lower",
  "array_ndims",
  "array_position",
  "array_positions",
  "array_prepend",
  "array_remove",
  "array_replace",
  "array_upper",
  "cardinality",
  "unnest",
  "generate_subscripts",

  // Range
  "lower",
  "upper",
  "isempty",
  "lower_inc",
  "lower_inf",
  "upper_inc",
  "upper_inf",
  "range_merge",

  // Misc
  "generate_series",
  "pg_typeof",
  "current_setting",
  "current_database",
  "current_schema",
  "current_schemas",
  "current_user",
  "session_user",
  "inet_client_addr",
  "inet_client_port",
  "version",
  "obj_description",
  "col_description",
  "shobj_description",
  "has_table_privilege",
  "has_column_privilege",
  "has_schema_privilege",
  "txid_current",
  "txid_current_snapshot",

  // Geometric (harmless)
  "area",
  "center",
  "diameter",
  "height",
  "width",
  "isclosed",
  "isopen",
  "npoints",
  "pclose",
  "popen",
  "radius",

  // Network
  "abbrev",
  "broadcast",
  "family",
  "host",
  "hostmask",
  "masklen",
  "netmask",
  "network",
  "set_masklen",
  "inet_merge",
  "inet_same_family",

  // UUID
  "gen_random_uuid",
  "uuid_generate_v1",
  "uuid_generate_v4",

  ...PGVECTOR_FUNCTIONS,
  ...POSTGIS_FUNCTIONS,
];

/*
 * SQLite built-in functions.
 *
 * Dangerous SQLite functions that are deliberately excluded:
 *   load_extension — loads arbitrary shared libraries (RCE)
 *   readfile       — reads arbitrary files from disk (fileio ext)
 *   writefile      — writes arbitrary files to disk  (fileio ext)
 *   edit           — opens an editor (edit ext)
 *   fts3, fts4, fts5 — these are virtual-table modules, not plain functions
 */
const SQLITE_FUNCTIONS: readonly string[] = [
  ...COMMON_FUNCTIONS,

  // Aggregate (SQLite extras)
  "group_concat",
  "total",

  // String (SQLite extras)
  "char",
  "format",
  "glob",
  "hex",
  "unhex",
  "instr",
  "like",
  "ltrim",
  "rtrim",
  "trim",
  "printf",
  "quote",
  "soundex",
  "unicode",
  "zeroblob",

  // Math (SQLite extras — available since 3.35+)
  "acos",
  "acosh",
  "asin",
  "asinh",
  "atan",
  "atan2",
  "atanh",
  "cos",
  "cosh",
  "sin",
  "sinh",
  "tan",
  "tanh",

  // Date / time
  "date",
  "time",
  "datetime",
  "julianday",
  "unixepoch",
  "strftime",
  "timediff",

  // Type / meta
  "typeof",
  "type",
  "last_insert_rowid",
  "changes",
  "total_changes",
  "sqlite_version",

  // JSON (SQLite 3.38+)
  "json",
  "json_array",
  "json_array_length",
  "json_extract",
  "json_insert",
  "json_object",
  "json_patch",
  "json_remove",
  "json_replace",
  "json_set",
  "json_type",
  "json_valid",
  "json_quote",
  "json_group_array",
  "json_group_object",
  "json_each",
  "json_tree",

  // Misc
  "iif",
  "ifnull",
  "likely",
  "unlikely",
  "max",
  "min",
  "nullif",
  "randomblob",
  "row_number",
  "rank",
  "dense_rank",
  "percent_rank",
  "cume_dist",
  "ntile",
  "lag",
  "lead",
  "first_value",
  "last_value",
  "nth_value",
];

/*
 * ClickHouse built-in functions.
 *
 * Dangerous ClickHouse functions deliberately excluded:
 *   file, url, s3, s3Cluster, hdfs, remote, remoteSecure, cluster,
 *   clusterAllReplicas — table functions that read external/arbitrary data
 *   input             — reads from client-supplied data stream
 *   executable, executablePool — run external processes (RCE)
 *   dictionary        — accesses external dictionary sources
 *
 * Note: ClickHouse aggregate combinators (-If, -Array, -State, -Merge, etc.)
 * are suffixes applied at the engine level, not separate function names visible
 * to the parser. Functions like countIf / sumIf are included explicitly because
 * they are the canonical names agents will write.
 */
const CLICKHOUSE_FUNCTIONS: readonly string[] = [
  ...COMMON_FUNCTIONS,

  // Conditional / null helpers
  "if",
  "multiif",
  "ifnull",
  "isnull",
  "isnotnull",
  "isnan",
  "isinf",
  "isfinite",

  // Type conversion — to<Type> family
  "toint8",
  "toint16",
  "toint32",
  "toint64",
  "toint128",
  "toint256",
  "touint8",
  "touint16",
  "touint32",
  "touint64",
  "touint128",
  "touint256",
  "tofloat32",
  "tofloat64",
  "todecimal32",
  "todecimal64",
  "todecimal128",
  "tobool",
  "tostring",
  "todate",
  "todate32",
  "todatetime",
  "todatetime64",
  "touuid",
  // OrNull / OrZero safe-conversion variants
  "toint8ornull",
  "toint16ornull",
  "toint32ornull",
  "toint64ornull",
  "touint8ornull",
  "touint16ornull",
  "touint32ornull",
  "touint64ornull",
  "tofloat32ornull",
  "tofloat64ornull",
  "tostringorNull",
  "todateornull",
  "todatetimeornull",
  "toint8orzero",
  "toint16orzero",
  "toint32orzero",
  "toint64orzero",
  "touint8orzero",
  "touint16orzero",
  "touint32orzero",
  "touint64orzero",
  "tofloat32orzero",
  "tofloat64orzero",
  "todateorzero",
  "todatetimeorzero",
  // reinterpret
  "reinterpret",
  "reinterpretasuint8",
  "reinterpretasuint16",
  "reinterpretasuint32",
  "reinterpretasuint64",
  "reinterpretasint8",
  "reinterpretasint16",
  "reinterpretasint32",
  "reinterpretasint64",
  "reinterpretasfloat32",
  "reinterpretasfloat64",
  "reinterpretasstring",
  "reinterpretasuuid",

  // Math
  "intdiv",
  "intdivorzero",
  "modulo",
  "moduloorzero",
  "positivemodulo",
  "negate",
  "abs",
  "gcd",
  "lcm",
  "max2",
  "min2",
  "sqrt",
  "cbrt",
  "exp",
  "exp2",
  "exp10",
  "log",
  "log2",
  "log10",
  "ln",
  "pow",
  "power",
  "sign",
  "floor",
  "ceil",
  "ceiling",
  "trunc",
  "truncate",
  "round",
  "roundbankers",
  "roundtoexp2",
  "roundduration",
  "roundage",
  "pi",
  "sin",
  "cos",
  "tan",
  "asin",
  "acos",
  "atan",
  "atan2",
  "sinh",
  "cosh",
  "tanh",
  "asinh",
  "acosh",
  "atanh",
  "hypot",
  "degrees",
  "radians",
  "factorial",
  "erf",
  "erfc",
  "lgamma",
  "tgamma",
  "random",
  "randuniform",
  "randnormal",
  "randlognormal",
  "randconstant",
  "bitand",
  "bitor",
  "bitxor",
  "bitnot",
  "bitshiftleft",
  "bitshiftright",
  "bitrotateleft",
  "bitrotateright",
  "bittest",
  "bitset",
  "bitcount",
  "intexp2",
  "intexp10",

  // String
  "notEmpty",
  "empty",
  "notempty",
  "lengthutf8",
  "charlength",
  "left",
  "right",
  "leftutf8",
  "rightutf8",
  "leftpad",
  "rightpad",
  "leftpadutf8",
  "rightpadutf8",
  "lowercase",
  "uppercase",
  "lowerutf8",
  "upperutf8",
  "isvalidutf8",
  "tovalidutf8",
  "trimboth",
  "trimleft",
  "trimright",
  "trimboth",
  "padstring",
  "startswith",
  "endswith",
  "substr",
  "substring",
  "substringutf8",
  "appendtrailingcharifabsent",
  "convertcharset",
  "base58encode",
  "base58decode",
  "trybase58decode",
  "base64encode",
  "base64decode",
  "trybase64decode",
  "base64urlencode",
  "base64urldecode",
  "trybase64urldecode",
  "hexencode",
  "hexdecode",
  "hex",
  "unhex",
  "char",
  "unicode",
  "encodeXMLComponent",
  "decodeXMLComponent",
  "decodeHTMLComponent",
  "extractTextFromHTML",
  "ascii",
  "soundex",
  "normalizeutf8nfc",
  "normalizeutf8nfd",
  "normalizeutf8nfkc",
  "normalizeutf8nfkd",
  "comparestringswithcollation",
  "format",
  "concat",
  "concatwithnull",
  "concatwithseparator",
  "concatwithseparatorornull",
  "splitbychar",
  "splitbystring",
  "splitbyregexp",
  "splitbywhitespace",
  "splitbynonalphanumeric",
  "arraystringsconcat",
  "alphatokenize",
  "tokens",
  "position",
  "locate",
  "positioncaseinsensitive",
  "positionutf8",
  "positioncaseinsensitiveutf8",
  "multipositioncaseinsensitive",
  "multisearchallanswers",
  "multisearchfirst",
  "multisearchfirstindex",
  "multisearchany",
  "match",
  "multimatchany",
  "multimatchanywhere",
  "multimatchallindices",
  "multifuzzymatchair",
  "extract",
  "extractall",
  "extractallgroups",
  "extractallgroupshorizontal",
  "extractallgroupsvertical",
  "like",
  "ilike",
  "notlike",
  "notilike",
  "replaceone",
  "replaceall",
  "replaceregexpone",
  "replaceregexpall",
  "replaceregexpwithregexp",
  "regexpquotemeta",
  "translate",
  "translateutf8",
  "countmatches",
  "countmatchescaseinsensitive",
  "printf",
  "format",
  "editdistance",
  "jarowinklerdistance",
  "fingerprint",
  "ngrams",
  "ngramssimhash",
  "ngramssimhashargnin",
  "ngramssimhashcaseinsensitive",
  "ngramssimhashcaseinsensitiveargmin",

  // Hashing / encoding (read-only, safe)
  "halfmd5",
  "md5",
  "sipHash64",
  "sipHash128",
  "sipHash64Keyed",
  "sipHash128Keyed",
  "sipHash128Reference",
  "sipHash128ReferenceKeyed",
  "cityHash64",
  "intHash32",
  "intHash64",
  "sha1",
  "sha224",
  "sha256",
  "sha512",
  "sha512_256",
  "xxhash32",
  "xxhash64",
  "xxh3",
  "farmhash64",
  "javaHash",
  "javaHashUTF16LE",
  "hiveHash",
  "metroHash64",
  "murmurHash2_32",
  "murmurHash2_64",
  "gccMurmurHash",
  "kafkaMurmurHash",
  "murmurHash3_32",
  "murmurHash3_64",
  "murmurHash3_128",
  "farmstrong32",
  "wyHash64",
  "kostikHash64",

  // Date / time
  "now",
  "now64",
  "today",
  "yesterday",
  "toYear",
  "toQuarter",
  "toMonth",
  "toDayOfYear",
  "toDayOfMonth",
  "toDayOfWeek",
  "toHour",
  "toMinute",
  "toSecond",
  "toMillisecond",
  "toUnixTimestamp",
  "toUnixTimestamp64Milli",
  "toUnixTimestamp64Micro",
  "toUnixTimestamp64Nano",
  "toStartOfYear",
  "toStartOfISOYear",
  "toStartOfQuarter",
  "toStartOfMonth",
  "toLastDayOfMonth",
  "toStartOfWeek",
  "toLastDayOfWeek",
  "toStartOfDay",
  "toStartOfHour",
  "toStartOfMinute",
  "toStartOfSecond",
  "toStartOfMillisecond",
  "toStartOfMicrosecond",
  "toStartOfNanosecond",
  "toStartOfFiveMinutes",
  "toStartOfTenMinutes",
  "toStartOfFifteenMinutes",
  "toStartOfInterval",
  "toTime",
  "toRelativeYearNum",
  "toRelativeQuarterNum",
  "toRelativeMonthNum",
  "toRelativeWeekNum",
  "toRelativeDayNum",
  "toRelativeHourNum",
  "toRelativeMinuteNum",
  "toRelativeSecondNum",
  "toISOYear",
  "toISOWeek",
  "toWeek",
  "toYearWeek",
  "toTimezone",
  "timezoneof",
  "timezoneOffset",
  "fromUnixTimestamp",
  "fromUnixTimestamp64Milli",
  "fromUnixTimestamp64Micro",
  "fromUnixTimestamp64Nano",
  "toYYYYMM",
  "toYYYYMMDD",
  "toYYYYMMDDhhmmss",
  "toDateTimeOrNull",
  "toDateTimeOrZero",
  "toDate32OrNull",
  "toDate32OrZero",
  "parseDateTimeBestEffort",
  "parseDateTimeBestEffortOrNull",
  "parseDateTimeBestEffortOrZero",
  "parseDateTime",
  "parseDateTimeOrNull",
  "parseDateTimeOrZero",
  "parseDateTime32BestEffort",
  "formatDateTime",
  "formatDateTimeInJodaSyntax",
  "dateDiff",
  "dateAdd",
  "dateSub",
  "timeStampAdd",
  "timeStampSub",
  "addYears",
  "addMonths",
  "addWeeks",
  "addDays",
  "addHours",
  "addMinutes",
  "addSeconds",
  "addMilliseconds",
  "addMicroseconds",
  "addNanoseconds",
  "addQuarters",
  "subtractYears",
  "subtractMonths",
  "subtractWeeks",
  "subtractDays",
  "subtractHours",
  "subtractMinutes",
  "subtractSeconds",
  "subtractMilliseconds",
  "subtractMicroseconds",
  "subtractNanoseconds",
  "subtractQuarters",
  "age",
  "date_trunc",
  "date_diff",
  "date_add",
  "date_sub",
  "extract",
  "toIntervalYear",
  "toIntervalQuarter",
  "toIntervalMonth",
  "toIntervalWeek",
  "toIntervalDay",
  "toIntervalHour",
  "toIntervalMinute",
  "toIntervalSecond",
  "toIntervalMillisecond",
  "toIntervalMicrosecond",
  "toIntervalNanosecond",

  // Aggregates (standard names)
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "any",
  "anyLast",
  "anyHeavy",
  "argMin",
  "argMax",
  "groupArray",
  "groupArrayLast",
  "groupUniqArray",
  "groupArrayInsertAt",
  "groupArrayMovingAvg",
  "groupArrayMovingSum",
  "groupArraySample",
  "groupBitAnd",
  "groupBitOr",
  "groupBitXor",
  "groupBitmap",
  "groupBitmapAnd",
  "groupBitmapOr",
  "groupBitmapXor",
  "sumWithOverflow",
  "sumMap",
  "minMap",
  "maxMap",
  "skewSamp",
  "skewPop",
  "kurtSamp",
  "kurtPop",
  "uniq",
  "uniqExact",
  "uniqCombined",
  "uniqCombined64",
  "uniqHLL12",
  "uniqTheta",
  "uniqUpTo",
  "topK",
  "topKWeighted",
  "corrStable",
  "corr",
  "covarSamp",
  "covarPop",
  "covarSampStable",
  "covarPopStable",
  "stddevSamp",
  "stddevPop",
  "stddevSampStable",
  "stddevPopStable",
  "varSamp",
  "varPop",
  "varSampStable",
  "varPopStable",
  "median",
  "quantile",
  "quantiles",
  "quantileExact",
  "quantilesExact",
  "quantileExactLow",
  "quantileExactHigh",
  "quantileExactWeighted",
  "quantilesExactWeighted",
  "quantileTiming",
  "quantilesDeterministic",
  "quantileDeterministic",
  "quantileTDigest",
  "quantilesTDigest",
  "quantileTDigestWeighted",
  "quantilesTDigestWeighted",
  "quantileBFloat16",
  "quantilesBFloat16",
  "quantileBFloat16Weighted",
  "quantilesBFloat16Weighted",
  "quantileInterpolatedWeighted",
  "quantilesInterpolatedWeighted",
  "simpleLinearRegression",
  "stochasticLinearRegression",
  "stochasticLogisticRegression",
  "categoricalInformationValue",
  "studentTTest",
  "welchTTest",
  "mannWhitneyUTest",
  "rankCorr",
  "kolmogorovSmirnovTest",
  "histogram",
  "sequenceMatch",
  "sequenceCount",
  "windowFunnel",
  "retention",
  "largestTriangleThreeBuckets",
  "meanZTest",
  "contingency",
  "cramersV",
  "cramersVBiasCorrected",
  "theilsU",
  // -If combinator forms (most common; agents write these explicitly)
  "countif",
  "sumif",
  "avgif",
  "minif",
  "maxif",
  "anyif",
  "anylastif",
  "argminif",
  "argmaxif",
  "grouparrayif",
  "groupuniqueif",
  "uniqif",
  "uniqexactif",
  "uniqcombinedif",
  // Window functions (same names as standard SQL)
  "row_number",
  "rank",
  "dense_rank",
  "percent_rank",
  "cume_dist",
  "ntile",
  "lag",
  "lead",
  "first_value",
  "last_value",
  "nth_value",

  // Array
  "array",
  "range",
  "length",
  "empty",
  "notempty",
  "has",
  "hasall",
  "hasany",
  "hassubstr",
  "indexof",
  "countequal",
  "arrayjoin",
  "arrayDifference",
  "arrayDistinct",
  "arrayEnumerate",
  "arrayEnumerateDense",
  "arrayEnumerateUniq",
  "arrayPopBack",
  "arrayPopFront",
  "arrayPushBack",
  "arrayPushFront",
  "arrayResize",
  "arraySlice",
  "arraySort",
  "arrayReverseSort",
  "arrayPartialSort",
  "arrayPartialReverseSort",
  "arraySortByArg",
  "arrayReverse",
  "arrayFlatten",
  "arrayCompact",
  "arrayZip",
  "arrayZipUnaligned",
  "arrayMap",
  "arrayFilter",
  "arrayFill",
  "arrayReverseFill",
  "arraySplit",
  "arrayReverseSplit",
  "arrayExists",
  "arrayAll",
  "arrayFirst",
  "arrayFirstOrNull",
  "arrayLast",
  "arrayLastOrNull",
  "arrayFirstIndex",
  "arrayLastIndex",
  "arrayMin",
  "arrayMax",
  "arraySum",
  "arrayAvg",
  "arrayCumSum",
  "arrayCumSumNonNegative",
  "arrayProduct",
  "arrayStringConcat",
  "arrayReduce",
  "arrayReduceInRanges",
  "arrayConcat",
  "arrayCount",
  "arrayDot",
  "arrayUniq",
  "arrayIntersect",
  "arraySymmetricDifference",
  "arrayWithConstant",
  "arraymaprange",

  // Tuple / Map
  "tuple",
  "tupleElement",
  "tupleNames",
  "tuplepluscludeselect",
  "map",
  "mapAdd",
  "mapSubtract",
  "mapPopulateSeries",
  "mapContains",
  "mapKeys",
  "mapValues",
  "mapFromArrays",
  "mapApply",
  "mapFilter",
  "mapUpdate",
  "mapConcat",
  "mapSort",
  "mapReverseSort",
  "mapPartialSort",
  "mapPartialReverseSort",

  // JSON
  "JSONExtract",
  "JSONExtractString",
  "JSONExtractInt",
  "JSONExtractUInt",
  "JSONExtractFloat",
  "JSONExtractBool",
  "JSONExtractRaw",
  "JSONExtractArrayRaw",
  "JSONExtractKeysAndValues",
  "JSONExtractKeysAndRawValues",
  "JSONHas",
  "JSONKey",
  "JSONLength",
  "JSONType",
  "JSONMergePatch",
  "JSON_VALUE",
  "JSON_QUERY",
  "JSON_EXISTS",
  "toJSONString",
  "formatJSON",

  // UUID
  "generateUUIDv4",
  "generateUUIDv7",
  "serverUUID",
  "toUUID",
  "UUIDStringToNum",
  "UUIDNumToString",

  // IP
  "IPv4ToIPv6",
  "IPv4StringToNum",
  "IPv4NumToString",
  "IPv4NumToStringClassC",
  "IPv6StringToNum",
  "IPv6NumToString",
  "cutIPv6",
  "toIPv4",
  "toIPv6",
  "isIPv4String",
  "isIPv6String",

  // URL (parsing only — no fetching)
  "protocol",
  "domain",
  "domainWithoutWWW",
  "topLevelDomain",
  "firstSignificantSubdomain",
  "cutToFirstSignificantSubdomain",
  "cutToFirstSignificantSubdomainWithWWW",
  "port",
  "path",
  "pathFull",
  "queryString",
  "fragment",
  "queryStringAndFragment",
  "extractURLParameter",
  "extractURLParameters",
  "extractURLParameterNames",
  "URLHierarchy",
  "URLPathHierarchy",
  "encodeURLComponent",
  "decodeURLComponent",
  "encodeURLFormComponent",
  "decodeURLFormComponent",
  "netloc",

  // Geo
  "greatCircleDistance",
  "geoDistance",
  "greatCircleAngle",
  "pointInEllipses",
  "pointInPolygon",
  "geohashEncode",
  "geohashDecode",
  "geohashesInBox",
  "h3IsValid",
  "h3GetResolution",
  "h3EdgeAngle",
  "h3EdgeLengthM",
  "h3EdgeLengthKm",
  "geoToH3",
  "h3ToGeo",
  "h3ToGeoBoundary",
  "h3kRing",
  "h3GetBaseCell",
  "h3HexAreaM2",
  "h3HexAreaKm2",
  "h3IndexesAreNeighbors",
  "h3ToChildren",
  "h3ToParent",
  "h3ToString",
  "stringToH3",
  "h3IsResClassIII",
  "h3IsPentagon",
  "h3GetFaces",
  "h3CellAreaM2",
  "h3CellAreaRads2",
  "h3ToCenterChild",
  "h3ExactEdgeLengthM",
  "h3ExactEdgeLengthKm",
  "h3ExactEdgeLengthRads",
  "h3NumHexagons",
  "h3Line",
  "h3Distance",
  "h3HexRing",
  "h3GetUnidirectionalEdge",
  "h3UnidirectionalEdgeIsValid",
  "h3GetOriginIndexFromUnidirectionalEdge",
  "h3GetDestinationIndexFromUnidirectionalEdge",
  "h3GetIndexesFromUnidirectionalEdge",
  "h3GetUnidirectionalEdgesFromHexagon",
  "h3GetUnidirectionalEdgeBoundary",
  "h3MakeAddress",

  // Misc / meta (read-only)
  "version",
  "hostName",
  "getMacro",
  "fqdn",
  "basename",
  "visibleWidth",
  "toTypeName",
  "blockSize",
  "currentProfiles",
  "currentRoles",
  "enabledRoles",
  "defaultRoles",
  "currentUser",
  "user",
  "userId",
  "currentDatabase",
  "currentSchemas",
  "isConstant",
  "sleep",
  "sleepEachRow",
  "ignore",
  "indexHint",
  "identity",
  "assumeNotNull",
  "ifNull",
  "nullIf",
  "isNull",
  "isNotNull",
  "throwIf",
  "bar",
  "transform",
  "formatReadableSize",
  "formatReadableDecimalSize",
  "formatReadableQuantity",
  "formatReadableTimeDelta",
  "runningDifference",
  "runningDifferenceStartingWithFirstValue",
  "runningConcatArray",
  "rowNumberInBlock",
  "rowNumberInAllBlocks",
  "neighbor",
  "initializeAggregation",
  "finalizeAggregation",
  "runningAccumulate",
  "joinGet",
  "catboostEvaluate",
  "numbers",
  "generateSeries",
  "generate_series",
];

/*
 * Walks the entire AST and rejects any FuncCall whose name is not in the
 * allowed set. Returns an Err with a SanitiseError if a disallowed function
 * is found.
 */
export function checkFunctions(
  ast: SelectStatement,
  db: DbType,
  allowExtraFunctions: string[],
): Result<SelectStatement> {
  // Collect all func calls from the AST
  const allowed = buildAllowedSet(db, allowExtraFunctions);
  const bad = findDisallowedFunction(ast, allowed);
  if (bad !== null) {
    return Err(new SanitiseError(`Function '${bad}' is not allowed`));
  }
  return Ok(ast);
}

function buildSet(list: readonly string[]): Set<string> {
  return new Set(list.map((f) => f.toLowerCase()));
}

function getAllowedFunctions(db: DbType): Set<string> {
  switch (db) {
    case "postgres":
    case "pglite":
      return buildSet(POSTGRES_FUNCTIONS);
    case "sqlite":
      return buildSet(SQLITE_FUNCTIONS);
    case "clickhouse":
      return buildSet(CLICKHOUSE_FUNCTIONS);
  }
}

function buildAllowedSet(db: DbType, extra: string[]): Set<string> {
  const base = getAllowedFunctions(db);
  if (extra.length === 0) return base;
  // Create a copy so we don't mutate the shared set
  const merged = new Set(base);
  for (const f of extra) {
    merged.add(f.toLowerCase());
  }
  return merged;
}

/*
 * Returns the first disallowed function name found, or null if all are ok.
 */
function findDisallowedFunction(ast: SelectStatement, allowed: Set<string>): string | null {
  // Check columns
  for (const col of ast.columns) {
    if (col.expr.kind === "expr" && col.expr.expr) {
      const bad = checkWhereValue(col.expr.expr, allowed);
      if (bad) return bad;
    }
  }

  // Check DISTINCT ON columns
  if (ast.distinct && ast.distinct.type === "distinct_on") {
    for (const val of ast.distinct.columns) {
      const bad = checkWhereValue(val, allowed);
      if (bad) return bad;
    }
  }

  // Check JOIN conditions
  for (const join of ast.joins) {
    if (join.condition && join.condition.type === "join_on") {
      const bad = checkWhereExpr(join.condition.expr, allowed);
      if (bad) return bad;
    }
  }

  // Check WHERE
  if (ast.where) {
    const bad = checkWhereExpr(ast.where.inner, allowed);
    if (bad) return bad;
  }

  // Check GROUP BY
  if (ast.groupBy) {
    for (const item of ast.groupBy.items) {
      const bad = checkWhereValue(item, allowed);
      if (bad) return bad;
    }
  }

  // Check HAVING
  if (ast.having) {
    const bad = checkWhereExpr(ast.having.expr, allowed);
    if (bad) return bad;
  }

  // Check ORDER BY
  if (ast.orderBy) {
    for (const item of ast.orderBy.items) {
      const bad = checkWhereValue(item.expr, allowed);
      if (bad) return bad;
    }
  }

  return null;
}

function checkFuncCall(func: FuncCall, allowed: Set<string>): string | null {
  if (!allowed.has(func.name.toLowerCase())) {
    return func.name;
  }
  // Also check arguments for nested function calls
  if (func.args.kind === "args") {
    for (const arg of func.args.args) {
      const bad = checkWhereValue(arg, allowed);
      if (bad) return bad;
    }
  }
  return null;
}

function checkWhereValue(val: WhereValue, allowed: Set<string>): string | null {
  switch (val.type) {
    case "where_value":
      if (val.kind === "func_call") {
        return checkFuncCall(val.func, allowed);
      }
      return null;

    case "where_arith":
    case "where_jsonb_op":
    case "where_pgvector_op": {
      const l = checkWhereValue(val.left, allowed);
      if (l) return l;
      return checkWhereValue(val.right, allowed);
    }

    case "where_unary_minus":
      return checkWhereValue(val.expr, allowed);

    case "case_expr": {
      if (val.subject) {
        const s = checkWhereValue(val.subject, allowed);
        if (s) return s;
      }
      for (const w of val.whens) {
        const c = checkWhereValue(w.condition, allowed);
        if (c) return c;
        const r = checkWhereValue(w.result, allowed);
        if (r) return r;
      }
      if (val.else) {
        return checkWhereValue(val.else, allowed);
      }
      return null;
    }

    case "cast_expr":
      return checkWhereValue(val.expr, allowed);

    default:
      return null;
  }
}

function checkWhereExpr(expr: WhereExpr, allowed: Set<string>): string | null {
  switch (expr.type) {
    case "where_and":
    case "where_or": {
      const l = checkWhereExpr(expr.left, allowed);
      if (l) return l;
      return checkWhereExpr(expr.right, allowed);
    }

    case "where_not":
      return checkWhereExpr(expr.expr, allowed);

    case "where_comparison": {
      const l = checkWhereValue(expr.left, allowed);
      if (l) return l;
      return checkWhereValue(expr.right, allowed);
    }

    case "where_is_null":
      return checkWhereValue(expr.expr, allowed);

    case "where_is_bool":
      return checkWhereValue(expr.expr, allowed);

    case "where_between": {
      const e = checkWhereValue(expr.expr, allowed);
      if (e) return e;
      const lo = checkWhereValue(expr.low, allowed);
      if (lo) return lo;
      return checkWhereValue(expr.high, allowed);
    }

    case "where_in": {
      const e = checkWhereValue(expr.expr, allowed);
      if (e) return e;
      for (const item of expr.list) {
        const bad = checkWhereValue(item, allowed);
        if (bad) return bad;
      }
      return null;
    }

    case "where_like": {
      const e = checkWhereValue(expr.expr, allowed);
      if (e) return e;
      return checkWhereValue(expr.pattern, allowed);
    }

    case "where_ts_match": {
      const l = checkWhereValue(expr.left, allowed);
      if (l) return l;
      return checkWhereValue(expr.right, allowed);
    }

    default:
      return null;
  }
}
