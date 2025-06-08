#ifndef __DUCKDB_CH
#define __DUCKDB_CH

/* DuckDB type definitions */
#define DUCKDB_TYPE_INVALID     0
#define DUCKDB_TYPE_BOOLEAN     1
#define DUCKDB_TYPE_TINYINT     2
#define DUCKDB_TYPE_SMALLINT    3
#define DUCKDB_TYPE_INTEGER     4
#define DUCKDB_TYPE_BIGINT      5
#define DUCKDB_TYPE_FLOAT       6
#define DUCKDB_TYPE_DOUBLE      7
#define DUCKDB_TYPE_VARCHAR     8
#define DUCKDB_TYPE_BLOB        9
#define DUCKDB_TYPE_NULL        10
#define DUCKDB_TYPE_DATE        11
#define DUCKDB_TYPE_TIME        12
#define DUCKDB_TYPE_TIMESTAMP   13
#define DUCKDB_TYPE_INTERVAL    14

/* DuckDB state definitions */
#define DUCKDB_SUCCESS          0
#define DUCKDB_ERROR            1

/* DuckDB memory management */
#define HB_DUCKDB_DB           7000001
#define HB_DUCKDB_RESULT       7000002
#define HB_DUCKDB_STMT         7000003
#define HB_DUCKDB_APPENDER     7000004
#define HB_DUCKDB_CONFIG       7000005

#endif /* __DUCKDB_CH */ 