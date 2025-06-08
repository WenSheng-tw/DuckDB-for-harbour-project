#include "hbapi.h"
#include "hbapiitm.h"
#include "hbapierr.h"
#include "hbstack.h"
#include "hbvm.h"
#include "hbthread.h"
#include "duckdb.h"
#include "duckdb.ch"

#define HB_DUCKDB_DB                       7000001
#define HB_DUCKDB_RESULT                   7000002
#define HB_DUCKDB_STMT                     7000003

/* GC object handlers */
static HB_GARBAGE_FUNC( duckdb_destructor )
{
   void ** ph = ( void ** ) Cargo;

   if( ph && *ph )
   {
      duckdb_close( ( duckdb_database ) *ph );
      *ph = NULL;
   }
}

static const HB_GC_FUNCS s_gcDuckDBFuncs =
{
   duckdb_destructor,
   hb_gcDummyMark
};

static HB_GARBAGE_FUNC( duckdb_result_destructor )
{
   void ** ph = ( void ** ) Cargo;

   if( ph && *ph )
   {
      duckdb_destroy_result( ( duckdb_result * ) *ph );
      *ph = NULL;
   }
}

static const HB_GC_FUNCS s_gcDuckDBResultFuncs =
{
   duckdb_result_destructor,
   hb_gcDummyMark
};

static HB_GARBAGE_FUNC( duckdb_stmt_destructor )
{
   void ** ph = ( void ** ) Cargo;

   if( ph && *ph )
   {
      duckdb_destroy_prepare( ( duckdb_prepared_statement ) *ph );
      *ph = NULL;
   }
}

static const HB_GC_FUNCS s_gcDuckDBStmtFuncs =
{
   duckdb_stmt_destructor,
   hb_gcDummyMark
};

/* Helper functions */
static void hb_duckdb_ret( duckdb_database db )
{
   if( db )
   {
      void ** ph = ( void ** ) hb_gcAllocate( sizeof( duckdb_database ), &s_gcDuckDBFuncs );
      *ph = db;
      hb_retptrGC( ph );
   }
   else
      hb_retptr( NULL );
}

static duckdb_database hb_duckdb_par( int iParam )
{
   void ** ph = ( void ** ) hb_parptrGC( &s_gcDuckDBFuncs, iParam );
   return ph ? ( duckdb_database ) *ph : NULL;
}

static void hb_duckdb_result_ret( duckdb_result * result )
{
   if( result )
   {
      void ** ph = ( void ** ) hb_gcAllocate( sizeof( duckdb_result * ), &s_gcDuckDBResultFuncs );
      *ph = result;
      hb_retptrGC( ph );
   }
   else
      hb_retptr( NULL );
}

static duckdb_result * hb_duckdb_result_par( int iParam )
{
   void ** ph = ( void ** ) hb_parptrGC( &s_gcDuckDBResultFuncs, iParam );
   return ph ? ( duckdb_result * ) *ph : NULL;
}

static void hb_duckdb_stmt_ret( duckdb_prepared_statement stmt )
{
   if( stmt )
   {
      void ** ph = ( void ** ) hb_gcAllocate( sizeof( duckdb_prepared_statement ), &s_gcDuckDBStmtFuncs );
      *ph = stmt;
      hb_retptrGC( ph );
   }
   else
      hb_retptr( NULL );
}

static duckdb_prepared_statement hb_duckdb_stmt_par( int iParam )
{
   void ** ph = ( void ** ) hb_parptrGC( &s_gcDuckDBStmtFuncs, iParam );
   return ph ? ( duckdb_prepared_statement ) *ph : NULL;
}

/* API Functions */
HB_FUNC( DUCKDB_OPEN ) /* duckdb_database duckdb_open(const char *path, duckdb_database *out_database) */
{
   const char * path = hb_parc( 1 );
   duckdb_database db;
   duckdb_state state;

   if( path )
   {
      state = duckdb_open( path, &db );
      if( state == DuckDBSuccess )
      {
         void ** ph = ( void ** ) hb_gcAllocate( sizeof( duckdb_database ), &s_gcDuckDBFuncs );
         *ph = db;
         hb_retptrGC( ph );
      }
      else
      {
         hb_retptr( NULL );
      }
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_QUERY )
{
   duckdb_connection con = hb_duckdb_par( 1 );
   const char * query = hb_parc( 2 );
   duckdb_result * result = ( duckdb_result * ) hb_xgrab( sizeof( duckdb_result ) );
   duckdb_state state;

   if( con && query )
   {
      state = duckdb_query( con, query, result );
      if( state == DuckDBSuccess )
      {
         void ** ph = ( void ** ) hb_gcAllocate( sizeof( duckdb_result * ), &s_gcDuckDBResultFuncs );
         *ph = result;
         hb_retptrGC( ph );
      }
      else
      {
         hb_xfree( result );
         hb_retptr( NULL );
      }
   }
   else
   {
      hb_xfree( result );
      if( !con )
         hb_errRT_BASE( EG_ARG, 2020, "Invalid connection handle", HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
      else
         hb_errRT_BASE( EG_ARG, 2020, "Invalid query string", HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_PREPARE ) /* duckdb_state duckdb_prepare(duckdb_connection connection, const char *query, duckdb_prepared_statement *out_prepared_statement) */
{
   duckdb_connection con = hb_duckdb_par( 1 );
   const char * query = hb_parc( 2 );
   duckdb_prepared_statement stmt;
   duckdb_state state;

   if( con && query )
   {
      state = duckdb_prepare( con, query, &stmt );
      if( state == DuckDBSuccess && stmt )
      {
         hb_duckdb_stmt_ret( stmt );
      }
      else
      {
         hb_retptr( NULL );
      }
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_EXECUTE )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   duckdb_result * result = NULL;
   duckdb_state state;

   if( stmt )
   {
      result = ( duckdb_result * ) hb_xgrab( sizeof( duckdb_result ) );
      if( result )
      {
         state = duckdb_execute_prepared( stmt, result );
         if( state == DuckDBSuccess )
         {
            void ** ph = ( void ** ) hb_gcAllocate( sizeof( duckdb_result * ), &s_gcDuckDBResultFuncs );
            if( ph )
            {
               *ph = result;
               hb_retptrGC( ph );
            }
            else
            {
               duckdb_destroy_result( result );
               hb_xfree( result );
               hb_retptr( NULL );
            }
         }
         else
         {
            hb_xfree( result );
            hb_retptr( NULL );
         }
      }
      else
      {
         hb_retptr( NULL );
      }
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_RESULT_COLUMN_COUNT ) /* idx_t duckdb_column_count(duckdb_result *result) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );

   if( result )
   {
      hb_retnint( duckdb_column_count( result ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_RESULT_ROW_COUNT ) /* idx_t duckdb_row_count(duckdb_result *result) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );

   if( result )
   {
      hb_retnint( duckdb_row_count( result ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_RESULT_COLUMN_NAME ) /* const char *duckdb_column_name(duckdb_result *result, idx_t col) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );

   if( result )
   {
      hb_retc( duckdb_column_name( result, col ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_COLUMN_TYPE ) /* duckdb_type duckdb_column_type(duckdb_result *result, idx_t col) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );

   if( result )
   {
      hb_retni( duckdb_column_type( result, col ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_BOOLEAN ) /* bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      hb_retl( duckdb_value_boolean( result, col, row ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_INT8 ) /* int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      hb_retni( duckdb_value_int8( result, col, row ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_INT16 ) /* int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      hb_retni( duckdb_value_int16( result, col, row ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_INT32 ) /* int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      hb_retnl( duckdb_value_int32( result, col, row ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_INT64 ) /* int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      hb_retnint( duckdb_value_int64( result, col, row ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_FLOAT ) /* float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      hb_retnd( duckdb_value_float( result, col, row ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_DOUBLE ) /* double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      hb_retnd( duckdb_value_double( result, col, row ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_VARCHAR ) /* const char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      hb_retc( duckdb_value_varchar( result, col, row ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_BLOB ) /* duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      duckdb_blob blob = duckdb_value_blob( result, col, row );
      hb_retclen( blob.data, blob.size );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_IS_NULL ) /* bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row) */
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   idx_t col = ( idx_t ) hb_parnint( 2 );
   idx_t row = ( idx_t ) hb_parnint( 3 );

   if( result )
   {
      hb_retl( duckdb_value_is_null( result, col, row ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

/* 新增的函數 */
HB_FUNC( DUCKDB_CONNECT )
{
   duckdb_database db = hb_duckdb_par( 1 );
   duckdb_connection con;
   duckdb_state state;

   if( db )
   {
      state = duckdb_connect( db, &con );
      if( state == DuckDBSuccess )
      {
         void ** ph = ( void ** ) hb_gcAllocate( sizeof( duckdb_connection ), &s_gcDuckDBFuncs );
         *ph = con;
         hb_retptrGC( ph );
      }
      else
      {
         hb_retptr( NULL );
      }
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_CLOSE )
{
   duckdb_database db = hb_duckdb_par( 1 );

   if( db )
   {
      duckdb_close( db );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_DISCONNECT )
{
   duckdb_connection con = hb_duckdb_par( 1 );

   if( con )
   {
      duckdb_disconnect( con );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_FREE )
{
   duckdb_result * result = hb_duckdb_result_par( 1 );

   if( result )
   {
      duckdb_destroy_result( result );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_retl( HB_FALSE );
   }
}

HB_FUNC( DUCKDB_FREE_STMT )
{
   void ** ph = ( void ** ) hb_parptrGC( &s_gcDuckDBStmtFuncs, 1 );

   if( ph && *ph )
   {
      duckdb_prepared_statement stmt = ( duckdb_prepared_statement ) *ph;
      duckdb_destroy_prepare( &stmt );
      *ph = NULL;
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_retl( HB_FALSE );
   }
}

HB_FUNC( DUCKDB_BIND_INT32 )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   int param_idx = hb_parni( 2 );
   int32_t value = hb_parni( 3 );

   if( stmt )
   {
      duckdb_bind_int32( stmt, param_idx, value );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_BIND_VARCHAR )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   int param_idx = hb_parni( 2 );
   const char * value = hb_parc( 3 );

   if( stmt && value )
   {
      duckdb_bind_varchar( stmt, param_idx, value );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_BIND_DOUBLE )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   int param_idx = hb_parni( 2 );
   double value = hb_parnd( 3 );

   if( stmt )
   {
      duckdb_bind_double( stmt, param_idx, value );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_BIND_BOOLEAN )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   int param_idx = hb_parni( 2 );
   bool value = hb_parl( 3 );

   if( stmt )
   {
      duckdb_bind_boolean( stmt, param_idx, value );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_TO_DATE )
{
   int year = hb_parni( 1 );
   int month = hb_parni( 2 );
   int day = hb_parni( 3 );
   duckdb_date_struct date = { year, month, day };
   duckdb_date result = duckdb_to_date( date );
   hb_retnint( result.days );
}

HB_FUNC( DUCKDB_BIND_DATE )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   int param_idx = hb_parni( 2 );
   duckdb_date date;
   date.days = hb_parnint( 3 );

   if( stmt )
   {
      duckdb_bind_date( stmt, param_idx, date );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_TO_TIME )
{
   int hour = hb_parni( 1 );
   int min = hb_parni( 2 );
   int sec = hb_parni( 3 );
   int micros = hb_parni( 4 );
   duckdb_time_struct time = { hour, min, sec, micros };
   duckdb_time result = duckdb_to_time( time );
   hb_retnint( result.micros );
}

HB_FUNC( DUCKDB_BIND_TIME )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   int param_idx = hb_parni( 2 );
   duckdb_time time;
   time.micros = hb_parnint( 3 );

   if( stmt )
   {
      duckdb_bind_time( stmt, param_idx, time );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_TO_TIMESTAMP )
{
   int year = hb_parni( 1 );
   int month = hb_parni( 2 );
   int day = hb_parni( 3 );
   int hour = hb_parni( 4 );
   int min = hb_parni( 5 );
   int sec = hb_parni( 6 );
   int micros = hb_parni( 7 );
   duckdb_timestamp_struct ts = { year, month, day, hour, min, sec, micros };
   duckdb_timestamp result = duckdb_to_timestamp( ts );
   hb_retnint( result.micros );
}

HB_FUNC( DUCKDB_BIND_TIMESTAMP )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   int param_idx = hb_parni( 2 );
   duckdb_timestamp ts;
   ts.micros = hb_parnint( 3 );

   if( stmt )
   {
      duckdb_bind_timestamp( stmt, param_idx, ts );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_BIND_INTERVAL )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   int param_idx = hb_parni( 2 );
   int months = hb_parni( 3 );
   int days = hb_parni( 4 );
   int64_t micros = hb_parnint( 5 );
   duckdb_interval interval = { months, days, micros };

   if( stmt )
   {
      duckdb_bind_interval( stmt, param_idx, interval );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_BIND_BLOB )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   int param_idx = hb_parni( 2 );
   const void * data = hb_parc( 3 );
   int64_t size = hb_parclen( 3 );

   if( stmt && data )
   {
      duckdb_bind_blob( stmt, param_idx, data, size );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_COLUMN_COUNT )
{
   duckdb_result * result = hb_duckdb_result_par( 1 );

   if( result )
   {
      hb_retnint( duckdb_column_count( result ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_COLUMN_NAME )
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   int col = hb_parni( 2 );

   if( result )
   {
      hb_retc( duckdb_column_name( result, col ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_ROW_COUNT )
{
   duckdb_result * result = hb_duckdb_result_par( 1 );

   if( result )
   {
      hb_retnint( duckdb_row_count( result ) );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_FROM_DATE )
{
   duckdb_date date;
   date.days = hb_parnint( 1 );
   duckdb_date_struct date_struct = duckdb_from_date( date );
   
   PHB_ITEM pArray = hb_itemArrayNew( 3 );
   hb_arraySetNI( pArray, 1, date_struct.year );
   hb_arraySetNI( pArray, 2, date_struct.month );
   hb_arraySetNI( pArray, 3, date_struct.day );
   hb_itemReturn( pArray );
   hb_itemRelease( pArray );
}

HB_FUNC( DUCKDB_VALUE_DATE )
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   int col = hb_parni( 2 );
   int row = hb_parni( 3 );

   if( result )
   {
      duckdb_date date = duckdb_value_date( result, col, row );
      hb_retnint( date.days );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_FROM_TIME )
{
   duckdb_time time;
   time.micros = hb_parnint( 1 );
   int hour, min, sec, micros;
   duckdb_from_time( time, &hour, &min, &sec, &micros );
   
   PHB_ITEM pArray = hb_itemArrayNew( 4 );
   hb_arraySetNI( pArray, 1, hour );
   hb_arraySetNI( pArray, 2, min );
   hb_arraySetNI( pArray, 3, sec );
   hb_arraySetNI( pArray, 4, micros );
   hb_itemReturn( pArray );
   hb_itemRelease( pArray );
}

HB_FUNC( DUCKDB_VALUE_TIME )
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   int col = hb_parni( 2 );
   int row = hb_parni( 3 );

   if( result )
   {
      duckdb_time time = duckdb_value_time( result, col, row );
      hb_retnint( time.micros );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_FROM_TIMESTAMP )
{
   duckdb_timestamp ts;
   ts.micros = hb_parnint( 1 );
   int year, month, day, hour, min, sec, micros;
   duckdb_from_timestamp( ts, &year, &month, &day, &hour, &min, &sec, &micros );
   
   PHB_ITEM pArray = hb_itemArrayNew( 7 );
   hb_arraySetNI( pArray, 1, year );
   hb_arraySetNI( pArray, 2, month );
   hb_arraySetNI( pArray, 3, day );
   hb_arraySetNI( pArray, 4, hour );
   hb_arraySetNI( pArray, 5, min );
   hb_arraySetNI( pArray, 6, sec );
   hb_arraySetNI( pArray, 7, micros );
   hb_itemReturn( pArray );
   hb_itemRelease( pArray );
}

HB_FUNC( DUCKDB_VALUE_TIMESTAMP )
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   int col = hb_parni( 2 );
   int row = hb_parni( 3 );

   if( result )
   {
      duckdb_timestamp ts = duckdb_value_timestamp( result, col, row );
      hb_retnint( ts.micros );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_VALUE_INTERVAL )
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   int col = hb_parni( 2 );
   int row = hb_parni( 3 );

   if( result )
   {
      duckdb_interval interval = duckdb_value_interval( result, col, row );
      PHB_ITEM pArray = hb_itemArrayNew( 3 );
      hb_arraySetNI( pArray, 1, interval.months );
      hb_arraySetNI( pArray, 2, interval.days );
      hb_arraySetNI( pArray, 3, interval.micros );
      hb_itemReturn( pArray );
      hb_itemRelease( pArray );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_OPEN_EXT )
{
   duckdb_database db;
   const char *path = hb_parc( 1 );
   const char *config_str = hb_parc( 2 );
   duckdb_config config;
   char *error = NULL;
   duckdb_state state;

   if( !path || !config_str )
   {
      hb_retptr( NULL );
      return;
   }

   // 創建配置
   state = duckdb_create_config( &config );
   if( state != DuckDBSuccess )
   {
      hb_retptr( NULL );
      return;
   }

   // 設置配置
   state = duckdb_set_config( config, "threads", config_str );
   if( state != DuckDBSuccess )
   {
      duckdb_destroy_config( &config );
      hb_retptr( NULL );
      return;
   }

   // 打開資料庫
   state = duckdb_open_ext( path, &db, config, &error );
   if( state == DuckDBSuccess && db )
   {
      void ** ph = ( void ** ) hb_gcAllocate( sizeof( duckdb_database ), &s_gcDuckDBFuncs );
      if( ph )
      {
         *ph = db;
         hb_retptrGC( ph );
      }
      else
      {
         duckdb_close( db );
         hb_retptr( NULL );
      }
   }
   else
   {
      if( error )
      {
         hb_errRT_BASE( EG_ARG, 2020, error, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
         duckdb_free( error );
      }
      hb_retptr( NULL );
   }

   // 清理配置
   duckdb_destroy_config( &config );
}

HB_FUNC( DUCKDB_QUERY_PROGRESS )
{
   duckdb_connection con = hb_duckdb_par( 1 );
   PHB_ITEM pArray;

   if( !con )
   {
      hb_errRT_BASE( EG_ARG, 2020, "Invalid connection handle", HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
      return;
   }

   duckdb_query_progress_type progress = duckdb_query_progress( con );

   pArray = hb_itemArrayNew( 3 );
   hb_arraySetND( pArray, 1, progress.percentage );
   hb_arraySetNInt( pArray, 2, progress.rows_processed );
   hb_arraySetNInt( pArray, 3, progress.total_rows_to_process );
   hb_itemReturn( pArray );
   hb_itemRelease( pArray );
}

HB_FUNC( DUCKDB_GET_ERROR )
{
   duckdb_result * result = hb_duckdb_result_par( 1 );
   const char * error;

   if( result )
   {
      error = duckdb_result_error( result );
      if( error )
      {
         hb_retc( error );
      }
      else
      {
         hb_retc( "" );
      }
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_PREPARE_ERROR )
{
   duckdb_prepared_statement stmt = hb_duckdb_stmt_par( 1 );
   const char * error;

   if( stmt )
   {
      error = duckdb_prepare_error( stmt );
      if( error )
      {
         hb_retc( error );
      }
      else
      {
         hb_retc( "" );
      }
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

HB_FUNC( DUCKDB_INTERRUPT )
{
   duckdb_connection con = hb_duckdb_par( 1 );

   if( con )
   {
      duckdb_interrupt( con );
      hb_retl( HB_TRUE );
   }
   else
   {
      hb_errRT_BASE( EG_ARG, 2020, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS );
   }
}

// ... existing code ... 