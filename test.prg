//#include "hbtypes.ch"
#include "duckdb.ch"

PROCEDURE Main()
   LOCAL db, con, stmt, result
   LOCAL i, j, nCols, nRows
   LOCAL aDate, aTime, aTimestamp
   LOCAL nYear, nMonth, nDay
   LOCAL nHour, nMin, nSec, nMicros
   LOCAL nMonths, nDays, nMicros2

   HB_CDPSELECT("UTF8")

   // 開啟資料庫
   db := DUCKDB_OPEN( "test.db" )
   IF db == NIL
      ? "無法開啟資料庫"
      RETURN
   ENDIF

   // 建立連線
   con := DUCKDB_CONNECT( db )
   IF con == NIL
      ? "無法建立連線"
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   // 建立測試表格
   result := DUCKDB_QUERY( con, "CREATE TABLE test ( " + ;
      "id INTEGER, " + ;
      "name VARCHAR, " + ;
      "value DOUBLE, " + ;
      "active BOOLEAN, " + ;
      "birth_date DATE, " + ;
      "work_time TIME, " + ;
      "created_at TIMESTAMP, " + ;
      "duration INTERVAL, " + ;
      "data BLOB " + ;
      ")" )

   IF result == NIL
      ? "無法建立表格"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   DUCKDB_FREE( result )

   // 使用預備語句插入資料
   stmt := DUCKDB_PREPARE( con, "INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" )
   IF stmt == NIL
      ? "無法準備語句"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   // 綁定參數
   DUCKDB_BIND_INT32( stmt, 1, 1 )
   DUCKDB_BIND_VARCHAR( stmt, 2, "John Doe" )
   DUCKDB_BIND_DOUBLE( stmt, 3, 123.45 )
   DUCKDB_BIND_BOOLEAN( stmt, 4, .T. )
   DUCKDB_BIND_DATE( stmt, 5, DUCKDB_TO_DATE( 1990, 1, 1 ) )
   DUCKDB_BIND_TIME( stmt, 6, DUCKDB_TO_TIME( 9, 0, 0, 0 ) )
   DUCKDB_BIND_TIMESTAMP( stmt, 7, DUCKDB_TO_TIMESTAMP( 2024, 1, 1, 12, 0, 0, 0 ) )
   DUCKDB_BIND_INTERVAL( stmt, 8, 1, 2, 3600000000 ) // 1個月2天1小時
   DUCKDB_BIND_BLOB( stmt, 9, "Test Data" )

   // 執行預備語句
   result := DUCKDB_EXECUTE( stmt )
   IF result == NIL
      ? "無法執行語句"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   DUCKDB_FREE( result )

   // 查詢資料
   result := DUCKDB_QUERY( con, "SELECT * FROM test" )
   IF result == NIL
      ? "無法查詢資料"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   // 顯示欄位名稱
   nCols := DUCKDB_COLUMN_COUNT( result )
   FOR i := 0 TO nCols - 1
      ?? DUCKDB_COLUMN_NAME( result, i ), " "
   NEXT
   ?

   // 顯示資料
   nRows := DUCKDB_ROW_COUNT( result )
   FOR i := 0 TO nRows - 1
      FOR j := 0 TO nCols - 1
         IF DUCKDB_VALUE_IS_NULL( result, j, i )
            ?? "NULL "
         ELSE
            DO CASE
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_BOOLEAN
               ?? DUCKDB_VALUE_BOOLEAN( result, j, i ), " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_TINYINT
               ?? DUCKDB_VALUE_INT8( result, j, i ), " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_SMALLINT
               ?? DUCKDB_VALUE_INT16( result, j, i ), " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_INTEGER
               ?? DUCKDB_VALUE_INT32( result, j, i ), " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_BIGINT
               ?? DUCKDB_VALUE_INT64( result, j, i ), " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_FLOAT
               ?? DUCKDB_VALUE_FLOAT( result, j, i ), " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_DOUBLE
               ?? DUCKDB_VALUE_DOUBLE( result, j, i ), " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_VARCHAR
               ?? DUCKDB_VALUE_VARCHAR( result, j, i ), " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_BLOB
               ?? DUCKDB_VALUE_BLOB( result, j, i ), " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_DATE
               aDate := DUCKDB_FROM_DATE( DUCKDB_VALUE_DATE( result, j, i ) )
               ?? aDate[1], "-", aDate[2], "-", aDate[3], " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_TIME
               aTime := DUCKDB_FROM_TIME( DUCKDB_VALUE_TIME( result, j, i ) )
               ?? aTime[1], ":", aTime[2], ":", aTime[3], ".", aTime[4], " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_TIMESTAMP
               aTimestamp := DUCKDB_FROM_TIMESTAMP( DUCKDB_VALUE_TIMESTAMP( result, j, i ) )
               ?? aTimestamp[1], "-", aTimestamp[2], "-", aTimestamp[3], " "
               ?? aTimestamp[4], ":", aTimestamp[5], ":", aTimestamp[6], ".", aTimestamp[7], " "
            CASE DUCKDB_COLUMN_TYPE( result, j ) == DUCKDB_TYPE_INTERVAL
               aInterval := DUCKDB_VALUE_INTERVAL( result, j, i )
               ?? aInterval[1], "個月", aInterval[2], "天", aInterval[3] / 3600000000, "小時 "
            ENDCASE
         ENDIF
      NEXT
      ?
   NEXT

   // 釋放資源
   DUCKDB_FREE( result )
   DUCKDB_DISCONNECT( con )
   DUCKDB_CLOSE( db )

RETURN 