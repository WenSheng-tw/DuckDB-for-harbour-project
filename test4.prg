//#include "harbour_duckdb.h"
#include "duckdb.ch"

PROCEDURE Main()
   LOCAL db, con, stmt, result
   LOCAL cError

   HB_CDPSELECT("UTF8")
   // 測試無效的資料庫路徑
   ? "測試無效的資料庫路徑:"
   db := DUCKDB_OPEN( "/invalid/path/test.db" )
   IF db == NIL
      ? "預期的錯誤: 無法開啟資料庫"
   ENDIF
   ?

   // 開啟資料庫
   db := DUCKDB_OPEN( "test4.db" )
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

   // 測試無效的 SQL 語法
   ? "測試無效的 SQL 語法:"
   result := DUCKDB_QUERY( con, "SELECT * FROM nonexistent_table" )
   IF result == NIL
      ? "預期的錯誤: 無法執行查詢"
   ELSE
      ? "查詢執行成功"
      DUCKDB_FREE( result )
   ENDIF
   ?

   // 測試無效的預備語句
   ? "測試無效的預備語句:"
   stmt := DUCKDB_PREPARE( con, "INSERT INTO nonexistent_table VALUES (?)" )
   IF stmt == NIL
      ? "預期的錯誤: 無法準備語句"
   ELSE
      ? "預備語句準備成功"
   ENDIF
   ?

   // 測試無效的參數綁定
   ? "測試無效的參數綁定:"
   stmt := DUCKDB_PREPARE( con, "SELECT ?" )
   IF stmt != NIL
      // 測試無效的參數索引
      result := DUCKDB_BIND_INT32( stmt, 2, 1 ) // 參數索引超出範圍
      ? "無效參數索引結果:", result
      ?

      // 測試無效的參數類型
      result := DUCKDB_BIND_INT32( stmt, 1, 1 )
      IF result
         result := DUCKDB_EXECUTE( stmt )
         IF result != NIL
            DUCKDB_FREE( result )
         ENDIF
      ENDIF
   ENDIF
   ?

   // 測試中斷查詢
   ? "測試中斷查詢:"
   result := DUCKDB_QUERY( con, "SELECT * FROM (SELECT 1) t1, (SELECT 1) t2, " + ;
      "(SELECT 1) t3, (SELECT 1) t4, (SELECT 1) t5" )
   IF result != NIL
      DUCKDB_FREE( result )
   ENDIF

   DUCKDB_INTERRUPT( con )
   ? "查詢已中斷"
   ?

   // 釋放資源
   DUCKDB_DISCONNECT( con )
   DUCKDB_CLOSE( db )

RETURN 