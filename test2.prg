//#include "harbour_duckdb.h"
#include "duckdb.ch"

PROCEDURE Main()
   LOCAL db, con, stmt1, stmt2, result
   LOCAL i, j, nCols, nRows
   LOCAL aDate, aTime, aTimestamp
   LOCAL nYear, nMonth, nDay
   LOCAL nHour, nMin, nSec, nMicros
   LOCAL nMonths, nDays, nMicros2
   LOCAL aProgress

   HB_CDPSELECT("UTF8")
   // 開啟資料庫
   db := DUCKDB_OPEN( "test2.db" )
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
   result := DUCKDB_QUERY( con, "CREATE TABLE IF NOT EXISTS employees ( " + ;
      "id INTEGER PRIMARY KEY, " + ;
      "name VARCHAR, " + ;
      "department VARCHAR, " + ;
      "salary DOUBLE, " + ;
      "hire_date DATE, " + ;
      "is_active BOOLEAN " + ;
      ")" )

   IF result == NIL
      ? "無法建立表格"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   DUCKDB_FREE( result )

   // 使用預備語句插入資料
   stmt1 := DUCKDB_PREPARE( con, "INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?)" )
   IF stmt1 == NIL
      ? "無法準備語句"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   // 插入多筆資料
   FOR i := 1 TO 10
      DUCKDB_BIND_INT32( stmt1, 1, i )
      DUCKDB_BIND_VARCHAR( stmt1, 2, "Employee " + hb_ntos( i ) )
      DUCKDB_BIND_VARCHAR( stmt1, 3, iif( i % 2 == 0, "IT", "HR" ) )
      DUCKDB_BIND_DOUBLE( stmt1, 4, 50000 + ( i * 1000 ) )
      DUCKDB_BIND_DATE( stmt1, 5, DUCKDB_TO_DATE( 2020 + ( i % 3 ), 1 + ( i % 12 ), 1 + ( i % 28 ) ) )
      DUCKDB_BIND_BOOLEAN( stmt1, 6, i % 2 == 0 )

      result := DUCKDB_EXECUTE( stmt1 )
      IF result == NIL
         ? "無法執行語句"
         DUCKDB_DISCONNECT( con )
         DUCKDB_CLOSE( db )
         RETURN
      ENDIF

      DUCKDB_FREE( result )
   NEXT

   // 釋放第一個 stmt
   IF stmt1 != NIL
      DUCKDB_FREE_STMT( stmt1 )
      stmt1 := NIL
   ENDIF

   // 查詢各部門平均薪資
   result := DUCKDB_QUERY( con, "SELECT department, AVG(salary) as avg_salary " + ;
      "FROM employees GROUP BY department ORDER BY avg_salary DESC" )

   IF result == NIL
      ? "無法查詢資料"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   ? "各部門平均薪資:"
   nCols := DUCKDB_COLUMN_COUNT( result )
   nRows := DUCKDB_ROW_COUNT( result )

   FOR i := 0 TO nRows - 1
      FOR j := 0 TO nCols - 1
         IF j == 0
            ?? DUCKDB_VALUE_VARCHAR( result, j, i ), ": "
         ELSE
            ?? DUCKDB_VALUE_DOUBLE( result, j, i )
         ENDIF
      NEXT
      ?
   NEXT

   DUCKDB_FREE( result )

   // 查詢最近入職的員工
   result := DUCKDB_QUERY( con, "SELECT name, department, hire_date " + ;
      "FROM employees ORDER BY hire_date DESC LIMIT 3" )

   IF result == NIL
      ? "無法查詢資料"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   ? "最近入職的員工:"
   nCols := DUCKDB_COLUMN_COUNT( result )
   nRows := DUCKDB_ROW_COUNT( result )

   FOR i := 0 TO nRows - 1
      FOR j := 0 TO nCols - 1
         IF j == 2
            aDate := DUCKDB_FROM_DATE( DUCKDB_VALUE_DATE( result, j, i ) )
            ?? aDate[1], "-", aDate[2], "-", aDate[3]
         ELSE
            ?? DUCKDB_VALUE_VARCHAR( result, j, i ), " "
         ENDIF
      NEXT
      ?
   NEXT

   DUCKDB_FREE( result )

   // 查詢薪資範圍
   stmt2 := DUCKDB_PREPARE( con, "SELECT name, salary FROM employees " + ;
      "WHERE salary BETWEEN ? AND ? ORDER BY salary" )

   IF stmt2 == NIL
      ? "無法準備語句"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   DUCKDB_BIND_DOUBLE( stmt2, 1, 55000 )
   DUCKDB_BIND_DOUBLE( stmt2, 2, 65000 )

   result := DUCKDB_EXECUTE( stmt2 )
   IF result == NIL
      ? "無法執行語句"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   ? "薪資在 55000-65000 之間的員工:"
   nCols := DUCKDB_COLUMN_COUNT( result )
   nRows := DUCKDB_ROW_COUNT( result )

   FOR i := 0 TO nRows - 1
      FOR j := 0 TO nCols - 1
         IF j == 1
            ?? DUCKDB_VALUE_DOUBLE( result, j, i )
         ELSE
            ?? DUCKDB_VALUE_VARCHAR( result, j, i ), " "
         ENDIF
      NEXT
      ?
   NEXT

   DUCKDB_FREE( result )

   // 釋放資源
   IF stmt2 != NIL
      DUCKDB_FREE_STMT( stmt2 )
   ENDIF
   DUCKDB_DISCONNECT( con )
   DUCKDB_CLOSE( db )

RETURN 