#include "duckdb.ch"

PROCEDURE Main()
   LOCAL db, con, stmt, result
   LOCAL i

   HB_CDPSELECT("UTF8")

   // 開啟資料庫
   db := DUCKDB_OPEN("test.db")
   IF db == NIL
      ? "無法開啟資料庫"
      RETURN
   ENDIF

   // 建立連線
   con := DUCKDB_CONNECT(db)
   IF con == NIL
      ? "無法建立連線"
      DUCKDB_CLOSE(db)
      RETURN
   ENDIF

   // 建立測試表格
   result := DUCKDB_QUERY(con, "CREATE TABLE test_data ( " + ;
      "id INTEGER PRIMARY KEY, " + ;
      "name VARCHAR, " + ;
      "value INTEGER" + ;
      ")" )

   IF result == NIL
      ? "無法建立表格"
      DUCKDB_DISCONNECT(con)
      DUCKDB_CLOSE(db)
      RETURN
   ENDIF

   DUCKDB_FREE(result)
   
   // 清空表格
   result := DUCKDB_QUERY(con, "DELETE FROM test_data")
   IF result != NIL
      DUCKDB_FREE(result)
   ENDIF

   // 準備插入語句
   stmt := DUCKDB_PREPARE(con, "INSERT INTO test_data VALUES (?, ?, ?)")
   ? "stmt =", stmt
   IF stmt == NIL
      ? "無法準備語句"
      DUCKDB_DISCONNECT(con)
      DUCKDB_CLOSE(db)
      RETURN
   ENDIF

   // 執行插入測試
   ? "執行插入測試..."
   
   // 第一次插入
   ? "第一次插入綁定參數..."
   ? "DUCKDB_BIND_INT32(stmt, 1, 1) =", DUCKDB_BIND_INT32(stmt, 1, 1)
   ? "DUCKDB_BIND_VARCHAR(stmt, 2, 'Test 1') =", DUCKDB_BIND_VARCHAR(stmt, 2, "Test 1")
   ? "DUCKDB_BIND_INT32(stmt, 3, 100) =", DUCKDB_BIND_INT32(stmt, 3, 100)
   
   result := DUCKDB_EXECUTE(stmt)
   IF result != NIL
      ? "第一次插入成功"
      DUCKDB_FREE(result)
   ELSE
      ? "第一次插入失敗"
      DUCKDB_DISCONNECT(con)
      DUCKDB_CLOSE(db)
      RETURN
   ENDIF

   // 第二次插入
   ? "第二次插入綁定參數..."
   ? "DUCKDB_BIND_INT32(stmt, 1, 2) =", DUCKDB_BIND_INT32(stmt, 1, 2)
   ? "DUCKDB_BIND_VARCHAR(stmt, 2, 'Test 2') =", DUCKDB_BIND_VARCHAR(stmt, 2, "Test 2")
   ? "DUCKDB_BIND_INT32(stmt, 3, 200) =", DUCKDB_BIND_INT32(stmt, 3, 200)
   
   result := DUCKDB_EXECUTE(stmt)
   IF result != NIL
      ? "第二次插入成功"
      DUCKDB_FREE(result)
   ELSE
      ? "第二次插入失敗"
      DUCKDB_DISCONNECT(con)
      DUCKDB_CLOSE(db)
      RETURN
   ENDIF

   // 查詢資料
   result := DUCKDB_QUERY(con, "SELECT * FROM test_data")
   IF result != NIL
      ? "資料表內容:"
      ? "ID  Name    Value"
      ? "----------------"
      FOR i := 0 TO DUCKDB_ROW_COUNT(result) - 1
         ? DUCKDB_VALUE_INT64(result, 0, i), ;
           DUCKDB_VALUE_VARCHAR(result, 1, i), ;
           DUCKDB_VALUE_INT32(result, 2, i)
      NEXT
      DUCKDB_FREE(result)
   ENDIF

   // 釋放資源
   DUCKDB_DISCONNECT(con)
   DUCKDB_CLOSE(db)

RETURN 