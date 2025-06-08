//#include "harbour_duckdb.h"
#include "duckdb.ch"
#include "hbthread.ch"

#define N_THREADS 4
#define N_BATCH_SIZE 1000
#define N_TOTAL_RECORDS 100000

STATIC s_hMutex
STATIC s_nCompleted := 0
STATIC s_nTotalBatches

PROCEDURE Main()
   LOCAL db, con, stmt, result
   LOCAL i, j, nCols, nRows
   LOCAL aProgress
   LOCAL nStart, nEnd
   LOCAL aThreads := {}
   LOCAL aDate
   LOCAL cValues := ""


   HB_CDPSELECT("UTF8")
   
   // 檢查多執行緒支援
   IF !hb_mtvm()
      ? "警告：此 Harbour 版本不支援多執行緒"
      ? "將使用單執行緒模式執行"
      RETURN
   ENDIF

   // 開啟資料庫
   db := DUCKDB_OPEN_EXT( "test5.db", "4" )
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
   result := DUCKDB_QUERY( con, "CREATE TABLE large_data ( " + ;
      "id INTEGER PRIMARY KEY, " + ;
      "value1 INTEGER, " + ;
      "value2 DOUBLE, " + ;
      "value3 VARCHAR, " + ;
      "value4 BOOLEAN, " + ;
      "value5 DATE " + ;
      ")" )

   IF result == NIL
      ? "無法建立表格"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF

   DUCKDB_FREE( result )

   // 開始事務
   result := DUCKDB_QUERY( con, "BEGIN TRANSACTION" )
   IF result == NIL
      ? "無法開始事務"
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF
   DUCKDB_FREE( result )

   nStart := seconds()
   ? "開始插入資料..."

   // 初始化多執行緒相關變數
   s_hMutex := hb_mutexCreate()
   s_nCompleted := 0
   s_nTotalBatches := N_TOTAL_RECORDS / N_BATCH_SIZE

   // 執行批量插入
   FOR i := 1 TO 100
      cValues := ""
      FOR j := 1 TO 1000
         IF j > 1
            cValues += ","
         ENDIF
         cValues += "(" + hb_ntos((i-1)*1000+j) + "," + ;
                   hb_ntos((i-1)*1000+j*2) + "," + ;
                   hb_ntos((i-1)*1000+j*1.5) + ",'" + ;
                   "Value " + hb_ntos((i-1)*1000+j) + "'," + ;
                   iif((i-1)*1000+j % 2 == 0, "true", "false") + "," + ;
                   "'2024-" + hb_ntos(1+((i-1)*1000+j)%12) + "-" + hb_ntos(1+((i-1)*1000+j)%28) + "'" + ;
                   ")"
      NEXT

      result := DUCKDB_QUERY( con, "INSERT INTO large_data VALUES " + cValues )
      IF result == NIL
         ? "無法執行批量插入"
         DUCKDB_QUERY( con, "ROLLBACK" )
         DUCKDB_DISCONNECT( con )
         DUCKDB_CLOSE( db )
         RETURN
      ENDIF

      DUCKDB_FREE( result )

      // 顯示進度
      aProgress := DUCKDB_QUERY_PROGRESS( con )
      ? "進度:", aProgress[1] * 100, "%"
   NEXT

   // 提交事務
   result := DUCKDB_QUERY( con, "COMMIT" )
   IF result == NIL
      ? "無法提交事務"
      DUCKDB_QUERY( con, "ROLLBACK" )
      DUCKDB_DISCONNECT( con )
      DUCKDB_CLOSE( db )
      RETURN
   ENDIF
   DUCKDB_FREE( result )

   nEnd := seconds()
   ? "插入完成，耗時:", nEnd - nStart, "秒"
   ?

   // 測試大量資料查詢
   ? "開始查詢測試..."
   nStart := seconds()

   // 測試 1: 簡單查詢
   result := DUCKDB_QUERY( con, "SELECT COUNT(*) FROM large_data" )
   IF result != NIL
      ? "總筆數:", DUCKDB_VALUE_INT64( result, 0, 0 )
      DUCKDB_FREE( result )
   ENDIF

   // 測試 2: 條件查詢
   result := DUCKDB_QUERY( con, "SELECT COUNT(*) FROM large_data WHERE value1 > 50000" )
   IF result != NIL
      ? "value1 > 50000 的筆數:", DUCKDB_VALUE_INT64( result, 0, 0 )
      DUCKDB_FREE( result )
   ENDIF

   // 測試 3: 分組查詢
   result := DUCKDB_QUERY( con, "SELECT value4, COUNT(*) as count " + ;
      "FROM large_data GROUP BY value4 ORDER BY count DESC" )
   IF result != NIL
      ? "依 value4 分組統計:"
      nCols := DUCKDB_COLUMN_COUNT( result )
      nRows := DUCKDB_ROW_COUNT( result )
      FOR i := 0 TO nRows - 1
         FOR j := 0 TO nCols - 1
            IF j == 0
               ?? DUCKDB_VALUE_BOOLEAN( result, j, i ), ": "
            ELSE
               ?? DUCKDB_VALUE_INT64( result, j, i )
            ENDIF
         NEXT
         ?
      NEXT
      DUCKDB_FREE( result )
   ENDIF

   // 測試 4: 複雜查詢
   result := DUCKDB_QUERY( con, "SELECT " + ;
      "value5, " + ;
      "COUNT(*) as count, " + ;
      "AVG(value2) as avg_value2, " + ;
      "MIN(value1) as min_value1, " + ;
      "MAX(value1) as max_value1 " + ;
      "FROM large_data " + ;
      "GROUP BY value5 " + ;
      "ORDER BY count DESC " + ;
      "LIMIT 5" )

   IF result != NIL
      ? "依日期分組統計 (前 5 筆):"
      nCols := DUCKDB_COLUMN_COUNT( result )
      nRows := DUCKDB_ROW_COUNT( result )
      FOR i := 0 TO nRows - 1
         FOR j := 0 TO nCols - 1
            IF j == 0
               aDate := DUCKDB_FROM_DATE( DUCKDB_VALUE_DATE( result, j, i ) )
               ?? aDate[1], "-", aDate[2], "-", aDate[3], ": "
            ELSEIF j == 1
               ?? DUCKDB_VALUE_INT64( result, j, i ), "筆, "
            ELSEIF j == 2
               ?? "平均 value2:", DUCKDB_VALUE_DOUBLE( result, j, i ), ", "
            ELSEIF j == 3
               ?? "最小 value1:", DUCKDB_VALUE_INT32( result, j, i ), ", "
            ELSE
               ?? "最大 value1:", DUCKDB_VALUE_INT32( result, j, i )
            ENDIF
         NEXT
         ?
      NEXT
      DUCKDB_FREE( result )
   ENDIF

   nEnd := seconds()
   ? "查詢完成，耗時:", nEnd - nStart, "秒"
   ?

   // 釋放資源
   DUCKDB_DISCONNECT( con )
   DUCKDB_CLOSE( db )

RETURN

// 執行緒函數：插入資料
FUNCTION InsertData( db, con, nThreadId )
   LOCAL result, cValues
   LOCAL i, j, nIndex, nBatchId
   LOCAL nStartBatch, nEndBatch
   LOCAL nBatchesPerThread

   // 計算此執行緒要處理的批次範圍
   nBatchesPerThread := s_nTotalBatches / N_THREADS
   nStartBatch := (nThreadId - 1) * nBatchesPerThread + 1
   nEndBatch := nThreadId * nBatchesPerThread

   FOR nBatchId := nStartBatch TO nEndBatch
      cValues := ""
      FOR j := 1 TO N_BATCH_SIZE
         nIndex := (nBatchId - 1) * N_BATCH_SIZE + j
         IF j > 1
            cValues += ","
         ENDIF
         cValues += "(" + hb_ntos(nIndex) + "," + ;
                   hb_ntos(nIndex * 2) + "," + ;
                   hb_ntos(nIndex * 1.5) + ",'" + ;
                   "Value " + hb_ntos(nIndex) + "'," + ;
                   iif(nIndex % 2 == 0, "true", "false") + "," + ;
                   "'2024-" + hb_ntos(1 + (nIndex % 12)) + "-" + hb_ntos(1 + (nIndex % 28)) + "'" + ;
                   ")"
      NEXT

      result := DUCKDB_QUERY( con, "INSERT INTO large_data VALUES " + cValues )
      IF result == NIL
         ? "執行緒", nThreadId, "無法執行批量插入"
         RETURN .F.
      ENDIF

      DUCKDB_FREE( result )

      // 更新完成計數
      hb_mutexLock( s_hMutex )
      s_nCompleted++
      hb_mutexUnlock( s_hMutex )
   NEXT

RETURN .T. 