//#include "harbour_duckdb.h"
#include "duckdb.ch"

PROCEDURE Main()
   LOCAL db, con, stmt, result
   LOCAL aDate, aTime, aTimestamp
   LOCAL nDays, nMicros, nMonths

   HB_CDPSELECT("UTF8")
   // 開啟資料庫
   db := DUCKDB_OPEN( "test3.db" )
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

   // 測試日期轉換
   ? "日期轉換測試:"
   ? "原始日期: 2024-03-15"
   nDays := DUCKDB_TO_DATE( 2024, 3, 15 )
   ? "轉換為天數:", nDays
   aDate := DUCKDB_FROM_DATE( nDays )
   ? "轉換回日期:", aDate[1], "-", aDate[2], "-", aDate[3]
   ?

   // 測試時間轉換
   ? "時間轉換測試:"
   ? "原始時間: 14:30:45.123456"
   nMicros := DUCKDB_TO_TIME( 14, 30, 45, 123456 )
   ? "轉換為微秒:", nMicros
   aTime := DUCKDB_FROM_TIME( nMicros )
   ? "轉換回時間:", aTime[1], ":", aTime[2], ":", aTime[3], ".", aTime[4]
   ?

   // 測試時間戳記轉換
   ? "時間戳記轉換測試:"
   ? "原始時間戳記: 2024-03-15 14:30:45.123456"
   nMicros := DUCKDB_TO_TIMESTAMP( 2024, 3, 15, 14, 30, 45, 123456 )
   ? "轉換為微秒:", nMicros
   aTimestamp := DUCKDB_FROM_TIMESTAMP( nMicros )
   ? "轉換回時間戳記:", aTimestamp[1], "-", aTimestamp[2], "-", aTimestamp[3], " "
   ?? aTimestamp[4], ":", aTimestamp[5], ":", aTimestamp[6], ".", aTimestamp[7]
   ?

   // 測試間隔時間轉換
   ? "間隔時間轉換測試:"
   ? "原始間隔: 1個月2天3小時"
   nMonths := 1
   nDays := 2
   nMicros := 3 * 3600000000 // 3小時轉換為微秒
   ? "轉換為結構:"
   ? "  月數:", nMonths
   ? "  天數:", nDays
   ? "  微秒:", nMicros
   ?

   // 釋放資源
   DUCKDB_DISCONNECT( con )
   DUCKDB_CLOSE( db )

RETURN 