-- B1 DEPENDS: AFTER:SP:ATP_A8_AGG_TIME_SERIES AFTER:SP:ATP_B8_CUMULATE AFTER:PT:PROCESS_END
CREATE PROCEDURE ATP_C3_FREE_QTY (
       IN item NVARCHAR(50), -- Product
       IN whs NVARCHAR(8), -- Warehouse
	   IN includePastReceipt TINYINT,
	   IN check_type INTEGER,
       IN date DATE
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
READS SQL DATA
AS
-- calculate cumulated ATP quantity
BEGIN
  CALL ATP_A8_AGG_TIME_SERIES (:item, :whs, 0, 0, 0, :check_type, :includePastReceipt, ?, TS);
  TIMESERIES = SELECT TO_INT(TO_DATS("Date")) AS date, "Qty" AS qty FROM :TS ORDER BY "Date";
  CALL ATP_B8_CUMULATE(:TIMESERIES, CUM_QTY);
  RESULT = (SELECT TOP 1 :date AS "Date", qty AS "Qty" FROM :CUM_QTY WHERE DATE <= TO_INT(TO_DATS(:date)) ORDER BY DATE DESC)
           UNION ALL
           (SELECT TO_DATE(TO_NCHAR(date)) AS "Date", qty AS "Qty" FROM :CUM_QTY WHERE DATE > TO_INT(TO_DATS(:date)));
SELECT * FROM :RESULT;
END;











