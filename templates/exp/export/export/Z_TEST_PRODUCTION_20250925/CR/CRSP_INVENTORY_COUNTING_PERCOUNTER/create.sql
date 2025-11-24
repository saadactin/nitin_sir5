-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_Inventory_Counting_PerCounter(
in DocKey nvarchar(12),
in MultipleCounterType nvarchar(1),
in MultipleCounterOrder nvarchar(12)
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
CountType nvarchar(1);
CountNumber int;
BEGIN


SELECT  "OINC"."CountType" into CountType FROM "OINC" WHERE "OINC"."DocEntry" = :DocKey;

IF :CountType = '1' --single counter
THEN 
		SELECT "T1"."DocEntry", "T1"."ItemCode", "T1"."ItemDesc", "T1"."WhsCode", "T1"."BinEntry", "T2"."BinCode", "T1"."InWhsQty", "T1"."Counted", "T1"."CountQty" 
		FROM "INC1" "T1" LEFT JOIN "OBIN" "T2" ON "T1"."BinEntry"="T2"."AbsEntry" WHERE "T1"."DocEntry"= :DocKey ORDER BY "T1"."VisOrder";
 
ELSE  --multiple counters
	IF :MultipleCounterType = 'I' --individual counter
	THEN
		SELECT "T1"."CounterNum" INTO CountNumber FROM "INC8" "T1" WHERE "T1"."DocEntry" = :DocKey AND "T1"."VisOrder" = :MultipleCounterOrder;
		
		SELECT "T1"."DocEntry", "T1"."ItemCode", "T1"."ItemDesc", "T1"."WhsCode", "T1"."BinEntry", "T2"."BinCode", "T1"."InWhsQty", "T1"."Counted", "T3"."TotalQty" AS "CountQty" 
		FROM "INC1" "T1" 
		LEFT JOIN "OBIN" "T2" ON "T1"."BinEntry"="T2"."AbsEntry"
		LEFT JOIN "INC9" "T3" ON "T1"."DocEntry"="T3"."DocEntry" AND  "T1"."LineNum"="T3"."LineNum" AND "T3"."CounterNum" = :CountNumber
		WHERE "T1"."DocEntry"= :DocKey ORDER BY "T1"."VisOrder";
	ELSE  --Team counters
		SELECT "T1"."CounterNum" INTO CountNumber FROM "INC4" "T1" WHERE "T1"."DocEntry" = :DocKey AND "T1"."VisOrder" = :MultipleCounterOrder;
		
		SELECT "T1"."DocEntry", "T1"."ItemCode", "T1"."ItemDesc", "T1"."WhsCode", "T1"."BinEntry", "T2"."BinCode", "T1"."InWhsQty", "T1"."Counted", "T3"."TotalQty" AS "CountQty" 
		FROM "INC1" "T1" 
		LEFT JOIN "OBIN" "T2" ON "T1"."BinEntry"="T2"."AbsEntry"
		LEFT JOIN "INC5" "T3" ON "T1"."DocEntry"="T3"."DocEntry" AND  "T1"."LineNum"="T3"."LineNum" AND "T3"."CounterNum" = :CountNumber
		WHERE "T1"."DocEntry"= :DocKey ORDER BY "T1"."VisOrder";
	END IF;
END IF;
END;











