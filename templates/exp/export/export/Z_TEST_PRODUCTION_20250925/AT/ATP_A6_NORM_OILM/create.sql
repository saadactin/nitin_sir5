-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END
CREATE PROCEDURE ATP_A6_NORM_OILM (
    IN item NVARCHAR(50),
    IN whs NVARCHAR(8),
    IN includePastReceipt TINYINT,
    OUT doc_lines DOC_LINES_VERSIONS,
    OUT result ATP_DATE_QTY)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER

READS SQL DATA
AS
-- read planned goods receipts from the database
BEGIN
	docline_series = Select TT."ItemCode", TT."LocCode", TT."TransType" as "ObjType", TT."DocEntry", TT."DocLineNum",
	-- if BaseDueDate is null, use DocDueDate, for production order, use document end date
	CASE WHEN TT."TransType" = '202' AND TT."DocLineNum" = -1 THEN TO_DATE(P0."DueDate")
	     WHEN TT."TransType" = '202' AND TT."DocLineNum" >= 0 THEN TO_DATE(P1."EndDate")
	     ELSE TO_DATE(IFNULL(B0."BaseDueDate", "DocDueDate")) END AS "Date", 
	TO_DATE(TT."DocDueDate") AS "RefDate", P0."DueDate", P1."EndDate",
	CASE WHEN (("AccumType" = 2 AND ("ActionType" = 2 or "ActionType" = 14) AND "IsNegLnQty" = 'Y' ) OR
	   ("AccumType" = 3 AND ("ActionType" = 1 or "ActionType" = 13) AND "IsNegLnQty" = 'N' )) THEN "EffectQty"
	    ELSE - "EffectQty"
	    END AS "Qty"
	-- Add column BaseDueDate for messages in OILM has another line accumType = 1
	FROM OILM TT LEFT OUTER JOIN
	(
		-- find max BaseDueDate for messages in OILM has another line accumType = 1
		-- for normal records except Issue For Production (ObjType <> 60)
		SELECT T."MessageID", MAX(B."DocDueDate") AS "BaseDueDate"
		FROM OILM T LEFT OUTER JOIN 
		(
			SELECT T1."MessageID", T1."TransType", T1."DocEntry", T1."DocLineNum",
			T1."ItemCode", T1."LocCode",
			CASE WHEN T1."TransType" = '202' AND T1."DocLineNum" = -1 THEN P0."DueDate"
	    	 WHEN T1."TransType" = '202' AND T1."DocLineNum" >= 0 THEN P1."EndDate"
	    	 ELSE "DocDueDate" END AS "DocDueDate",
	    	 "DocDueDate" AS "RefDate",
	    	 P0."DueDate", P1."EndDate"
			FROM OILM T1
			LEFT OUTER JOIN OWOR P0
			-- find end date for production order header
			ON T1."TransType" = '202' AND
			   T1."DocEntry" = P0."DocEntry" AND
			   T1."DocLineNum" = -1
			LEFT OUTER JOIN WOR1 P1
			-- find end date for production order line
			ON T1."TransType" = '202' AND
			   T1."DocEntry" = P1."DocEntry" AND
			   T1."DocLineNum" >= 0 AND
			   T1."DocLineNum" = P1."LineNum"
			WHERE
			T1."ItemCode" = :item AND T1."LocCode" = :whs
		)B
		ON T."BaseType" = B."TransType" and T."BaseAbsEnt" = B."DocEntry" and T."BaseLine" = B."DocLineNum"
		WHERE
			 T."BaseType" <> '202' AND
			T."ItemCode" = :item AND T."LocCode" = :whs AND
			 T."AccumType" IN (2,3) AND T."ActionType" IN (1,2,13,14) AND
			 EXISTS (
			 	SELECT 1 FROM OILM U WHERE U."AccumType" = 1 AND
			 	T."TransType" = U."TransType" AND T."DocEntry" = U."DocEntry" AND T."DocLineNum" = U."DocLineNum" 
			 ) AND
			( (T."AccumType" = 2 AND (T."ActionType" = 2 or T."ActionType" = 14) AND T."IsNegLnQty" = 'Y') OR
			(T."AccumType" = 3 AND (T."ActionType" = 1 or T."ActionType" = 13) AND T."IsNegLnQty" = 'N') OR
			(T."AccumType" = 2 AND (T."ActionType" = 1 or T."ActionType" = 13) AND T."IsNegLnQty" = 'Y') OR
			(T."AccumType" = 3 AND (T."ActionType" = 2 or T."ActionType" = 14) AND T."IsNegLnQty" = 'N') )
		GROUP BY T."MessageID"
		-- end of for normal records except Issue For Production (ObjType <> 60)		
		UNION ALL
		-- for Issue For Production of disassembly BOM (ObjType = 60)
		SELECT T."MessageID", MAX(B."DocDueDate") AS "BaseDueDate"
		FROM OILM T LEFT OUTER JOIN 
		(
			SELECT T1."MessageID", T1."TransType", T1."DocEntry", T1."DocLineNum",
			T1."ItemCode", T1."LocCode",
			CASE WHEN T1."DocLineNum" = -1 THEN P0."DueDate"
	    	 WHEN T1."DocLineNum" >= 0 THEN P1."EndDate"
	    	 ELSE "DocDueDate" END AS "DocDueDate",
	    	 "DocDueDate" AS "RefDate",
	    	 P0."DueDate", P1."EndDate"
			FROM OILM T1
			LEFT OUTER JOIN OWOR P0
			-- find end date for production order header
			ON T1."TransType" = '202' AND
			   T1."DocEntry" = P0."DocEntry" AND
			   T1."DocLineNum" = -1
			LEFT OUTER JOIN WOR1 P1
			-- find end date for production order line
			ON T1."TransType" = '202' AND
			   T1."DocEntry" = P1."DocEntry" AND
			   T1."DocLineNum" >= 0 AND
			   T1."DocLineNum" = P1."LineNum"
			WHERE T1."TransType" = '202' AND
			T1."ItemCode" = :item AND T1."LocCode" = :whs
		)B
		ON T."BaseType" = B."TransType" and T."BaseAbsEnt" = B."DocEntry" and T."AppObjLine" = B."DocLineNum"
		WHERE
			 T."BaseType" = '202' AND
			T."ItemCode" = :item AND T."LocCode" = :whs AND
			 T."AccumType" IN (2,3) AND T."ActionType" IN (1,2,13,14) AND
			 EXISTS (
			 	SELECT 1 FROM OILM U WHERE U."AccumType" = 1 AND
			 	T."TransType" = U."TransType" AND T."DocEntry" = U."DocEntry" AND T."DocLineNum" = U."DocLineNum" 
			 ) AND
			( (T."AccumType" = 2 AND (T."ActionType" = 2 or T."ActionType" = 14) AND T."IsNegLnQty" = 'Y') OR
			(T."AccumType" = 3 AND (T."ActionType" = 1 or T."ActionType" = 13) AND T."IsNegLnQty" = 'N') OR
			(T."AccumType" = 2 AND (T."ActionType" = 1 or T."ActionType" = 13) AND T."IsNegLnQty" = 'Y') OR
			(T."AccumType" = 3 AND (T."ActionType" = 2 or T."ActionType" = 14) AND T."IsNegLnQty" = 'N') )
		GROUP BY T."MessageID"
		-- end of for Issue For Production of disassembly BOM (ObjType = 60)
	) B0
	ON TT."MessageID" = B0."MessageID"
	LEFT OUTER JOIN OWOR P0
	-- find end date for production order header
	ON TT."TransType" = '202' AND
	   TT."DocEntry" = P0."DocEntry" AND
	   TT."DocLineNum" = -1
	LEFT OUTER JOIN WOR1 P1
	-- find end date for production order line
	ON TT."TransType" = '202' AND
	   TT."DocEntry" = P1."DocEntry" AND
	   TT."DocLineNum" >= 0 AND
	   TT."DocLineNum" = P1."LineNum"
	WHERE
	TT."ItemCode" = :item AND TT."LocCode" = :whs AND
	TT."AccumType" in (2,3) and TT."ActionType" in (1,2,13,14) and
	( (TT."AccumType" = 2 AND (TT."ActionType" = 2 or TT."ActionType" = 14) AND TT."IsNegLnQty" = 'Y') OR
	(TT."AccumType" = 3 AND (TT."ActionType" = 1 or TT."ActionType" = 13) AND TT."IsNegLnQty" = 'N') OR
	(TT."AccumType" = 2 AND (TT."ActionType" = 1 or TT."ActionType" = 13) AND TT."IsNegLnQty" = 'Y') OR
	(TT."AccumType" = 3 AND (TT."ActionType" = 2 or TT."ActionType" = 14) AND TT."IsNegLnQty" = 'N') ) AND
	(:includePastReceipt = 1 OR 
	 (TT."TransType" = '202' AND TT."DocLineNum" = -1 AND P0."DueDate" >= Current_date) OR
	 (TT."TransType" = '202' AND TT."DocLineNum" >= 0 AND P1."EndDate" >= Current_date) OR
	 (TT."TransType" <> '202' AND IFNULL(B0."BaseDueDate", "DocDueDate") >= Current_date))
	;
	doc_lines = Select "ObjType", "DocEntry", "DocLineNum", 0 as "Version" From :docline_series;
	result = Select "Date", "Qty" From :docline_series;
END;











