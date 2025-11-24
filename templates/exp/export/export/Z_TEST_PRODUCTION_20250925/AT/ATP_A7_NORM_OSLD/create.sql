-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_A7_NORM_OSLD ( IN item NVARCHAR(50),
	 IN whs NVARCHAR(8),
	 IN obj_type NVARCHAR (20),
	 IN doc_entry INTEGER,
	 IN doc_line_num INTEGER,
	 IN check_type INTEGER,
	 OUT doc_lines DOC_LINES_VERSIONS,
	 OUT result ATP_DATE_QTY) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
READS SQL DATA 
AS 
BEGIN 
IF :check_type = 1 --check against required quantities

THEN -- Query to get plan issues, which will be needed to subtract when checking against required qty ------
 -- Get the plan issues through posted transactions
 TRANS_REQ = 

SELECT cast(TT."TransType" as nvarchar(20))          as "ObjType", 
       TT."DocEntry", 
       TT."DocLineNum", 
		CASE WHEN TT."TransType" = '202' AND TT."DocLineNum" = -1 THEN TO_DATE(P0."StartDate")
		     WHEN TT."TransType" = '202' AND TT."DocLineNum" >= 0 THEN TO_DATE(P1."StartDate")
		     ELSE TO_DATE(IFNULL(B0."BaseDueDate", "DocDueDate")) END AS "Date", 
--		TO_DATE(TT."DocDueDate") AS "RefDate", P0."StartDate", P1."StartDate",
       CASE 
         WHEN ( "TransType" not in ( 163, 165 ) 
                and ( "AccumType" = 2 
                      AND "ActionType" = 1 
                      AND "IsNegLnQty" = 'N' 
                       OR ( "AccumType" = 3 
                            AND "ActionType" = 2 
                            AND "IsNegLnQty" = 'Y' ) ) ) THEN "EffectQty" 
         WHEN ( "TransType" not in ( 163, 165 ) 
                and "AccumType" = 2 
                AND "ActionType" = 2 
                AND "IsNegLnQty" = 'N' 
                 OR ( "AccumType" = 3 
                      AND "ActionType" = 1 
                      AND "IsNegLnQty" = 'Y' ) ) THEN -"EffectQty" 
         --use action=13,14,5 as was line(opposite line to reverse)  
         --use action=2 as should be line  
         WHEN ( "TransType" in ( 165 ) 
                and "AccumType" = 2 
                AND "ActionType" in ( 2 ) 
                AND "IsNegLnQty" = 'N' ) THEN "CIShbQty" 
         WHEN ( "TransType" in ( 165 ) 
                and "AccumType" = 2 
                AND "ActionType" in ( 2 ) 
                AND "IsNegLnQty" = 'Y' ) THEN -"CIShbQty" 
         WHEN ( "TransType" in ( 165 ) 
                and ( ( "AccumType" = 2 
                        AND "ActionType" in ( 13 ) ) 
                       or ( "AccumType" = 0 
                            AND "ActionType" = 5 ) ) 
                AND "IsNegLnQty" = 'N' ) THEN "EffectQty" - "CIShbQty" 
         WHEN ( "TransType" in ( 165 ) 
                and ( "AccumType" = 2 
                      AND "ActionType" in ( 14 ) ) 
                AND "IsNegLnQty" = 'N' ) THEN -( "EffectQty" + "CIShbQty" ) 
         WHEN ( "TransType" in ( 165 ) 
                and ( ( "AccumType" = 2 
                        AND "ActionType" in ( 14 ) ) 
                       or ( "AccumType" = 0 
                            AND "ActionType" = 5 ) ) 
                AND "IsNegLnQty" = 'Y' ) THEN "CIShbQty" - "EffectQty" 
         WHEN ( "TransType" in ( 165 ) 
                and ( "AccumType" = 2 
                      AND "ActionType" in ( 13 ) ) 
                AND "IsNegLnQty" = 'Y' ) THEN "EffectQty" + "CIShbQty" 
         --use action=13,14,5 as was line(opposite line to reverse)  
         --use action=2 as should be line  
         WHEN ( "TransType" in ( 163 ) 
                and "AccumType" = 3 
                AND "ActionType" in ( 2 ) 
                AND "IsNegLnQty" = 'N' ) THEN -"CIShbQty" 
         WHEN ( "TransType" in ( 163 ) 
                and "AccumType" = 3 
                AND "ActionType" in ( 2 ) 
                AND "IsNegLnQty" = 'Y' ) THEN "CIShbQty" 
         WHEN ( "TransType" in ( 163 ) 
                and ( ( "AccumType" = 3 
                        AND "ActionType" in ( 14 ) ) 
                       or ( "AccumType" = 0 
                            AND "ActionType" = 5 ) ) 
                AND "IsNegLnQty" = 'N' ) THEN "EffectQty" + "CIShbQty" 
         WHEN ( "TransType" in ( 163 ) 
                and ( "AccumType" = 3 
                      AND "ActionType" in ( 13 ) ) 
                AND "IsNegLnQty" = 'N' ) THEN "CIShbQty" - "EffectQty" 
         WHEN ( "TransType" in ( 163 ) 
                and ( ( "AccumType" = 3 
                        AND "ActionType" in ( 13 ) ) 
                       or ( "AccumType" = 0 
                            AND "ActionType" = 5 ) ) 
                AND "IsNegLnQty" = 'Y' ) THEN -( "EffectQty" + "CIShbQty" ) 
         WHEN ( "TransType" in ( 163 ) 
                and ( "AccumType" = 3 
                      AND "ActionType" in ( 14 ) ) 
                AND "IsNegLnQty" = 'Y' ) THEN "EffectQty" - "CIShbQty" 
         ELSE -"EffectQty" 
       END                   as "Qty" 
-- Add column BaseDueDate for messages in OILM has another line accumType = 1
FROM OILM TT LEFT OUTER JOIN
(
	-- find max BaseDueDate for messages in OILM has another line accumType = 1
	SELECT T."MessageID", MAX(B."DocDueDate") AS "BaseDueDate"
	FROM OILM T LEFT OUTER JOIN 
	(
		SELECT T1."MessageID", T1."TransType", T1."DocEntry", T1."DocLineNum",
		T1."ItemCode", T1."LocCode",
		CASE WHEN T1."TransType" = '202' AND T1."DocLineNum" = -1 THEN P0."StartDate"
    	 WHEN T1."TransType" = '202' AND T1."DocLineNum" >= 0 THEN P1."StartDate"
    	 ELSE "DocDueDate" END AS "DocDueDate",
    	 "DocDueDate" AS "RefDate",
    	 P0."DueDate", P1."EndDate"
		FROM OILM T1
		LEFT OUTER JOIN OWOR P0
		-- find start date for production order header
		ON T1."TransType" = '202' AND
		   T1."DocEntry" = P0."DocEntry" AND
		   T1."DocLineNum" = -1
		LEFT OUTER JOIN WOR1 P1
		-- find start date for production order line
		ON T1."TransType" = '202' AND
		   T1."DocEntry" = P1."DocEntry" AND
		   T1."DocLineNum" >= 0 AND
		   T1."DocLineNum" = P1."LineNum"
		WHERE T1."ItemCode" = :item 
     	  and T1."LocCode" = :whs 
	)B
	ON T."BaseType" = B."TransType" and T."BaseAbsEnt" = B."DocEntry" and T."BaseLine" = B."DocLineNum"
	WHERE T."ItemCode" = :item 
       and T."LocCode" = :whs 
       and T."AccumType" IN (0,2,3) AND T."ActionType" IN (1,2,5,13,14) AND
		 EXISTS (
		 	SELECT 1 FROM OILM U WHERE U."AccumType" = 1 AND
		 	T."TransType" = U."TransType" AND T."DocEntry" = U."DocEntry" AND T."DocLineNum" = U."DocLineNum" 
		 ) AND
		 ( ( "AccumType" = 2 AND "ActionType" in (1, 13) AND "IsNegLnQty" = 'N' ) OR 
		   ( "AccumType" = 3 AND "ActionType" in (2, 14) AND "IsNegLnQty" = 'Y' ) OR 
		   ( "AccumType" = 2 AND "ActionType" in (2, 14) AND "IsNegLnQty" = 'N' ) OR
           ( "AccumType" = 3 AND "ActionType" in (1, 13) AND "IsNegLnQty" = 'Y' ) OR 
           ( "AccumType" = 0 AND "ActionType" = 5 ) )
	GROUP BY T."MessageID"
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
WHERE  	TT."ItemCode" = :item AND TT."LocCode" = :whs AND
		"AccumType" in ( 0, 2, 3 ) 
       and "ActionType" in ( 1, 2, 5, 13, 14 ) 
       and ( ( "AccumType" = 2 
               AND "ActionType" in (1, 13) 
               AND "IsNegLnQty" = 'N' ) 
              OR ( "AccumType" = 3 
                   AND  "ActionType" in (2, 14)
                   AND "IsNegLnQty" = 'Y' ) 
              OR ( "AccumType" = 2 
                   AND "ActionType" in (2, 14)  
                   AND "IsNegLnQty" = 'N' ) 
              OR ( "AccumType" = 3 
                   AND "ActionType" in (1, 13 ) 
                   AND "IsNegLnQty" = 'Y' ) 
              OR ( "AccumType" = 0 AND "ActionType" = 5 ) ) 
              --AND ((part 1 and part 2) or part 3 or part 4)
--part 1 = non-CI docs which need ATP check
--part 2 = part what was copied away part
--part 3 = AP correction reserve invoice
--part 4 = AR correction reserve invoice
       AND ( (( ( ( "TransType" != :obj_type 
				OR TT."DocEntry" != :doc_entry 
				OR TT."DocLineNum" != :doc_line_num
				or  "AccumType"!=2 or "ActionType" !=1)  
				 and ( "TransType" != :obj_type 
				OR TT."DocEntry" != :doc_entry 
				OR TT."DocLineNum" != :doc_line_num
				or  "AccumType"!=3 or "ActionType" !=2))--these two kinds will need ATP check in non-CI docs
			and "TransType" not in ( 163,
	 165 ) ) --no need to exclude 164/166 since they don't have ATP
	 
	  
		and ( ( "BaseType" != :obj_type --Base fields here are for current line's partially copied part
				OR "BaseAbsEnt" != :doc_entry 
				OR "BaseLine" != :doc_line_num ) 
			and ("BaseType" not in ( 163, 165 )  or "TransType" in ( 164, 166 )) ))
              --no partial copy in CI     
              or (( ( "TransType" != :obj_type 
                       OR TT."DocEntry" != :doc_entry 
                       OR TT."DocLineNum" != :doc_line_num 
                       or "AccumType" != 3 ) 
                    and "TransType" = 163 )) 
              or (( ( "TransType" != :obj_type 
                       OR TT."DocEntry" != :doc_entry 
                       OR TT."DocLineNum" != :doc_line_num 
                       --keep accumtype=0 as was line(opposite line to reverse)  
                       or "AccumType" != 2 ) 
                    and "TransType" = 165 )) )  
;
 -- Get the plan issues that still waiting for approvement.
 -- When creating a draft that sent for approvement, the required quantity will be insert into OSLD
 WFA_REQ = Select
	 "ObjType",
	 "DocEntry",
	 "DocLineNum",
	 TO_DATE("CfmDate") as "Date",
	 "ReqQty" as "Qty" 
From OSLD 
Where "ReqQty" != 0 
And "ItemCode" = :item 
And "WhsCode" = :whs 
And ("ObjType" != :obj_type 
	OR "DocEntry" != :doc_entry 
	OR "DocLineNum" != :doc_line_num)
;
 docline_series = Select
	 "ObjType",
	 "DocEntry",
	 "DocLineNum",
	 "Date",
	 cast(-"Qty" as DECIMAL(21,6)) as "Qty" 
From :TRANS_REQ 
UNION ALL Select
	 "ObjType",
	 "DocEntry",
	 "DocLineNum",
	 "Date",
	 cast(-"Qty" as DECIMAL(21,6)) as "Qty" 
From :WFA_REQ
;
 doc_lines = Select
	 "ObjType",
	 "DocEntry",
	 "DocLineNum",
	 0 as "Version" 
From :docline_series
;
 Result = Select
	 "Date",
	 cast("Qty" as DECIMAL(21,6)) as "Qty"
From :docline_series
;
ELSEIF :check_type = 2 --check against confirmed quantities

THEN -- ignore confirmations of order line with key (obj_type, doc_entry, doc_line_num)
 docline_series = SELECT
	 "ObjType",
	 "DocEntry",
	 "DocLineNum",
	 TO_DATE("CfmDate") as "Date",
	 cast(-1*"CfmQty" as DECIMAL(21,6)) as "Qty" 
FROM OSLD 
WHERE "ItemCode" = :item 
AND "WhsCode" = :whs 
AND ("ObjType" != :obj_type 
	OR "DocEntry" != :doc_entry 
	OR "DocLineNum" != :doc_line_num)
;
 doc_lines = Select
	 "ObjType",
	 "DocEntry",
	 "DocLineNum",
	 0 as "Version" 
From :docline_series
;
 Result = Select
	 "Date",
	 cast("Qty" as DECIMAL(21,6)) as "Qty"
From :docline_series
;

END 
IF
;

END
;











