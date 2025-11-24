-- B1 DEPENDS: AFTER:SP:DWZ_CREATE_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE DWZ_CM (
IN pagingID NVARCHAR(100),
OUT dunning_cm_out DWZ_REPORT_OUT
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
docDate NVARCHAR(20);
dueDate NVARCHAR(20);
dunningDate NVARCHAR(20);
crdtNBased NVARCHAR(1);
payNoBased NVARCHAR(1);
UseBranches NVARCHAR(1);
BEGIN 

SELECT "Value" INTO crdtNBased FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'CrdtNBased';
SELECT "Value" INTO payNoBased FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'PayNoBased';

SELECT "Value" INTO docDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'docDate' ;
SELECT "Value" INTO dueDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'dueDate' ;
SELECT "Value" INTO dunningDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'dunningDate' ;

SELECT "Value" INTO UseBranches FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'UseBranches' ;

dunning_cm =

SELECT

T0."CardCode" AS "CardCode",
T2."CardName" AS "CardName",
T3."TermCode" AS "DunningCode",
T0."DocCur" AS "DocCur",
0 AS "DunnLevel", 
--TO_NVARCHAR ('INV CM') AS "ObjType",
T0."ObjType" AS "ObjType", 
T0."DocNum" AS "DocNum",
cast(T1."InstlmntID" as INTEGER) AS "InstlmntID",
T1."DueDate" AS "DueDate",
cast(NULL as TIMESTAMP) AS "LastLevelUpdateDate",
cast(NULL as TIMESTAMP) AS "LastDunningDate",
cast(NULL as TIMESTAMP) AS "NewLvlUpdDate",
T1."InsTotal" AS "DocAmountLC",
T1."InsTotalFC" AS "DocAmountFC",
cast(T1."InsTotal" - T1."PaidToDate" as DECIMAL(21,6)) AS "OpenAmountLC",
cast(T1."InsTotalFC" - T1."PaidFC" as DECIMAL(21,6)) AS "OpenAmountFC",
0 AS "InterestDays",
0 AS "Interest",
0 AS "InterestAmountLC",
0 AS "InterestAmountFC",
T3."GrpMethod" AS "GrpMethod", 0 AS "FeeLC",  0 AS "FeeFC" ,  T3."AutoPost" AS "AutoPost", cast(0 as SMALLINT) AS "YearDays", 0 AS "YearlyRate",  
T3."LetterFrmt" AS "LetterFrmt", 0 AS "OrigMinBalance", 0 AS "MinBalance",  T0."DocEntry" AS "DocEntry", 
T0."DocRate" AS "DocRate", T3."FeeCurr" AS "FeeCurr" , T3."TotalFee" AS "OrigFee" , T3."BalCurr" AS "MinBalCurr", 'N' AS "LevelUpdated", 
T0."CardCode" AS "BpCode2", 'C' AS "BpType", cast('' as nvarchar(100)) AS "CardName2", T0."Comments"AS "Comment",
T0."BPLId" AS "BPLId"

FROM "OINV" T0 
INNER JOIN "INV6" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN "TMP_DWZ_CRD" T4 ON T4."CardCode" = T0."CardCode"
INNER JOIN "OCRD" T2 ON T4."CardCode" = T2."CardCode"
INNER JOIN "ODUT" T3 ON T3."TermCode" = T2."DunTerm" 
WHERE 
T0."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T0."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
AND T0."DocDueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DocDueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
AND T0."CANCELED" = ( 'N' ) 
AND T1."Status" = ( 'O' ) 
AND T1."DunWizBlck" = ( 'N' ) 
AND T0."BlockDunn" = ( 'N' ) 
AND T0."TransId" <> ( 0 ) 
AND ( T1."InsTotal" < ( 0 ) 
	OR T1."InsTotalFC" < ( 0 ) ) 
AND (:UseBranches = 'N' OR T0."BPLId" IN 
(
	SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
))


UNION ALL
--CSI
SELECT

T0."CardCode" AS "CardCode",
T2."CardName" AS "CardName",
T3."TermCode" AS "DunningCode",
T0."DocCur" AS "DocCur",
0 AS "DunnLevel", 
--TO_NVARCHAR ('INV CM') AS "ObjType",
T0."ObjType" AS "ObjType", 
T0."DocNum" AS "DocNum",
cast(T1."InstlmntID" as INTEGER) AS "InstlmntID",
T1."DueDate" AS "DueDate",
cast(NULL as TIMESTAMP) AS "LastLevelUpdateDate",
cast(NULL as TIMESTAMP) AS "LastDunningDate",
cast(NULL as TIMESTAMP) AS "NewLvlUpdDate",
T1."InsTotal" AS "DocAmountLC",
T1."InsTotalFC" AS "DocAmountFC",
cast(T1."InsTotal" - T1."PaidToDate" as DECIMAL(21,6)) AS "OpenAmountLC",
cast(T1."InsTotalFC" - T1."PaidFC" as DECIMAL(21,6)) AS "OpenAmountFC",
0 AS "InterestDays",
0 AS "Interest",
0 AS "InterestAmountLC",
0 AS "InterestAmountFC",
T3."GrpMethod" AS "GrpMethod", 0 AS "FeeLC",  0 AS "FeeFC" ,  T3."AutoPost" AS "AutoPost",cast(0 as SMALLINT) AS "YearDays", 0 AS "YearlyRate",  
T3."LetterFrmt" AS "LetterFrmt", 0 AS "OrigMinBalance", 0 AS "MinBalance",  T0."DocEntry" AS "DocEntry",
T0."DocRate" AS "DocRate", T3."FeeCurr" AS "FeeCurr" , T3."TotalFee" AS "OrigFee" , T3."BalCurr"  AS "MinBalCurr", 'N' AS "LevelUpdated",
T0."CardCode" AS "BpCode2", 'C' AS "BpType",cast('' as nvarchar(100)) AS "CardName2", T0."Comments"AS "Comment",
T0."BPLId" AS "BPLId"

FROM "OCSI" T0 
INNER JOIN "CSI6" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN "TMP_DWZ_CRD" T4 ON T4."CardCode" = T0."CardCode"
INNER JOIN "OCRD" T2 ON T4."CardCode" = T2."CardCode"
INNER JOIN "ODUT" T3 ON T3."TermCode" = T2."DunTerm" 
WHERE 
T0."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T0."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
AND T0."DocDueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DocDueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
AND T0."CANCELED" = ( 'N' ) 
AND T1."Status" = ( 'O' ) 
AND T1."DunWizBlck" = ( 'N' ) 
AND T0."BlockDunn" = ( 'N' ) 
AND T0."TransId" <> ( 0 ) 
AND ( T1."InsTotal" < ( 0 ) 
	OR T1."InsTotalFC" < ( 0 ) ) 
AND (:UseBranches = 'N' OR T0."BPLId" IN 
(
	SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
))


UNION ALL 
--DPI
SELECT

T0."CardCode" AS "CardCode",
T2."CardName" AS "CardName",
T3."TermCode" AS "DunningCode",
T0."DocCur" AS "DocCur",
0 AS "DunnLevel",
--TO_NVARCHAR ('INV CM') AS "ObjType",
T0."ObjType" AS "ObjType", 
T0."DocNum" AS "DocNum",
cast(T1."InstlmntID" as INTEGER) AS "InstlmntID",
T1."DueDate" AS "DueDate",
cast(NULL as TIMESTAMP) AS "LastLevelUpdateDate",
cast(NULL as TIMESTAMP) AS "LastDunningDate",
cast(NULL as TIMESTAMP) AS "NewLvlUpdDate", 
-T1."InsTotal" AS "DocAmountLC",
-T1."InsTotalFC" AS "DocAmountFC",
cast(-T1."InsTotal" + T1."PaidToDate" as DECIMAL(21,6)) AS "OpenAmountLC",
cast(-T1."InsTotalFC" + T1."PaidFC" as DECIMAL(21,6)) AS "OpenAmountFC",
0 AS "InterestDays",
0 AS "Interest",
0 AS "InterestAmountLC",
0 AS "InterestAmountFC",
T3."GrpMethod" AS "GrpMethod", 0 AS "Fee",  0 AS "FeeFC" ,  T3."AutoPost" AS "AutoPost", cast(0 as SMALLINT)AS "YearDays", 0 AS "YearlyRate", 
T3."LetterFrmt" AS "LetterFrmt",
0 AS "OrigMinBalance", 0 AS "MinBalance",  T0."DocEntry",
T0."DocRate" AS "DocRate", T3."FeeCurr" AS "FeeCurr" , T3."TotalFee" AS "OrigFee" , T3."BalCurr"  AS "MinBalCurr", 'N' AS "LevelUpdated",
T0."CardCode" AS "BpCode2", 'C' AS "BpType", cast('' as nvarchar(100)) AS "CardName2", T0."Comments"AS "Comment",
T0."BPLId" AS "BPLId"
 
FROM "ODPI" T0 
INNER JOIN "DPI6" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN "TMP_DWZ_CRD" T4 ON T4."CardCode" = T0."CardCode"
INNER JOIN "OCRD" T2 ON T4."CardCode" = T2."CardCode"
INNER JOIN "ODUT" T3 ON T3."TermCode" = T2."DunTerm" 
WHERE 
T0."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T0."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
AND T0."DocDueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DocDueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
AND T0."CANCELED" = ( 'N' ) 
AND T1."Status" = ( 'O' ) 
AND T1."DunWizBlck" = ( 'N' ) 
AND T0."BlockDunn" = ( 'N' ) 
AND T0."TransId" <> ( 0 ) 
AND ( T1."InsTotal" < ( 0 ) 
	OR T1."InsTotalFC" < ( 0 ) ) 
AND (:UseBranches = 'N' OR T0."BPLId" IN 
(
	SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
))
;	
 
dunning_cm_RIN =
--RIN

SELECT

T0."CardCode" AS "CardCode",
T2."CardName" AS "CardName",
T3."TermCode" AS "DunningCode",
T0."DocCur" AS "DocCur",
0 AS "DunnLevel",
--TO_NVARCHAR ('INV CM') AS "ObjType",
T0."ObjType" AS "ObjType", 
T0."DocNum" AS "DocNum",
cast(T1."InstlmntID" as INTEGER) AS "InstlmntID",
T1."DueDate" AS "DueDate",
cast(NULL as TIMESTAMP) AS "LastLevelUpdateDate",
cast(NULL as TIMESTAMP) AS "LastDunningDate",
cast(NULL as TIMESTAMP) AS "NewLvlUpdDate", 
-T1."InsTotal" AS "DocAmountLC",
-T1."InsTotalFC" AS "DocAmountFC",
cast(-T1."InsTotal" +T1."PaidToDate" as DECIMAL(21,6)) AS "OpenAmountLC",
cast(-T1."InsTotalFC" +T1."PaidFC" as DECIMAL(21,6)) AS "OpenAmountFC",
0 AS "InterestDays",
0 AS "Interest",
0 AS "InterestAmountLC",
0 AS "InterestAmountFC",
T3."GrpMethod" AS "GrpMethod", 0 AS "Fee",  0 AS "FeeFC" ,  T3."AutoPost" AS "AutoPost",cast(0 as SMALLINT) AS "YearDays", 0 AS "YearlyRate", 
T3."LetterFrmt"  AS "LetterFrmt", 0 AS "OrigMinBalance",0 AS "MinBalance",  T0."DocEntry",
T0."DocRate" AS "DocRate", T3."FeeCurr" AS "FeeCurr" , T3."TotalFee" AS "OrigFee" , T3."BalCurr"  AS "MinBalCurr", 'N' AS "LevelUpdated",
T0."CardCode" AS "BpCode2", 'C' AS "BpType", cast('' as nvarchar(100)) AS "CardName2", T0."Comments"AS "Comment",
T0."BPLId" AS "BPLId"

FROM "ORIN" T0 
INNER JOIN "RIN6" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN "TMP_DWZ_CRD" T4 ON T4."CardCode" = T0."CardCode"
INNER JOIN "OCRD" T2 ON T4."CardCode" = T2."CardCode"
INNER JOIN "ODUT" T3 ON T3."TermCode" = T2."DunTerm" 
WHERE 
T0."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T0."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
AND T0."DocDueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DocDueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
AND T0."CANCELED" = ( 'N' ) 
AND T1."Status" = ( 'O' ) 
AND T1."DunWizBlck" = ( 'N' ) 
AND T0."BlockDunn" = ( 'N' ) 
AND T0."TransId" <> ( 0 ) 
AND ( T1."InsTotal" > ( 0 ) 
	OR T1."InsTotalFC" > ( 0 ) ) 
AND (:UseBranches = 'N' OR T0."BPLId" IN 
(
	SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
))
;

dunning_cm_RCT =
--RCT

SELECT

T1."ShortName" AS "CardCode",
T3."CardName" AS "CardName",
T4."TermCode" AS "DunningCode",
T0."DocCurr" AS "DocCur",
--'RC' AS "ObjType",
0 AS "DunnLevel",
'24' AS "ObjType",
T0."DocNum" AS "DocNum",
T1."Line_ID" AS "InstlmntID",
T0."DocDueDate" AS "DueDate",
cast(NULL as TIMESTAMP) AS "LastLevelUpdateDate",
cast(NULL as TIMESTAMP) AS "LastDunningDate",
cast(NULL as TIMESTAMP) AS "NewLvlUpdDate", 
-T0."NoDocSum" AS "DocAmountLC", 
-T0."NoDocSumFC" AS "DocAmountFC", 
-T0."OpenBal" AS "OpenAmountLC",
-T0."OpenBalFc" AS "OpenAmountFC",
0 AS "InterestDays",
0 AS "Interest",
0 AS "InterestAmountLC",
0 AS "InterestAmountFC",
T4."GrpMethod" AS "GrpMethod", 0 AS "Fee",  0 AS "FeeFC" ,  T4."AutoPost" AS "AutoPost",cast(0 as SMALLINT) AS "YearDays", 0 AS "YearlyRate", 
T4."LetterFrmt"  AS "LetterFrmt", 0 AS "OrigMinBalance", 0 AS "MinBalance", T0."DocEntry",
T0."DocRate" AS "DocRate", T4."FeeCurr" AS "FeeCurr" , T4."TotalFee" AS "OrigFee" , T4."BalCurr"  AS "MinBalCurr", 'N' AS "LevelUpdated",
T0."CardCode" AS "BpCode2", 'C' AS "BpType", cast('' as nvarchar(100)) AS "CardName2", T0."Comments"AS "Comment",
T0."BPLId" AS "BPLId"

FROM "ORCT" T0 
INNER JOIN "JDT1" T1 ON T1."TransId" = T0."TransId" AND T1."TransType" = '24' 
INNER JOIN "OJDT" T2 ON T2."TransId" = T1."TransId" 
INNER JOIN "TMP_DWZ_CRD" T5 ON T5."CardCode" = T1."ShortName"
INNER JOIN "OCRD" T3 ON T5."CardCode" = T3."CardCode"
INNER JOIN "ODUT" T4 ON T4."TermCode" = T3."DunTerm" 
WHERE
T0."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T0."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
AND T0."DocDueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DocDueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
AND (T0."NoDocSum" <> ( 0 ) OR T0."NoDocSumFC" <> ( 0 ) )
AND T0."Canceled" = ( 'N' ) 
AND ( ( T0."OpenBal" <> ( 0 ) 
		AND T0."OpenBal" IS NOT NULL ) 
	OR ( T0."OpenBalFc" <> ( 0 ) 
		AND T0."OpenBalFc" IS NOT NULL ) ) 
AND T0."WizDunBlck" = ( 'N' )
AND (:UseBranches = 'N' OR T0."BPLId" IN 
(
	SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
))
;

IF :crdtNBased = 'Y' THEN
	IF :payNoBased = 'N' THEN dunning_cm_out = SELECT * FROM :dunning_cm UNION ALL SELECT * FROM :dunning_cm_RIN ; END IF ;
	IF :payNoBased = 'Y' THEN dunning_cm_out = SELECT * FROM :dunning_cm UNION ALL SELECT * FROM :dunning_cm_RIN UNION ALL SELECT * FROM :dunning_cm_RCT ; END IF ;
ELSE
	IF :payNoBased = 'N' THEN dunning_cm_out = SELECT * FROM :dunning_cm ; END IF ;
	IF :payNoBased = 'Y' THEN dunning_cm_out = SELECT * FROM :dunning_cm UNION ALL SELECT * FROM :dunning_cm_RCT ; END IF ;
END IF;

END;











