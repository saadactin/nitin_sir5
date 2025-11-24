-- B1 DEPENDS: AFTER:SP:DWZ_CREATE_OBJECTS AFTER:SP:DWZ_JE AFTER:PT:PROCESS_END

CREATE PROCEDURE DWZ_VENDOR_DOC (
IN pagingID NVARCHAR(100),
OUT dunning_vendor_out DWZ_REPORT_OUT
) LANGUAGE SQLSCRIPT AS 
docDate NVARCHAR(20);
dueDate NVARCHAR(20);
dunningDate NVARCHAR(20);
crdtNBased NVARCHAR(1);
payNoBased NVARCHAR(1);
manualJEs NVARCHAR(1);
UseBranches NVARCHAR(1);
BEGIN 

SELECT "Value" INTO crdtNBased FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'CrdtNBased';
SELECT "Value" INTO payNoBased FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'PayNoBased';
SELECT "Value" INTO manualJEs FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'ManualJEs';

SELECT "Value" INTO docDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'docDate' ;
SELECT "Value" INTO dueDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'dueDate' ;
SELECT "Value" INTO dunningDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'dunningDate' ;

SELECT "Value" INTO UseBranches FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'UseBranches' ;

dunning_pch =
SELECT
T2."CardCode" AS "CardCode",
T2."CardName" AS "CardName",
T3."TermCode" AS "DunningCode",
T0."DocCur" AS "DocCur",
0 AS "DunnLevel", 
T0."ObjType" AS "ObjType", 
T0."DocNum" AS "DocNum",
cast(T1."InstlmntID" as INTEGER) AS "InstlmntID",
T1."DueDate" AS "DueDate",
cast(NULL as TIMESTAMP) AS "LastLevelUpdateDate",
cast(NULL as TIMESTAMP) AS "LastDunningDate",
cast(NULL as TIMESTAMP) AS "NewLvlUpdDate",
-T1."InsTotal" AS "DocAmountLC",
-T1."InsTotalFC" AS "DocAmountFC",
cast(-(T1."InsTotal" - T1."PaidToDate") as DECIMAL(21,6)) AS "OpenAmountLC",
cast(-(T1."InsTotalFC" - T1."PaidFC") as DECIMAL(21,6)) AS "OpenAmountFC",
0 AS "InterestDays",
cast(0 as DECIMAL(21,6)) AS "Interest",
cast(0 as DECIMAL(21,6)) AS "InterestAmountLC",
cast(0 as DECIMAL(21,6)) AS "InterestAmountFC",
T3."GrpMethod" AS "GrpMethod", cast(0 as DECIMAL(21,6)) AS "FeeLC",cast(0 as DECIMAL(21,6)) AS "FeeFC" ,  T3."AutoPost" AS "AutoPost", cast(0 as SMALLINT) AS "YearDays",cast(0 as DECIMAL(21,6)) AS "YearlyRate",  
T3."LetterFrmt" AS "LetterFrmt",cast(0 as DECIMAL(21,6)) AS "OrigMinBalance",cast(0 as DECIMAL(21,6)) AS "MinBalance",  T0."DocEntry" AS "DocEntry", 
T0."DocRate" AS "DocRate", T3."FeeCurr" AS "FeeCurr" , T3."TotalFee" AS "OrigFee" , T3."BalCurr" AS "MinBalCurr", cast('N' as NVARCHAR(1)) AS "LevelUpdated", 
T0."CardCode" AS "BpCode2", cast('S' as NVARCHAR(1)) AS "BpType", T5."CardName" AS "CardName2", T0."Comments" AS "Comment", T0."BPLId" AS "BPLId"
FROM "OPCH" T0 
INNER JOIN "PCH6" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN "OCRD" T2 ON T0."CardCode" = T2."ConnBP"
INNER JOIN "TMP_DWZ_CRD" T4 ON T4."CardCode" = T2."CardCode"
INNER JOIN "ODUT" T3 ON T3."TermCode" = T2."DunTerm"
INNER JOIN "OCRD" T5 ON T0."CardCode" = T5."CardCode" 
WHERE 
T0."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T0."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
AND T0."DocDueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DocDueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
AND T0."CANCELED" = ( 'N' ) 
AND T1."Status" = ( 'O' ) 
AND T1."DunWizBlck" = ( 'N' ) 
AND T0."BlockDunn" = ( 'N' ) 
AND T0."TransId" <> ( 0 )
AND (:UseBranches = 'N' OR T0."BPLId" IN 
(
	SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
))
 ;

dunning_dpo =
SELECT
T2."CardCode" AS "CardCode",
T2."CardName" AS "CardName",
T3."TermCode" AS "DunningCode",
T0."DocCur" AS "DocCur",
0 AS "DunnLevel", 
T0."ObjType" AS "ObjType", 
T0."DocNum" AS "DocNum",
cast(T1."InstlmntID" as INTEGER) AS "InstlmntID",
T1."DueDate" AS "DueDate",
cast(NULL as TIMESTAMP) AS "LastLevelUpdateDate",
cast(NULL as TIMESTAMP) AS "LastDunningDate",
cast(NULL as TIMESTAMP) AS "NewLvlUpdDate",
-T1."InsTotal" AS "DocAmountLC",
-T1."InsTotalFC" AS "DocAmountFC",
cast(-(T1."InsTotal" - T1."PaidToDate") as DECIMAL(21,6)) AS "OpenAmountLC",
cast(-(T1."InsTotalFC" - T1."PaidFC") as DECIMAL(21,6)) AS "OpenAmountFC",
0 AS "InterestDays",
cast(0 as DECIMAL(21,6)) AS "Interest",
cast(0 as DECIMAL(21,6)) AS "InterestAmountLC",
cast(0 as DECIMAL(21,6)) AS "InterestAmountFC",
T3."GrpMethod" AS "GrpMethod",cast(0 as DECIMAL(21,6)) AS "FeeLC",cast(0 as DECIMAL(21,6)) AS "FeeFC" ,  T3."AutoPost" AS "AutoPost",cast(0 as SMALLINT) AS "YearDays",cast(0 as DECIMAL(21,6)) AS "YearlyRate",  
T3."LetterFrmt" AS "LetterFrmt", 0 AS "OrigMinBalance", cast(0 as DECIMAL(21,6)) AS "MinBalance",  T0."DocEntry" AS "DocEntry", 
T0."DocRate" AS "DocRate", T3."FeeCurr" AS "FeeCurr" , T3."TotalFee" AS "OrigFee" , T3."BalCurr" AS "MinBalCurr", cast('N' as NVARCHAR(1)) AS "LevelUpdated", 
T0."CardCode" AS "BpCode2", cast('S' as NVARCHAR(1)) AS "BpType", T5."CardName" AS "CardName2", T0."Comments" AS "Comment", T0."BPLId" AS "BPLId"
FROM "ODPO" T0 
INNER JOIN "DPO6" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN "OCRD" T2 ON T0."CardCode" = T2."ConnBP"
INNER JOIN "TMP_DWZ_CRD" T4 ON T4."CardCode" = T2."CardCode"
INNER JOIN "ODUT" T3 ON T3."TermCode" = T2."DunTerm"
INNER JOIN "OCRD" T5 ON T0."CardCode" = T5."CardCode" 
WHERE 
T0."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T0."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
AND T0."DocDueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DocDueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
AND T0."CANCELED" = ( 'N' ) 
AND T1."Status" = ( 'O' ) 
AND T1."DunWizBlck" = ( 'N' ) 
AND T0."BlockDunn" = ( 'N' ) 
AND T0."TransId" <> ( 0 )
AND (:UseBranches = 'N' OR T0."BPLId" IN 
(
	SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
))
 ;

dunning_cpi =
SELECT
T2."CardCode" AS "CardCode",
T2."CardName" AS "CardName",
T3."TermCode" AS "DunningCode",
T0."DocCur" AS "DocCur",
0 AS "DunnLevel", 
T0."ObjType" AS "ObjType", 
T0."DocNum" AS "DocNum",
cast(T1."InstlmntID" as INTEGER) AS "InstlmntID",
T1."DueDate" AS "DueDate",
cast(NULL as TIMESTAMP) AS "LastLevelUpdateDate",
cast(NULL as TIMESTAMP) AS "LastDunningDate",
cast(NULL as TIMESTAMP) AS "NewLvlUpdDate",
-T1."InsTotal" AS "DocAmountLC",
-T1."InsTotalFC" AS "DocAmountFC",
cast(-(T1."InsTotal" - T1."PaidToDate") as DECIMAL(21,6)) AS "OpenAmountLC",
cast(-(T1."InsTotalFC" - T1."PaidFC") as DECIMAL(21,6)) AS "OpenAmountFC",
0 AS "InterestDays",
cast(0 as DECIMAL(21,6)) AS "Interest",
cast(0 as DECIMAL(21,6)) AS "InterestAmountLC",
cast(0 as DECIMAL(21,6)) AS "InterestAmountFC",
T3."GrpMethod" AS "GrpMethod", cast(0 as DECIMAL(21,6)) AS "FeeLC",cast(0 as DECIMAL(21,6)) AS "FeeFC" ,  T3."AutoPost" AS "AutoPost",cast(0 as SMALLINT) AS "YearDays",cast(0 as DECIMAL(21,6)) AS "YearlyRate",  
T3."LetterFrmt" AS "LetterFrmt",cast(0 as DECIMAL(21,6)) AS "OrigMinBalance",cast(0 as DECIMAL(21,6)) AS "MinBalance",  T0."DocEntry" AS "DocEntry", 
T0."DocRate" AS "DocRate", T3."FeeCurr" AS "FeeCurr" , T3."TotalFee" AS "OrigFee" , T3."BalCurr" AS "MinBalCurr", cast('N' as NVARCHAR(1)) AS "LevelUpdated", 
T0."CardCode" AS "BpCode2", cast('S' as NVARCHAR(1)) AS "BpType", T5."CardName" AS "CardName2", T0."Comments" AS "Comment", T0."BPLId" AS "BPLId"
FROM "OCPI" T0 
INNER JOIN "CPI6" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN "OCRD" T2 ON T0."CardCode" = T2."ConnBP"
INNER JOIN "TMP_DWZ_CRD" T4 ON T4."CardCode" = T2."CardCode"
INNER JOIN "ODUT" T3 ON T3."TermCode" = T2."DunTerm"
INNER JOIN "OCRD" T5 ON T0."CardCode" = T5."CardCode" 
WHERE 
T0."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T0."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
AND T0."DocDueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DocDueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
AND T0."CANCELED" = ( 'N' ) 
AND T1."Status" = ( 'O' ) 
AND T1."DunWizBlck" = ( 'N' ) 
AND T0."BlockDunn" = ( 'N' ) 
AND T0."TransId" <> ( 0 )
AND (:UseBranches = 'N' OR T0."BPLId" IN 
(
	SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
))
;

dunning_rpc =
SELECT
T2."CardCode" AS "CardCode",
T2."CardName" AS "CardName",
T3."TermCode" AS "DunningCode",
T0."DocCur" AS "DocCur",
0 AS "DunnLevel", 
T0."ObjType" AS "ObjType", 
T0."DocNum" AS "DocNum",
cast(T1."InstlmntID" as INTEGER) AS "InstlmntID",
T1."DueDate" AS "DueDate",
cast(NULL as TIMESTAMP) AS "LastLevelUpdateDate",
cast(NULL as TIMESTAMP) AS "LastDunningDate",
cast(NULL as TIMESTAMP) AS "NewLvlUpdDate",
T1."InsTotal" AS "DocAmountLC",
T1."InsTotalFC" AS "DocAmountFC",
cast((T1."InsTotal" - T1."PaidToDate") as DECIMAL(21,6)) AS "OpenAmountLC",
cast((T1."InsTotalFC" - T1."PaidFC") as DECIMAL(21,6)) AS "OpenAmountFC",
0 AS "InterestDays",
cast(0 as DECIMAL(21,6)) AS "Interest",
cast(0 as DECIMAL(21,6)) AS "InterestAmountLC",
cast(0 as DECIMAL(21,6)) AS "InterestAmountFC",
T3."GrpMethod" AS "GrpMethod",cast(0 as DECIMAL(21,6)) AS "FeeLC",cast(0 as DECIMAL(21,6)) AS "FeeFC" ,  T3."AutoPost" AS "AutoPost",cast(0 as SMALLINT) AS "YearDays",cast(0 as DECIMAL(21,6)) AS "YearlyRate",  
T3."LetterFrmt" AS "LetterFrmt",cast(0 as DECIMAL(21,6)) AS "OrigMinBalance",cast(0 as DECIMAL(21,6)) AS "MinBalance",  T0."DocEntry" AS "DocEntry", 
T0."DocRate" AS "DocRate", T3."FeeCurr" AS "FeeCurr" , T3."TotalFee" AS "OrigFee" , T3."BalCurr" AS "MinBalCurr", cast('N' as NVARCHAR(1)) AS "LevelUpdated", 
T0."CardCode" AS "BpCode2", cast('S' as NVARCHAR(1)) AS "BpType", T5."CardName" AS "CardName2", T0."Comments" AS "Comment", T0."BPLId" AS "BPLId"
FROM "ORPC" T0 
INNER JOIN "RPC6" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN "OCRD" T2 ON T0."CardCode" = T2."ConnBP"
INNER JOIN "TMP_DWZ_CRD" T4 ON T4."CardCode" = T2."CardCode"
INNER JOIN "ODUT" T3 ON T3."TermCode" = T2."DunTerm"
INNER JOIN "OCRD" T5 ON T0."CardCode" = T5."CardCode" 
WHERE 
T0."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T0."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
AND T0."DocDueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DocDueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
AND T0."CANCELED" = ( 'N' ) 
AND T1."Status" = ( 'O' ) 
AND T1."DunWizBlck" = ( 'N' ) 
AND T0."BlockDunn" = ( 'N' ) 
AND T0."TransId" <> ( 0 )
AND (:UseBranches = 'N' OR T0."BPLId" IN 
(
	SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
))
;

dunning_RCT =

SELECT
T3."CardCode" AS "CardCode",
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
cast(0 as DECIMAL(21,6)) AS "Interest",
cast(0 as DECIMAL(21,6)) AS "InterestAmountLC",
cast(0 as DECIMAL(21,6)) AS "InterestAmountFC",
T4."GrpMethod" AS "GrpMethod", 0 AS "Fee",  0 AS "FeeFC" ,  T4."AutoPost" AS "AutoPost",cast(0 as SMALLINT) AS "YearDays",cast(0 as DECIMAL(21,6)) AS "YearlyRate", 
T4."LetterFrmt"  AS "LetterFrmt", 0 AS "OrigMinBalance", 0 AS "MinBalance", T0."DocEntry",
T0."DocRate" AS "DocRate", T4."FeeCurr" AS "FeeCurr" , T4."TotalFee" AS "OrigFee" , T4."BalCurr"  AS "MinBalCurr", cast('N' as NVARCHAR(1)) AS "LevelUpdated",
T0."CardCode" AS "BpCode2", cast('S' as NVARCHAR(1)) AS "BpType", T6."CardName" AS "CardName2", T0."Comments"AS "Comment", T0."BPLId" AS "BPLId"
	 
FROM "ORCT" T0 
INNER JOIN "JDT1" T1 ON T1."ShortName" = T0."CardCode" AND T1."TransId" = T0."TransId" AND T1."TransType" = '24' 
INNER JOIN "OJDT" T2 ON T2."TransId" = T1."TransId" 
INNER JOIN "OCRD" T3 ON T0."CardCode" = T3."ConnBP"
INNER JOIN "ODUT" T4 ON T4."TermCode" = T3."DunTerm" 
INNER JOIN "TMP_DWZ_CRD" T5 ON T5."CardCode" = T3."CardCode"
INNER JOIN "OCRD" T6 ON T0."CardCode" = T6."CardCode"
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

-- select * from :dunning_RCT;

IF :crdtNBased = 'Y' THEN
	marketing_doc_out = SELECT * FROM :dunning_pch 
		UNION ALL SELECT * FROM :dunning_dpo 
		UNION ALL SELECT * FROM :dunning_cpi
		UNION ALL SELECT * FROM :dunning_rpc;
ELSE
	marketing_doc_out = SELECT * FROM :dunning_pch 
		UNION ALL SELECT * FROM :dunning_dpo 
		UNION ALL SELECT * FROM :dunning_cpi;
END IF;

IF :payNoBased = 'N' THEN 
	mktDocAndPayment = SELECT * FROM :marketing_doc_out;
ELSE
	mktDocAndPayment = SELECT * FROM :marketing_doc_out UNION ALL SELECT * FROM :dunning_RCT;
END IF ;

IF :manualJEs = 'N' THEN
	mktDocAndPaymentAndJE = SELECT * FROM :mktDocAndPayment;
ELSE
	CALL DWZ_JE(pagingID, 'S', vendor_je_out);
	mktDocAndPaymentAndJE = SELECT * FROM :mktDocAndPayment UNION ALL SELECT * FROM :vendor_je_out;
END IF;

dunning_vendor_out = SELECT * FROM :mktDocAndPaymentAndJE;
END;











