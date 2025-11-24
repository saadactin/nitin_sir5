-- B1 DEPENDS: AFTER:SP:DWZ_CREATE_OBJECTS AFTER:PT:PROCESS_END
CREATE PROCEDURE DWZ_INV (
IN pagingID NVARCHAR(100)
,OUT dunning_doc_out DWZ_REPORT_OUT
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
percentDec SMALLINT;
priceDec SMALLINT;
dspOpnItms NVARCHAR(1);
allowNegLt NVARCHAR(1);
crdtNBased NVARCHAR(1);
docDate NVARCHAR(20);
dueDate NVARCHAR(20);
dunningDate NVARCHAR(20);
directRate NVARCHAR(1);
mainCurncy NVARCHAR(3);
UseBranches NVARCHAR(1);
BEGIN 

SELECT "PercentDec" INTO percentDec FROM "OADM";
SELECT "PriceDec" INTO priceDec FROM "OADM";
SELECT "DirectRate" INTO directRate FROM "OADM";
SELECT "MainCurncy" INTO mainCurncy FROM "OADM";

SELECT "Value" INTO dspOpnItms FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'DspOpnItms';
SELECT "Value" INTO allowNegLt FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'AllowNegLt';
SELECT "Value" INTO crdtNBased FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'CrdtNBased';

SELECT "Value" INTO docDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'docDate' ;
SELECT "Value" INTO dueDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'dueDate' ;
SELECT "Value" INTO dunningDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'dunningDate' ;

SELECT "Value" INTO UseBranches FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'UseBranches' ;

doc_out1 = 

SELECT T2."CardCode" AS "CardCode", 
       T2."CardName" AS "CardName", 
	   T3."TermCode" AS "DunningCode",
	   T1."DocCur" AS "DocCur", 
       T1."DocNum" AS "DocNum", 
       T0."DueDate" AS "DueDate", 
       MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1) AS "DunnLevel", 
	   MAP(T0."DunnLevel", T3."MaxLevel", 'Y', 'N') AS "IsMaxLevel", 
       T0."InstlmntID" AS "InstlmntID", 
       T3."YearDays" AS "YearDays", 
       T3."YearlyRate" AS "YearlyRate", 
       T0."InsTotal" AS "InsTotal", 
       T0."InsTotalFC" AS "InsTotalFC", 
       T0."PaidToDate" AS "PaidToDate", 
       T0."PaidFC", 
       T0."DunDate" AS "DunDate", 
       T1."ObjType" AS "ObjType", 
       T1."DocEntry" AS "DocEntry", 
       T0."LvlUpdDate" AS "LvlUpdDate", 
       T3."RemIntrst" AS "RemIntrst",
	   T3."GrpMethod" AS "GrpMethod",
	   T3."AutoPost" AS "AutoPost",
	   
	   --CASE WHEN T3."GrpMethod" = 'B' THEN T3."TotalFee"
	   --ELSE T7."LetterFee" END AS "Fee",
	   T7."LetterFee"  AS "Fee",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."LetterFrmt"
	   ELSE T7."LetterFrmt" END AS "LetterFrmt",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."MinBalance"
	   ELSE T7."MinBalance" END AS "MinBalance",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."BalCurr"
	   ELSE T7."MinBlnCurr" END AS "MinBlnCurr",
	   
	   T7."FeeCurr" AS "FeeCurr",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."TotalFee"
	   ELSE T7."LetterFee" END AS "OrigFee",	   
	   
	   CASE 
	   WHEN T3."XchgOrig"='Y'
	   THEN T1."DocRate"
	   ELSE T8."Rate" 
	   END
	   AS "Rate",
	   
	   T8."Rate" AS "DunningRate",
	   
	   T1."DocRate" AS "DocRate",
       
       CASE 
       WHEN T3."DateCalInt" = 'Y'
       THEN
       ( ( YEAR (:dunningDate) - YEAR (T0."DueDate")) * 12 + 
		( MONTH (:dunningDate) - MONTH (T0."DueDate")) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM T0."DueDate")
		ELSE
		( ( YEAR (:dunningDate) - YEAR (IFNULL (T0."LvlUpdDate",T0."DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL (T0."LvlUpdDate",T0."DueDate")))
		END		
		 AS "InterDays",       

       T7."CalcIntrst" AS "CalcIntrst",       
       CASE 
       WHEN T5."DocDate" > :dunningDate  THEN  T4."SumApplied"
       ELSE 0
       END AS "AppliedAfterDunningLC" ,
	   
	   CASE 
       WHEN T5."DocDate" > :dunningDate  THEN  T4."AppliedFC"
       ELSE 0
       END AS "AppliedAfterDunningFC" ,
	   
	   T4."SumApplied" AS "AppliedLC" ,
	   T4."AppliedFC" AS "AppliedFC" ,

	   CASE 
	   WHEN T3."RemIntrst" = 'Y' THEN 0
	   ELSE	   
	   CASE 
       WHEN T5."DocDate" <= :dunningDate AND T5."DocDate" >= MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))
	   THEN  T4."SumApplied"* ( ( ( YEAR (T5."DocDate") - YEAR (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) * 12 + 
		( MONTH (T5."DocDate") - MONTH (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) )* T3."MonthDays" + 
		EXTRACT (DAY FROM T5."DocDate") - EXTRACT (DAY FROM (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) ) * T3."YearlyRate" / T3."YearDays"/100
       ELSE 0
	   END	   
       END AS "RemIntrstAmountLC" ,

		CASE 
	   WHEN T3."RemIntrst" = 'Y' THEN 0
	   ELSE
	   
	   CASE 
       WHEN T5."DocDate" <= :dunningDate AND T5."DocDate" >= MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))  THEN
	    T4."AppliedFC" * ( ( ( YEAR (T5."DocDate") - YEAR (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) * 12 + 
		( MONTH (T5."DocDate") - MONTH (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) )* T3."MonthDays" + 
		EXTRACT (DAY FROM T5."DocDate") - EXTRACT (DAY FROM (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) ) * T3."YearlyRate" / T3."YearDays"/100
       ELSE 0
	   END
	   
       END AS "RemIntrstAmountFC",
	   
	   T3."MonthDays" AS "MonthDays", 
	   T7."EffctAftr" AS "EffctAftr",
       T1."BPLId" AS "BPLId"
	   
FROM   "INV6" T0 
       INNER JOIN "OINV" T1 
               ON T1."DocEntry" = T0."DocEntry" 
       INNER JOIN "TMP_DWZ_CRD" T9
				ON T9."CardCode" =  MAP(T1."FatherType", 'P', ifnull(T1."FatherCard",T1."CardCode"), T1."CardCode")
       INNER JOIN "OCRD" T2 
                ON T2."CardCode" = T9."CardCode"
       INNER JOIN "ODUT" T3 
               ON T3."TermCode" = T2."DunTerm" 
	   INNER JOIN "DUT1" T7
               ON T3."TermCode" = T7."TermCode"
			   AND MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1)  = T7."LevelNum" 
	   LEFT OUTER JOIN "ORTT" T8
			   ON T1."DocCur" = T8."Currency" AND T8."RateDate" = (:dunningDate)
       LEFT OUTER JOIN "RCT2" T4 
                    ON T4."DocEntry" = T1."DocEntry" 
                       AND T4."InvType" = '13' 
       LEFT OUTER JOIN "ORCT" T5 
                    ON T5."DocNum" = T4."DocNum" 
       LEFT OUTER JOIN "OBOE" T6 
                    ON T6."BoeKey" = T5."BoeAbs" 
WHERE  		
	   T0."Status" = ( 'O' ) 
       AND T1."TransId" <> ( 0 ) 
       AND T1."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T1."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
       AND T0."DueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
       AND T1."BlockDunn" <> ( 'Y' ) 
       AND T0."DunWizBlck" <> ( 'Y' ) 
       AND ( T0."DunnLevel" IS NULL 
              OR T0."DunnLevel" <= T3."MaxLevel" ) 
       AND ( T0."InsTotal" > ( 0 ) 
              OR T0."InsTotalFC" > ( 0 ) ) 
	   AND MAP(:dspOpnItms, 'Y', 1, ( ( ( YEAR (:dunningDate) - YEAR (IFNULL (T0."LvlUpdDate",T0."DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )	-T7."EffctAftr"	) > 0

		AND (:UseBranches = 'N' OR T1."BPLId" IN 
		(
			SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
		))
;

doc_out2 =

SELECT T2."CardCode" AS "CardCode", 
       T2."CardName" AS "CardName", 
	   T3."TermCode" AS "DunningCode",
	   T1."DocCur" AS "DocCur", 
       T1."DocNum" AS "DocNum", 
       T0."DueDate"  AS "DueDate", 
       MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1) AS "DunnLevel",
	   MAP(T0."DunnLevel", T3."MaxLevel", 'Y', 'N') AS "IsMaxLevel", 
       T0."InstlmntID" AS "InstlmntID", 
       T3."YearDays" AS "YearDays", 
       T3."YearlyRate" AS "YearlyRate", 
       T0."InsTotal" AS "InsTotal", 
       T0."InsTotalFC", 
       T0."PaidToDate" AS "PaidToDate", 
       T0."PaidFC", 
       T0."DunDate" AS "DunDate", 
       T1."ObjType" AS "ObjType", 
       T1."DocEntry" AS "DocEntry", 
       T0."LvlUpdDate" AS "LvlUpdDate", 
       T3."RemIntrst" AS "RemIntrst",
	   T3."GrpMethod" AS "GrpMethod",
	   T3."AutoPost" AS "AutoPost",
	   
	   --CASE WHEN T3."GrpMethod" = 'B' THEN T3."TotalFee"
	   --ELSE T7."LetterFee" END AS "Fee",
	   T7."LetterFee"  AS "Fee",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."LetterFrmt"
	   ELSE T7."LetterFrmt" END AS "LetterFrmt",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."MinBalance"
	   ELSE T7."MinBalance" END AS "MinBalance",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."BalCurr"
	   ELSE T7."MinBlnCurr" END AS "MinBlnCurr",
	   
	   T7."FeeCurr" AS "FeeCurr",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."TotalFee"
	   ELSE T7."LetterFee" END AS "OrigFee",
	   
	   
	   CASE 
	   WHEN T3."XchgOrig"='Y'
	   THEN T1."DocRate"
	   ELSE T8."Rate" 
	   END
	   AS "Rate",
	   
	   T8."Rate" AS "DunningRate",
	   
	   T1."DocRate" AS "DocRate",
       
       CASE 
       WHEN T3."DateCalInt" = 'Y'
       THEN
       ( ( YEAR (:dunningDate) - YEAR (T0."DueDate")) * 12 + 
		( MONTH (:dunningDate) - MONTH (T0."DueDate")) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM T0."DueDate")
		ELSE
		( ( YEAR (:dunningDate) - YEAR (IFNULL (T0."LvlUpdDate",T0."DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL (T0."LvlUpdDate",T0."DueDate")))
		END		
		 AS "InterDays",       

       T7."CalcIntrst" AS "CalcIntrst",       
       CASE 
       WHEN T5."DocDate" > :dunningDate  THEN  T4."SumApplied"
       ELSE 0
       END AS "AppliedAfterDunningLC" ,
	   
	   CASE 
       WHEN T5."DocDate" > :dunningDate  THEN  T4."AppliedFC"
       ELSE 0
       END AS "AppliedAfterDunningFC" ,
	   
	   T4."SumApplied" AS "AppliedLC" ,
	   T4."AppliedFC" AS "AppliedFC" ,

	   CASE 
	   WHEN T3."RemIntrst" = 'Y' THEN 0
	   ELSE	   
	   CASE 
       WHEN T5."DocDate" <= :dunningDate  AND T5."DocDate" >= MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate")) 
	   THEN  T4."SumApplied"* ( ( ( YEAR (T5."DocDate") - YEAR (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) * 12 + 
		( MONTH (T5."DocDate") - MONTH (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) )* T3."MonthDays" + 
		EXTRACT (DAY FROM T5."DocDate") - EXTRACT (DAY FROM (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) ) * T3."YearlyRate" / T3."YearDays"/100
       ELSE 0
	   END	   
       END AS "RemIntrstAmountLC" ,

		CASE 
	   WHEN T3."RemIntrst" = 'Y' THEN 0
	   ELSE
	   
	   CASE 
       WHEN T5."DocDate" <= :dunningDate  AND T5."DocDate" >= MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))  THEN
	    T4."AppliedFC" * ( ( ( YEAR (T5."DocDate") - YEAR (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) * 12 + 
		( MONTH (T5."DocDate") - MONTH (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) )* T3."MonthDays" + 
		EXTRACT (DAY FROM T5."DocDate") - EXTRACT (DAY FROM (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) ) * T3."YearlyRate" / T3."YearDays"/100
       ELSE 0
	   END
	   
       END AS "RemIntrstAmount"  ,
	   
	   T3."MonthDays" AS "MonthDays", 
	   T7."EffctAftr" AS "EffctAftr",
       T1."BPLId" AS "BPLId"
	   
FROM   "CSI6" T0 
       INNER JOIN "OCSI" T1 
               ON T1."DocEntry" = T0."DocEntry" 
       INNER JOIN "TMP_DWZ_CRD" T9
				ON T9."CardCode" =  MAP(T1."FatherType", 'P', ifnull(T1."FatherCard",T1."CardCode"), T1."CardCode")
       INNER JOIN "OCRD" T2 
                ON T2."CardCode" = T9."CardCode"
       INNER JOIN "ODUT" T3 
               ON T3."TermCode" = T2."DunTerm" 
	   INNER JOIN "DUT1" T7
               ON T3."TermCode" = T7."TermCode"
			   AND MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1) = T7."LevelNum" 
	   LEFT OUTER JOIN "ORTT" T8
			   ON T1."DocCur" = T8."Currency" AND T8."RateDate" = (:dunningDate)
       LEFT OUTER JOIN "RCT2" T4 
                    ON T4."DocEntry" = T1."DocEntry" 
                       AND T4."InvType" = '165' 
       LEFT OUTER JOIN "ORCT" T5 
                    ON T5."DocNum" = T4."DocNum" 
       LEFT OUTER JOIN "OBOE" T6 
                    ON T6."BoeKey" = T5."BoeAbs" 
WHERE  		
	   T0."Status" = ( 'O' ) 
       AND T1."TransId" <> ( 0 ) 
       AND T1."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T1."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
       AND T0."DueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
       AND T1."BlockDunn" <> ( 'Y' ) 
       AND T0."DunWizBlck" <> ( 'Y' ) 
       AND ( T0."DunnLevel" IS NULL 
              OR T0."DunnLevel" <= T3."MaxLevel" ) 
       AND ( T0."InsTotal" > ( 0 ) 
              OR T0."InsTotalFC" > ( 0 ) ) 
	   AND MAP(:dspOpnItms, 'Y', 1, ( ( ( YEAR (:dunningDate) - YEAR (IFNULL (T0."LvlUpdDate",T0."DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )	-T7."EffctAftr"	) > 0		
		
		AND (:UseBranches = 'N' OR T1."BPLId" IN 
		(
			SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
		))
;

doc_out3 =

SELECT T2."CardCode" AS "CardCode", 
       T2."CardName" AS "CardName", 
	   T3."TermCode" AS "DunningCode",
	   T1."DocCur" AS "DocCur", 
       T1."DocNum" AS "DocNum", 
       T0."DueDate"  AS "DueDate", 
       MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1) AS "DunnLevel",
	   MAP(T0."DunnLevel", T3."MaxLevel", 'Y', 'N') AS "IsMaxLevel", 
       T0."InstlmntID" AS "InstlmntID", 
       T3."YearDays" AS "YearDays", 
       T3."YearlyRate" AS "YearlyRate", 
       T0."InsTotal" AS "InsTotal", 
       T0."InsTotalFC", 
       T0."PaidToDate" AS "PaidToDate", 
       T0."PaidFC", 
       T0."DunDate" AS "DunDate", 
       T1."ObjType" AS "ObjType", 
       T1."DocEntry" AS "DocEntry", 
       T0."LvlUpdDate" AS "LvlUpdDate", 
       T3."RemIntrst" AS "RemIntrst",
	   T3."GrpMethod" AS "GrpMethod",
	   T3."AutoPost" AS "AutoPost",
	   
	   --CASE WHEN T3."GrpMethod" = 'B' THEN T3."TotalFee"
	   --ELSE T7."LetterFee" END AS "Fee",
	   T7."LetterFee"  AS "Fee",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."LetterFrmt"
	   ELSE T7."LetterFrmt" END AS "LetterFrmt",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."MinBalance"
	   ELSE T7."MinBalance" END AS "MinBalance",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."BalCurr"
	   ELSE T7."MinBlnCurr" END AS "MinBlnCurr",
	   
	   T7."FeeCurr" AS "FeeCurr",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."TotalFee"
	   ELSE T7."LetterFee" END AS "OrigFee",
	   
	   
	   CASE 
	   WHEN T3."XchgOrig"='Y'
	   THEN T1."DocRate"
	   ELSE T8."Rate" 
	   END
	   AS "Rate",
	   
	   T8."Rate" AS "DunningRate",
	   
	   T1."DocRate" AS "DocRate",
       
       CASE 
       WHEN T3."DateCalInt" = 'Y'
       THEN
       ( ( YEAR (:dunningDate) - YEAR (T0."DueDate")) * 12 + 
		( MONTH (:dunningDate) - MONTH (T0."DueDate")) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM T0."DueDate")
		ELSE
		( ( YEAR (:dunningDate) - YEAR (IFNULL (T0."LvlUpdDate",T0."DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL (T0."LvlUpdDate",T0."DueDate")))
		END		
		 AS "InterDays",       

       T7."CalcIntrst" AS "CalcIntrst",       
       CASE 
       WHEN T5."DocDate" > :dunningDate  THEN  T4."SumApplied"
       ELSE 0
       END AS "AppliedAfterDunningLC" ,
	   
	   CASE 
       WHEN T5."DocDate" > :dunningDate  THEN  T4."AppliedFC"
       ELSE 0
       END AS "AppliedAfterDunningFC" ,
	   
	   T4."SumApplied" AS "AppliedLC" ,
	   T4."AppliedFC" AS "AppliedFC" ,

	   CASE 
	   WHEN T3."RemIntrst" = 'Y' THEN 0
	   ELSE	   
	   CASE 
       WHEN T5."DocDate" <= :dunningDate   AND T5."DocDate" >= MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))
	   THEN  T4."SumApplied"* ( ( ( YEAR (T5."DocDate") - YEAR (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) * 12 + 
		( MONTH (T5."DocDate") - MONTH (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) )* T3."MonthDays" + 
		EXTRACT (DAY FROM T5."DocDate") - EXTRACT (DAY FROM (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) ) * T3."YearlyRate" / T3."YearDays"/100
       ELSE 0
	   END	   
       END AS "RemIntrstAmountLC" ,

		CASE 
	   WHEN T3."RemIntrst" = 'Y' THEN 0
	   ELSE
	   
	   CASE 
       WHEN T5."DocDate" <= :dunningDate  AND T5."DocDate" >= MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))  THEN
	    T4."AppliedFC" * ( ( ( YEAR (T5."DocDate") - YEAR (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) * 12 + 
		( MONTH (T5."DocDate") - MONTH (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) )* T3."MonthDays" + 
		EXTRACT (DAY FROM T5."DocDate") - EXTRACT (DAY FROM (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) ) * T3."YearlyRate" / T3."YearDays"/100
       ELSE 0
	   END
	   
       END AS "RemIntrstAmount"  	,
	   
	   T3."MonthDays" AS "MonthDays", 
	   T7."EffctAftr" AS "EffctAftr",
	   T1."BPLId" AS "BPLId"
       
FROM   "DPI6" T0 
       INNER JOIN "ODPI" T1 
               ON T1."DocEntry" = T0."DocEntry" 
       INNER JOIN "TMP_DWZ_CRD" T9
				ON T9."CardCode" =  MAP(T1."FatherType", 'P', ifnull(T1."FatherCard",T1."CardCode"), T1."CardCode")
       INNER JOIN "OCRD" T2 
                ON T2."CardCode" = T9."CardCode"
       INNER JOIN "ODUT" T3 
               ON T3."TermCode" = T2."DunTerm" 
	   INNER JOIN "DUT1" T7
               ON T3."TermCode" = T7."TermCode"
			   AND MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1) = T7."LevelNum" 
	   LEFT OUTER JOIN "ORTT" T8
			   ON T1."DocCur" = T8."Currency" AND T8."RateDate" = (:dunningDate)
       LEFT OUTER JOIN "RCT2" T4 
                    ON T4."DocEntry" = T1."DocEntry" 
                       AND T4."InvType" = '203' 
       LEFT OUTER JOIN "ORCT" T5 
                    ON T5."DocNum" = T4."DocNum" 
       LEFT OUTER JOIN "OBOE" T6 
                    ON T6."BoeKey" = T5."BoeAbs" 
WHERE  		
	    T0."Status" = ( 'O' ) 
       AND T1."TransId" <> ( 0 ) 
       AND T1."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T1."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
       AND T0."DueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
       AND T1."BlockDunn" <> ( 'Y' ) 
       AND T0."DunWizBlck" <> ( 'Y' ) 
       AND ( T0."DunnLevel" IS NULL 
              OR T0."DunnLevel" <= T3."MaxLevel" ) 
       AND ( T0."InsTotal" > ( 0 ) 
              OR T0."InsTotalFC" > ( 0 ) ) 
	   AND MAP(:dspOpnItms, 'Y', 1, ( ( ( YEAR (:dunningDate) - YEAR (IFNULL (T0."LvlUpdDate",T0."DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )	-T7."EffctAftr"	) > 0
		
		AND (:UseBranches = 'N' OR T1."BPLId" IN 
		(
			SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
		))
;

doc_out4 =

SELECT T2."CardCode" AS "CardCode", 
       T2."CardName" AS "CardName", 
	   T3."TermCode" AS "DunningCode",
	   T1."DocCur" AS "DocCur", 
       T1."DocNum" AS "DocNum", 
       T0."DueDate"  AS "DueDate", 
       MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1) AS "DunnLevel",
	   MAP(T0."DunnLevel", T3."MaxLevel", 'Y', 'N') AS "IsMaxLevel", 
       T0."InstlmntID" AS "InstlmntID", 
       T3."YearDays" AS "YearDays", 
       T3."YearlyRate" AS "YearlyRate", 
       T0."InsTotal" AS "InsTotal", 
       T0."InsTotalFC", 
       T0."PaidToDate" AS "PaidToDate", 
       T0."PaidFC", 
       T0."DunDate" AS "DunDate", 
       T1."ObjType" AS "ObjType", 
       T1."DocEntry" AS "DocEntry", 
       T0."LvlUpdDate" AS "LvlUpdDate", 
       T3."RemIntrst" AS "RemIntrst",
	   T3."GrpMethod" AS "GrpMethod",
	   T3."AutoPost" AS "AutoPost",
	   
	   --CASE WHEN T3."GrpMethod" = 'B' THEN T3."TotalFee"
	   --ELSE T7."LetterFee" END AS "Fee",
	   T7."LetterFee"  AS "Fee",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."LetterFrmt"
	   ELSE T7."LetterFrmt" END AS "LetterFrmt",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."MinBalance"
	   ELSE T7."MinBalance" END AS "MinBalance",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."BalCurr"
	   ELSE T7."MinBlnCurr" END AS "MinBlnCurr",
	   
	   T7."FeeCurr" AS "FeeCurr",
	   
	   CASE WHEN T3."GrpMethod" = 'B' THEN T3."TotalFee"
	   ELSE T7."LetterFee" END AS "OrigFee",
	   
	   
	   CASE 
	   WHEN T3."XchgOrig"='Y'
	   THEN T1."DocRate"
	   ELSE T8."Rate" 
	   END
	   AS "Rate",
	   
	   T8."Rate" AS "DunningRate",
	   
	   T1."DocRate" AS "DocRate",
       
       CASE 
       WHEN T3."DateCalInt" = 'Y'
       THEN
       ( ( YEAR (:dunningDate) - YEAR (T0."DueDate")) * 12 + 
		( MONTH (:dunningDate) - MONTH (T0."DueDate")) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM T0."DueDate")
		ELSE
		( ( YEAR (:dunningDate) - YEAR (IFNULL (T0."LvlUpdDate",T0."DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL (T0."LvlUpdDate",T0."DueDate")))
		END		
		 AS "InterDays",       

       T7."CalcIntrst" AS "CalcIntrst",       
       CASE 
       WHEN T5."DocDate" > :dunningDate  THEN  T4."SumApplied"
       ELSE 0
       END AS "AppliedAfterDunningLC" ,
	   
	   CASE 
       WHEN T5."DocDate" > :dunningDate  THEN  T4."AppliedFC"
       ELSE 0
       END AS "AppliedAfterDunningFC" ,
	   
	   T4."SumApplied" AS "AppliedLC" ,
	   T4."AppliedFC" AS "AppliedFC" ,

	   CASE 
	   WHEN T3."RemIntrst" = 'Y' THEN 0
	   ELSE	   
	   CASE 
       WHEN T5."DocDate" <= :dunningDate  AND T5."DocDate" >= MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate")) 
	   THEN  T4."SumApplied"* ( ( ( YEAR (T5."DocDate") - YEAR (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) * 12 + 
		( MONTH (T5."DocDate") - MONTH (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) )* T3."MonthDays" + 
		EXTRACT (DAY FROM T5."DocDate") - EXTRACT (DAY FROM (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) ) * T3."YearlyRate" / T3."YearDays"/100
       ELSE 0
	   END	   
       END AS "RemIntrstAmountLC" ,

		CASE 
	   WHEN T3."RemIntrst" = 'Y' THEN 0
	   ELSE
	   
	   CASE 
       WHEN T5."DocDate" <= :dunningDate  AND T5."DocDate" >= MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate")) THEN
	    T4."AppliedFC" * ( ( ( YEAR (T5."DocDate") - YEAR (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) * 12 + 
		( MONTH (T5."DocDate") - MONTH (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) )* T3."MonthDays" + 
		EXTRACT (DAY FROM T5."DocDate") - EXTRACT (DAY FROM (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) ) * T3."YearlyRate" / T3."YearDays"/100
       ELSE 0
	   END
	   
       END AS "RemIntrstAmount"  ,
	   
	   T3."MonthDays" AS "MonthDays", 
	   T7."EffctAftr" AS "EffctAftr",
	   T1."BPLId" AS "BPLId"
       
FROM   "RIN6" T0 
       INNER JOIN "ORIN" T1 
               ON T1."DocEntry" = T0."DocEntry" 
       INNER JOIN "TMP_DWZ_CRD" T9
				ON T9."CardCode" =  MAP(T1."FatherType", 'P', ifnull(T1."FatherCard",T1."CardCode"), T1."CardCode")
       INNER JOIN "OCRD" T2 
                ON T2."CardCode" = T9."CardCode"
       INNER JOIN "ODUT" T3 
               ON T3."TermCode" = T2."DunTerm" 
	   INNER JOIN "DUT1" T7
               ON T3."TermCode" = T7."TermCode"
			   AND MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1) = T7."LevelNum" 
	   LEFT OUTER JOIN "ORTT" T8
			   ON T1."DocCur" = T8."Currency" AND T8."RateDate" = (:dunningDate)
       LEFT OUTER JOIN "RCT2" T4 
                    ON T4."DocEntry" = T1."DocEntry" 
                       AND T4."InvType" = '14' 
       LEFT OUTER JOIN "ORCT" T5 
                    ON T5."DocNum" = T4."DocNum" 
       LEFT OUTER JOIN "OBOE" T6 
                    ON T6."BoeKey" = T5."BoeAbs" 
WHERE  		
	    T0."Status" = ( 'O' ) 
       AND T1."TransId" <> ( 0 ) 
       AND T1."DocDate" <= ( MAP (LENGTH (:docDate) , 0, T1."DocDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
       AND T0."DueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
       AND T1."BlockDunn" <> ( 'Y' ) 
       AND T0."DunWizBlck" <> ( 'Y' ) 
       AND ( T0."DunnLevel" IS NULL 
              OR T0."DunnLevel" <= T3."MaxLevel" ) 
       AND ( T0."InsTotal" < ( 0 ) 
              OR T0."InsTotalFC" < ( 0 ) ) 
	   AND MAP(:dspOpnItms, 'Y', 1, ( ( ( YEAR (:dunningDate) - YEAR (IFNULL (T0."LvlUpdDate",T0."DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )	-T7."EffctAftr"	) > 0	
		
		AND (:UseBranches = 'N' OR T1."BPLId" IN 
		(
			SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
		))
;

IF :crdtNBased='Y' THEN 
doc_out_all =
SELECT * FROM :doc_out1 UNION ALL
SELECT * FROM :doc_out2 UNION ALL
SELECT * FROM :doc_out3 UNION ALL
SELECT * FROM :doc_out4 ;
ELSE
doc_out_all =
SELECT * FROM :doc_out1 UNION ALL
SELECT * FROM :doc_out2 UNION ALL
SELECT * FROM :doc_out3 ;
END IF ;

doc_out_pre =
SELECT 
"CardCode",
"CardName",
"DunningCode",
"DocCur",
MAP("IsMaxLevel",'Y',"DunnLevel" ,
CASE WHEN ( ( ( YEAR (:dunningDate) - YEAR (IFNULL ("LvlUpdDate","DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL ("LvlUpdDate","DueDate"))) )* "MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL ("LvlUpdDate","DueDate"))) ) -"EffctAftr" >0
		THEN "DunnLevel" ELSE "DunnLevel"-1
END)
AS "DunnLevel",
"ObjType", 
--CASE WHEN "ObjType" = '13' THEN 'IN' ELSE "ObjType" END AS "ObjType",
"DocNum",
"InstlmntID",
"DueDate",
"LvlUpdDate" AS "LastLevelUpdateDate",
"DunDate" AS "LastDunningDate",

CASE WHEN ( ( ( YEAR (:dunningDate) - YEAR (IFNULL ("LvlUpdDate","DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL ("LvlUpdDate","DueDate"))) )* "MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL ("LvlUpdDate","DueDate"))) ) -"EffctAftr" >0
		THEN :dunningDate ELSE '10000000'
END
AS "NewLvlUpdDate",

CASE
WHEN "ObjType" = '14' THEN -"InsTotal" 
ELSE "InsTotal" 
END
AS "DocAmountLC",

CASE
WHEN "ObjType" = '14' THEN -"InsTotalFC" 
ELSE "InsTotalFC" 
END
AS "DocAmountFC",

ROUND(
CASE 
WHEN "ObjType" = '14' THEN -"InsTotal"+ ("PaidToDate" - SUM("AppliedAfterDunningLC"))
ELSE "InsTotal"- ("PaidToDate" - SUM("AppliedAfterDunningLC"))
END
,:priceDec)
AS "OpenAmountLC", 

ROUND(
CASE 
WHEN "ObjType" = '14' THEN -"InsTotalFC"+ ("PaidFC" - SUM("AppliedAfterDunningFC"))
ELSE "InsTotalFC"- ("PaidFC" - SUM("AppliedAfterDunningFC"))
END
,:priceDec)
AS "OpenAmountFC", 

CASE 
WHEN "RemIntrst" = 'N' AND (SUM("AppliedLC") + SUM("AppliedFC")) <> 0 THEN NULL
ELSE "InterDays" 
END AS "InterestDays", 

CASE WHEN  "InterDays" <= 0 THEN NULL ELSE
ROUND(
CASE 
WHEN "RemIntrst" = 'N' AND (SUM("AppliedLC") + SUM("AppliedFC")) <> 0  THEN NULL
ELSE "YearlyRate" * "InterDays" / "YearDays"  
END
,:percentDec)
END AS "Interest",

CASE WHEN "CalcIntrst" = 'N' OR "InterDays" <= 0 THEN 0
ELSE
	ROUND(CASE WHEN IFNULL("InsTotalFC",0) = 0 THEN
		CASE WHEN "ObjType" = '14'
		THEN (-"InsTotal"+ ("PaidToDate" - SUM("AppliedAfterDunningLC"))) * "YearlyRate" * "InterDays" / "YearDays" / 100 - SUM ("RemIntrstAmountLC" )
		ELSE ("InsTotal"- ("PaidToDate" - SUM("AppliedAfterDunningLC"))) * "YearlyRate" * "InterDays" / "YearDays" / 100  + SUM ("RemIntrstAmountLC" )
		END
	ELSE
		ROUND(CASE WHEN "ObjType" = '14'
		THEN (-"InsTotalFC"+ ("PaidFC" - SUM("AppliedAfterDunningFC"))) * "YearlyRate" * "InterDays" / "YearDays" / 100 - SUM ("RemIntrstAmountFC" )
		ELSE ("InsTotalFC"- ("PaidFC" - SUM("AppliedAfterDunningFC"))) * "YearlyRate" * "InterDays" / "YearDays" / 100  + SUM ("RemIntrstAmountFC" )
		END,:priceDec)* map(:directRate,'Y', "Rate", 1/"Rate")
	END,:priceDec)
END AS "InterestAmountLC",

ROUND(
CASE 
WHEN "CalcIntrst" = 'N' OR "InterDays" <= 0 THEN 0
ELSE

CASE 
WHEN "ObjType" = '14'
THEN (-"InsTotalFC"+ ("PaidFC" - SUM("AppliedAfterDunningFC"))) * "YearlyRate" * "InterDays" / "YearDays" / 100 - SUM ("RemIntrstAmountFC" )
ELSE
("InsTotalFC"- ("PaidFC" - SUM("AppliedAfterDunningFC"))) * "YearlyRate" * "InterDays" / "YearDays" / 100  + SUM ("RemIntrstAmountFC" )
END

END 
,:priceDec)
AS "InterestAmountFC",

"GrpMethod" , 

"Fee",

ROUND (
MAP("FeeCurr" , :mainCurncy , "Fee" ,
	MAP (:directRate, 'Y', "Fee" * (SELECT "Rate" FROM ORTT WHERE "Currency" = "FeeCurr" AND "RateDate" = :dunningDate ),  "Fee" / (SELECT "Rate" FROM ORTT WHERE "Currency" = "FeeCurr" AND "RateDate" = :dunningDate ))
	),:priceDec ) AS "FeeLC", 
	
--"Fee" AS "FeeLC",
0  AS "FeeFC",

"AutoPost", "YearDays", "YearlyRate", "LetterFrmt", "MinBalance" as "OrigMinBalance", 
ROUND (
CASE WHEN "MinBlnCurr" = :mainCurncy THEN "MinBalance" 
ELSE MAP (:directRate, 'Y', "MinBalance"* (SELECT "Rate" FROM ORTT WHERE "Currency" = "MinBlnCurr" AND "RateDate" = :dunningDate ),  "MinBalance" / (SELECT "Rate" FROM ORTT WHERE "Currency" = "MinBlnCurr" AND "RateDate" = :dunningDate ))
END,:priceDec ) AS "MinBalance", 

"DocEntry", "DocRate", "FeeCurr" ,"OrigFee","MinBlnCurr" AS "MinBalCurr", "BPLId"

FROM
:doc_out_all

GROUP BY  "CardCode","CardName", "DunningCode", "IsMaxLevel", "FeeCurr", "OrigFee", "MinBlnCurr", "DocCur","GrpMethod", "ObjType","Rate","DocNum","InstlmntID", "RemIntrst","InstlmntID","DueDate", "DunDate", "DocNum" ,"DunnLevel","LvlUpdDate", "InsTotal", "InsTotalFC" ,"PaidToDate","PaidFC", "YearlyRate", "InterDays", "YearDays", "CalcIntrst" , 
"Fee", "DunningRate", "AutoPost", "LetterFrmt", "MinBalance","MinBlnCurr","MonthDays","EffctAftr", "DocEntry", "DocRate", "BPLId"

;

dunning_doc_out = 

--DOC with Debit
SELECT "CardCode", "CardName", "DunningCode",  "DocCur",  "DunnLevel", "ObjType", "DocNum", "InstlmntID", 
"DueDate", "LastLevelUpdateDate", "LastDunningDate",  "NewLvlUpdDate", 
"DocAmountLC", "DocAmountFC", cast("OpenAmountLC" as DECIMAL(21,6)) as "OpenAmountLC", cast("OpenAmountFC" as DECIMAL(21,6)) as "OpenAmountFC", "InterestDays", 
cast("Interest" as DECIMAL(21,6)) as "Interest", cast("InterestAmountLC" as DECIMAL(21,6)) as "InterestAmountLC",  cast("InterestAmountFC" as DECIMAL(21,6)) as "InterestAmountFC", 
"GrpMethod" ,  cast("FeeLC" as DECIMAL(21,6))  as "FeeLC",  
cast(
MAP("DocCur" , :mainCurncy , 0 ,
MAP("FeeCurr", "DocCur", "Fee",
ROUND (
MAP (:directRate, 'Y', "FeeLC" / (SELECT "Rate" FROM ORTT WHERE "Currency" = "DocCur" AND "RateDate" = :dunningDate ),  "FeeLC" * (SELECT "Rate" FROM ORTT WHERE "Currency" = "DocCur" AND "RateDate" = :dunningDate ))
,:priceDec ) 
))
as DECIMAL(21,6))
AS "FeeFC", 
"AutoPost" ,  "YearDays",  "YearlyRate",  "LetterFrmt" , "OrigMinBalance", cast("MinBalance" as DECIMAL(21,6)) as "MinBalance",  "DocEntry", "DocRate", "FeeCurr" , "OrigFee","MinBalCurr", 
(CASE WHEN "NewLvlUpdDate" = '10000000' THEN 'N' ELSE 'Y' END) AS "LevelUpdated",
--MAP("NewLvlUpdDate", '10000000', 'N', 'Y') AS "LevelUpdated"
"CardCode" AS "BpCode2", 'C' AS "BpType", "CardName" AS "CardName2", '' AS "Comment", "BPLId"

FROM
:doc_out_pre
;   
	  
--select * from :dunning_doc_out;
END;











