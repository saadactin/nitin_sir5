-- B1 DEPENDS: AFTER:SP:DWZ_CREATE_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE DWZ_JE (
IN pagingID NVARCHAR(100),
IN bpType NVARCHAR(1),
OUT dunning_je_out DWZ_REPORT_OUT
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
percentDec SMALLINT;
priceDec SMALLINT;
dspOpnItms NVARCHAR(1);
allowNegLt NVARCHAR(1);
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

SELECT "Value" INTO docDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'docDate' ;
SELECT "Value" INTO dueDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'dueDate' ;
SELECT "Value" INTO dunningDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'dunningDate' ;

SELECT "Value" INTO UseBranches FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'UseBranches' ;

dunning_je =
SELECT T2."CardCode" AS "CardCode", 
       T2."CardName" AS "CardName", 
	   T3."TermCode" AS "DunningCode",
	   MAP ( T0."FCCurrency", NULL, :mainCurncy,  MAP(LENGTH ( T0."FCCurrency"), 0, :mainCurncy, T0."FCCurrency") ) AS "DocCur", 
       T0."DueDate" AS "DueDate", 
       CASE 
       WHEN T0."Debit" >0 OR  T0."FCDebit" >0 THEN   MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1) 
       ELSE 0
       END AS "DunnLevel", 
	   MAP(T0."DunnLevel", T3."MaxLevel", 'Y', 'N') AS "IsMaxLevel", 
       T3."YearDays" AS "YearDays", 
       T3."YearlyRate" AS "YearlyRate",  
       T0."Credit" AS "Credit", 
       T0."FCCredit" AS "FCCredit", 
       T0."Debit" AS "Debit", 
       T0."FCDebit" AS "FCDebit", 
       T0."DunDate" AS "DunDate", 
       T1."TransType" AS "TransType",  
       T1."TransId" AS "TransId", 
       T0."Line_ID" AS "Line_ID", 
       T0."BalDueCred" AS "BalDueCred", 
       T0."BalFcCred", 
       T0."BalDueDeb" AS "BalDueDeb", 
       T0."BalFcDeb", 
       T0."LvlUpdDate" AS "LvlUpdDate", 
       T3."RemIntrst" AS "RemIntrst",
	   
	   T3."GrpMethod" AS "GrpMethod",
	   T3."AutoPost" AS "AutoPost",
	   
	   CASE WHEN T0."Credit" >0 OR T0."FCCredit" >0  THEN T3."TotalFee"
	   ELSE T7."LetterFee" END AS "Fee",
	   
	   CASE WHEN T3."GrpMethod" = 'B' OR T0."Credit" >0 OR T0."FCCredit" >0 THEN T3."LetterFrmt"
	   ELSE T7."LetterFrmt" END AS "LetterFrmt",
	   
	   CASE WHEN T3."GrpMethod" = 'B' OR T0."Credit" >0 OR T0."FCCredit" >0 THEN T3."MinBalance"
	   ELSE T7."MinBalance" END AS "MinBalance",
	   
	   CASE WHEN T3."GrpMethod" = 'B' OR T0."Credit" >0 OR T0."FCCredit" >0 THEN T3."BalCurr"
	   ELSE T7."MinBlnCurr" END AS "MinBlnCurr",
	   
	   CASE WHEN T3."GrpMethod" = 'B' AND T3."HiLtrFrmt" = 'N' OR T0."Credit" >0 OR T0."FCCredit" >0 THEN T3."FeeCurr"
	   ELSE T7."FeeCurr" END AS "FeeCurr",
	   
	   CASE WHEN T3."GrpMethod" = 'B' OR T0."Credit" >0 OR T0."FCCredit" >0 THEN T3."TotalFee"
	   ELSE T7."LetterFee" END AS "OrigFee",
	   
	   CASE  WHEN T3."XchgOrig"='Y'  THEN 0 ELSE T8."Rate"  END AS "Rate",
	   
	   T8."Rate" AS "DunningRate",
	   MAP ( T0."FCCurrency", NULL, 1,  MAP(LENGTH ( T0."FCCurrency"), 0, 1, (SELECT "Rate" FROM ORTT WHERE "Currency" = T0."FCCurrency" AND "RateDate" = T0."DueDate") ) ) AS "DocRate", 
	   
        
        CASE 
        WHEN  T0."Credit" >0 OR T0."FCCredit" >0 THEN 0
        ELSE
          
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
       WHEN T5."DocDate" <= :dunningDate  AND T5."DocDate" >= MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate")) THEN
	    T4."AppliedFC" * ( ( ( YEAR (T5."DocDate") - YEAR (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) * 12 + 
		( MONTH (T5."DocDate") - MONTH (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) )* T3."MonthDays" + 
		EXTRACT (DAY FROM T5."DocDate") - EXTRACT (DAY FROM (MAP (T3."DateCalInt" ,'Y', T0."DueDate", IFNULL (T0."LvlUpdDate",T0."DueDate"))) ) ) * T3."YearlyRate" / T3."YearDays"/100
       ELSE 0
	   END
	   
       END AS "RemIntrstAmountFC" ,
	   
	   T3."MonthDays" AS "MonthDays", 
	   T7."EffctAftr" AS "EffctAftr",
	   T10."CardCode" AS "BpCode2", T10."CardName" AS "CardName2", T0."LineMemo" AS "Comment",
	   T0."BPLId" AS "BPLId"
       
FROM   "JDT1" T0 
       INNER JOIN "OJDT" T1 
               ON T1."TransId" = T0."TransId" 
       INNER JOIN "OCRD" T2 
               ON (T2."CardCode" = T0."ShortName" AND bpType = 'C' OR T2."ConnBP" = T0."ShortName" AND bpType = 'S')
       INNER JOIN "TMP_DWZ_CRD" T9 ON (T2."CardCode" = T9."CardCode")
       INNER JOIN "ODUT" T3 
               ON T3."TermCode" = T2."DunTerm" 
	INNER JOIN "DUT1" T7
        ON T3."TermCode" = T7."TermCode"
			   AND MAP(T0."DunnLevel", T3."MaxLevel", T0."DunnLevel", T0."DunnLevel"+1) = T7."LevelNum" 
	INNER JOIN "OCRD" T10
               ON (T10."CardCode" = T0."ShortName")
	LEFT OUTER JOIN "ORTT" T8
			   ON MAP ( T0."FCCurrency", NULL, :mainCurncy,  MAP(LENGTH ( T0."FCCurrency"), 0, :mainCurncy, T0."FCCurrency") ) = T8."Currency" AND T8."RateDate" = (:dunningDate)
       LEFT OUTER JOIN "RCT2" T4 
                    ON T4."DocTransId" = T0."TransId" 
		    			AND T4."InvType" = '30' 
       LEFT OUTER JOIN "ORCT" T5 
                    ON T5."DocNum" = T4."DocNum" 
WHERE  
		T1."TransType" IN ('30','-2','-3') 
       AND  (IFNULL(T0."BalDueCred",0) <> 0 OR IFNULL(T0."BalDueDeb",0) <> 0  OR IFNULL(T0."BalFcCred",0) <> 0  OR IFNULL(T0."BalFcDeb",0) <> 0 )
       AND T0."RevSource" = ( 'N' ) 
       AND T1."RefDate" <= ( MAP (LENGTH (:docDate) , 0, T1."RefDate", TO_TIMESTAMP(:docDate, 'YYYYMMDD') ) ) 
       AND T0."DueDate" <= ( MAP (LENGTH (:dueDate) , 0, T0."DueDate", TO_TIMESTAMP(:dueDate, 'YYYYMMDD') ) ) 
       AND T1."BlockDunn" <> ( 'Y' ) 
       AND T0."DunWizBlck" <> ( 'Y' ) 
       AND ( T0."DunnLevel" IS NULL 
              OR T0."DunnLevel" <= T3."MaxLevel" ) 
       AND T1."StornoToTr" IS NULL 
       AND NOT EXISTS (SELECT T0.* 
                       FROM   "OJDT" U0 
                       WHERE  U0."StornoToTr" = T1."TransId") 
	   AND MAP(:dspOpnItms, 'Y', 1, ( ( ( YEAR (:dunningDate) - YEAR (IFNULL (T0."LvlUpdDate",T0."DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )* T3."MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL (T0."LvlUpdDate",T0."DueDate"))) )	-T7."EffctAftr"	) > 0
		
		AND (:UseBranches = 'N' OR T0."BPLId" IN 
		(
			SELECT CAST ("Value" as INTEGER) FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" LIKE 'BranchList%'
		))
;

dunning_je_pre =
SELECT 

"CardCode",
"CardName",
"DunningCode",
"DocCur",
CASE WHEN ( ( ( YEAR (:dunningDate) - YEAR (IFNULL ("LvlUpdDate","DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL ("LvlUpdDate","DueDate"))) )* "MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL ("LvlUpdDate","DueDate"))) ) -"EffctAftr" > 0 THEN "DunnLevel"
		ELSE "DunnLevel" -1
END
AS "DunnLevel",
"TransType" AS "ObjType", 
"TransId" AS "DocNum",
"Line_ID" AS "InstlmntID",
"DueDate",
"LvlUpdDate" AS "LastLevelUpdateDate",
"DunDate" AS "LastDunningDate",

CASE WHEN ( ( ( YEAR (:dunningDate) - YEAR (IFNULL ("LvlUpdDate","DueDate"))) * 12 + 
		( MONTH (:dunningDate) - MONTH (IFNULL ("LvlUpdDate","DueDate"))) )* "MonthDays" + 
		EXTRACT (DAY FROM :dunningDate) - EXTRACT (DAY FROM (IFNULL ("LvlUpdDate","DueDate"))) ) -"EffctAftr" > 0 
	THEN :dunningDate ELSE NULL
END
AS "NewLvlUpdDate",

ROUND("Debit"-"Credit",:priceDec)
AS "DocAmountLC",

ROUND("FCDebit"-"FCCredit",:priceDec)
AS "DocAmountFC",

ROUND(
CASE 
WHEN MAX("Debit") > 0
THEN ("BalDueDeb" + SUM("AppliedAfterDunningLC")) 
ELSE -MAX("BalDueCred")
END
,:priceDec)
AS "OpenAmountLC",

ROUND(
CASE 
WHEN MAX("FCDebit") > 0
THEN ("BalFcDeb" + SUM("AppliedAfterDunningFC")) 
ELSE -MAX("BalFcCred")
END
,:priceDec)
AS "OpenAmountFC",

CASE 
WHEN "RemIntrst" = 'N' AND MAX ("RemIntrstAmountFC" ) <> 0  THEN NULL
ELSE "InterDays"
END
AS "InterestDays",

ROUND(
CASE 
WHEN ("RemIntrst" = 'N' AND MAX ("RemIntrstAmountFC" ) <> 0 ) OR "InterDays"<=0 THEN NULL
ELSE

CASE 
WHEN MAX("Debit")> 0 OR MAX("FCDebit") > 0
THEN "YearlyRate" * "InterDays" / "YearDays" 
ELSE NULL
END 

END
,:percentDec)
AS "Interest",

ROUND(
CASE
WHEN MIN("Credit") > 0 OR MIN("FCCredit") >0 OR "InterDays"<=0 THEN NULL
ELSE

CASE 
WHEN "CalcIntrst" = 'N' THEN 0
ELSE 
	CASE WHEN IFNULL("FCDebit",0) = 0 THEN	("BalDueDeb" + SUM("AppliedAfterDunningLC")) * "YearlyRate" * "InterDays" / "YearDays" / 100 + SUM ("RemIntrstAmountLC" )
	ELSE CASE WHEN "Rate" = 0 THEN ("BalDueDeb" + SUM("AppliedAfterDunningLC")) * "YearlyRate" * "InterDays" / "YearDays" / 100 + SUM ("RemIntrstAmountLC" )
		ELSE (("BalFcDeb" + SUM("AppliedAfterDunningFC")) * "YearlyRate" * "InterDays" / "YearDays" / 100 + SUM ("RemIntrstAmountFC" ))* map(:directRate,'Y', "Rate", 1/"Rate")	END
	END
END 

END
,:priceDec)
AS "InterestAmountLC" ,

ROUND(
CASE
WHEN MIN("Credit") > 0 OR MIN("FCCredit") >0 OR "InterDays"<=0 THEN NULL
ELSE

CASE 
WHEN "CalcIntrst" = 'Y'
THEN ("BalFcDeb" + SUM("AppliedAfterDunningFC")) * "YearlyRate" * "InterDays" / "YearDays" / 100 + SUM ("RemIntrstAmountFC" )
ELSE 0
END 

END
,:priceDec)
AS "InterestAmountFC" ,

"GrpMethod" , 

"Fee" ,
ROUND (
CASE
WHEN MIN("Credit") > 0 OR MIN("FCCredit") >0 THEN 0
ELSE
MAP("FeeCurr" , :mainCurncy , "Fee" ,
	MAP (:directRate, 'Y', "Fee" * (SELECT "Rate" FROM ORTT WHERE "Currency" = "FeeCurr" AND "RateDate" = :dunningDate ),  "Fee" / (SELECT "Rate" FROM ORTT WHERE "Currency" = "FeeCurr" AND "RateDate" = :dunningDate ))
	)
	
END	,:priceDec ) AS "FeeLC", 

0 AS "FeeFC" , "AutoPost", "YearDays", "YearlyRate", "LetterFrmt", 

CASE WHEN "DunnLevel" > 0 THEN  "MinBalance" ELSE 0 END as "OrigMinBalance", 

ROUND (
CASE WHEN "MinBlnCurr" = :mainCurncy THEN "MinBalance" 
ELSE MAP (:directRate, 'Y', "MinBalance"* (SELECT "Rate" FROM ORTT WHERE "Currency" = "MinBlnCurr" AND "RateDate" = :dunningDate ),  "MinBalance" / (SELECT "Rate" FROM ORTT WHERE "Currency" = "MinBlnCurr" AND "RateDate" = :dunningDate ))
END,:priceDec ) AS "MinBalance", 

"TransId" AS "DocEntry", "DocRate", "FeeCurr", "OrigFee", "MinBlnCurr",
"BpCode2", "CardName2", "Comment", "BPLId"

FROM :dunning_je

GROUP BY "CardCode","CardName","DunningCode","DocCur","DocRate","FeeCurr","OrigFee", "MinBlnCurr","TransId" ,"TransType", "Line_ID","GrpMethod", "Rate", "DunningRate", "RemIntrst","DunnLevel", "Debit","Credit","FCDebit","FCCredit",
 "DueDate","DunDate","LvlUpdDate","DunnLevel", "CalcIntrst", "BalDueDeb", "BalFcDeb", "YearlyRate", "InterDays", "YearDays", "Fee" , "AutoPost", "LetterFrmt", "MinBalance","MonthDays","EffctAftr", "BpCode2", "CardName2", "Comment", "BPLId"
--  "AutoPost", "YearDays", "YearlyRate", "LetterFrmt"
;	
-- SELECT * FROM :dunning_je_pre;

dunning_je_out = 

SELECT 
"CardCode", "CardName", "DunningCode",  "DocCur",  
CASE WHEN bpType = 'S' THEN 0 ELSE "DunnLevel" END AS "DunnLevel", "ObjType", "DocNum", "InstlmntID", 
"DueDate", "LastLevelUpdateDate", "LastDunningDate",  "NewLvlUpdDate", 
cast("DocAmountLC" as DECIMAL(21,6)) as "DocAmountLC", cast("DocAmountFC" as DECIMAL(21,6)) as "DocAmountFC", cast("OpenAmountLC" as DECIMAL(21,6)) as "OpenAmountLC", cast("OpenAmountFC" as DECIMAL(21,6)) as "OpenAmountFC", "InterestDays", cast("Interest" as DECIMAL(21,6)) as "Interest", cast("InterestAmountLC" as DECIMAL(21,6)) as "InterestAmountLC",  cast("InterestAmountFC" as DECIMAL(21,6)) as "InterestAmountFC", 
"GrpMethod" ,  cast("FeeLC" as DECIMAL(21,6)) as  "FeeLC",  
cast(
MAP("FeeCurr", "DocCur", "Fee", 
ROUND (
MAP("DocCur" , :mainCurncy , 0 ,
	MAP (:directRate, 'Y', "FeeLC" / (SELECT "Rate" FROM ORTT WHERE "Currency" = "DocCur" AND "RateDate" = :dunningDate ),  "FeeLC" * (SELECT "Rate" FROM ORTT WHERE "Currency" = "DocCur" AND "RateDate" = :dunningDate ))
	),:priceDec ) 
)
as DECIMAL(21,6))
AS "FeeFC", 
"AutoPost" ,  "YearDays",  "YearlyRate",  "LetterFrmt" , "OrigMinBalance", cast("MinBalance" as DECIMAL(21,6)) as "MinBalance",  "DocEntry", "DocRate", "FeeCurr", "OrigFee", "MinBlnCurr" AS "MinBalCurr", 
CASE WHEN "DunnLevel" > 0 THEN MAP("NewLvlUpdDate", NULL, 'N', 'Y') ELSE 'N' END AS "LevelUpdated", 

"BpCode2" AS "BpCode2", bpType AS "BpType", "CardName2", "Comment", "BPLId"

FROM :dunning_je_pre
;

END;











