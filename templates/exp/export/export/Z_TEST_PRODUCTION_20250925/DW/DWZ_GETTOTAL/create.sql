-- B1 DEPENDS: AFTER:SP:DWZ_INV AFTER:SP:DWZ_JE AFTER:SP:DWZ_CM AFTER:SP:DWZ_VENDOR_DOC AFTER:PT:PROCESS_END

CREATE PROCEDURE DWZ_GETTOTAL (
IN pagingID NVARCHAR(100)
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
dspOpnItms NVARCHAR(1);
ovrDueOnly NVARCHAR(1);
allowNegLt NVARCHAR(1);
manualJEs NVARCHAR(1);
considerVendor NVARCHAR(1);
dunningDate TIMESTAMP;
foreignCurrNum INT;
priceDec SMALLINT;
directRate NVARCHAR(1);
mainCurncy NVARCHAR(3);
rowidx INT;
forCurrNum INT;
foreignCurrency NVARCHAR(1000);
forCurr NVARCHAR(3);
BEGIN 

SELECT "Value" INTO dspOpnItms FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'DspOpnItms';
SELECT "Value" INTO manualJEs FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'ManualJEs';
SELECT "Value" INTO considerVendor FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'ConsiderVendor';

SELECT "PriceDec" INTO priceDec FROM "OADM";
SELECT "DirectRate" INTO directRate FROM "OADM";
SELECT "MainCurncy" INTO mainCurncy FROM "OADM";
SELECT TO_TIMESTAMP("Value", 'YYYYMMDD') INTO dunningDate FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'dunningDate';

UPDATE "SPPP" SET "Value"='' WHERE "PagingID" = :pagingID AND "Name" = 'dueDate' AND "Value" is null;

CALL DWZ_INV(pagingID, doc_out);
CALL DWZ_CM(pagingID, cm_out);

IF :manualJEs = 'Y' THEN 
	CALL DWZ_JE(pagingID, 'C',je_out); 
	customer_out = SELECT * FROM :doc_out 
		UNION ALL SELECT * FROM :je_out 
		UNION ALL SELECT * FROM :cm_out ;
ELSE
	customer_out = SELECT * FROM :doc_out 
		UNION ALL SELECT * FROM :cm_out ;
END IF ;

IF (:considerVendor = 'Y') THEN 
	CALL DWZ_VENDOR_DOC(pagingID, vendor_out);	
	primary_out = SELECT * FROM :customer_out UNION ALL SELECT * FROM :vendor_out;
ELSE
	primary_out = SELECT * FROM :customer_out;
END IF;

DELETE FROM TMP_DWZ_CRD;
DELETE FROM TMP_DWZ_OUT_LINEPROPERTY;

tmpresult1 = 
SELECT
"CardCode", "DunnLevel" ,"DunningCode", "CardName","DocCur", "ObjType" ,"DocNum" ,"InstlmntID" ,"DueDate","LastLevelUpdateDate","LastDunningDate", "NewLvlUpdDate" ,
CASE WHEN "GrpMethod" IS NULL THEN NULL ELSE SUM("DocAmountLC") END AS "DocAmountLC" ,
CASE WHEN "GrpMethod" IS NULL THEN NULL ELSE SUM("DocAmountFC") END AS "DocAmountFC" ,
CASE WHEN "GrpMethod" IS NULL THEN NULL ELSE SUM("OpenAmountLC") END AS "OpenAmountLC" ,
CASE WHEN "GrpMethod" IS NULL THEN NULL ELSE SUM("OpenAmountFC") END AS "OpenAmountFC" ,
"InterestDays","Interest", 
CASE WHEN "GrpMethod" IS NULL THEN NULL ELSE SUM("InterestAmountLC") END AS "InterestAmountLC" ,
CASE WHEN "GrpMethod" IS NULL THEN NULL ELSE SUM("InterestAmountFC") END AS "InterestAmountFC" ,
"FeeLC" , "FeeFC" ,
	
(SELECT "AutoPost" FROM OCRD WHERE "CardCode" = T0."CardCode" ) AS"AutoPost",
CASE WHEN "GrpMethod" is null THEN 0 
WHEN "DueDate" is not null THEN 999 
WHEN "GrpMethod" = 'B' and "ObjType" is null and "DunnLevel" is null THEN 1
WHEN "GrpMethod" = 'L' and "ObjType" is null and "DunnLevel" is not null THEN 1 
-- If per Invoice in dunning term, CM type documents should in one letter
WHEN "GrpMethod" = 'I' 	THEN 
    (CASE WHEN SUM("DocAmountLC") >0 and "ObjType" is not null and "DueDate" is null THEN 1
		 WHEN SUM("DocAmountLC") <=0 and "ObjType" is null and "DunnLevel" is not null THEN 1
	 END)
END AS "LineProperty",
"YearDays", "YearlyRate", "LetterFrmt" , "OrigMinBalance", "MinBalance", 
"GrpMethod", "DocEntry", "DocRate", "FeeCurr", "OrigFee", "MinBalCurr", "LevelUpdated", 
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"

FROM :primary_out T0
GROUP BY
GROUPING SETS 
--BP layer , "LineProperty" =0
( ("CardCode","CardName"),
-- !!!"LetterFrmt", "MinBalance"
--Letter title layer , "LineProperty" =1
	--"GrpMethod" = 'B'
("CardCode","CardName", "DunningCode", "BPLId", "LetterFrmt", "OrigFee", "DocCur", "GrpMethod", "AutoPost"), 
	--"GrpMethod" = 'L'
("CardCode","CardName", "DunningCode", "LetterFrmt", "FeeCurr", "OrigFee", "MinBalCurr", "DocCur","GrpMethod" , "DunnLevel", "BPLId", "FeeLC","FeeFC", "AutoPost", "MinBalance"), 
	--"GrpMethod" = 'I'
("CardCode","CardName", "DunningCode", "LetterFrmt", "FeeCurr", "OrigFee", "MinBalCurr", "DocCur","GrpMethod" , "DunnLevel", "BPLId","FeeLC","FeeFC","ObjType", "DocNum", "InstlmntID", "AutoPost", "MinBalance", "BpType"),
--Transaction detail layer , "LineProperty" =999
("CardCode","CardName", "DunningCode", "LetterFrmt", "FeeCurr", "OrigFee", "MinBalCurr", "OrigMinBalance", "MinBalance", "DocCur", "DocRate", "YearDays", "YearlyRate", "GrpMethod" , "DunnLevel", "BPLId", "FeeLC","FeeFC", "ObjType", "DocNum","DocEntry", "InstlmntID","DueDate","LastLevelUpdateDate","LastDunningDate","NewLvlUpdDate" ,"LevelUpdated", "InterestDays", "Interest", "BpCode2", "BpType", "CardName2", "Comment", "BPLId") 
);

-- select * from :tmpresult1 ;

tmpresult2 =

SELECT 'N' AS "CheckLine" ,'N' AS "ExeChkLine",
row_number () over (order by "CardCode","LetterFrmt", "DocCur","BpType","DunnLevel","BPLId","ObjType" ,"DocNum" ,"InstlmntID" , "DueDate", "DocAmountLC", "DocAmountFC") AS "RowId" , 
"CardCode","DunningCode",
row_number () over (PARTITION BY "CardCode", "LineProperty" )  AS "LetterNum",
"DunnLevel","CardName","DocCur", "ObjType" ,"DocNum" ,"InstlmntID" ,"DueDate","LastLevelUpdateDate","LastDunningDate", "NewLvlUpdDate", 
"DocAmountLC" ,"DocAmountFC" ,"OpenAmountLC" ,"OpenAmountFC" ,"InterestDays","Interest", "InterestAmountLC" ,"InterestAmountFC" ,
IFNULL ("OpenAmountLC",0) + IFNULL ("InterestAmountLC",0) AS "TotalInclAmountLC", IFNULL ("OpenAmountFC",0) + IFNULL ("InterestAmountFC",0) AS "TotalInclAmountFC" ,
"FeeLC" , "FeeFC" ,
0 AS "OverallTotalLC", 0 AS "OverallTotalFC",
"AutoPost","LineProperty", "YearDays" , "YearlyRate", "LetterFrmt" , 

MAP("GrpMethod", 'B', (SELECT "MinBalance" FROM ODUT WHERE "TermCode" = "DunningCode"), "OrigMinBalance" ) AS "OrigMinBalance", 
MAP("GrpMethod", 'B', 
	(ROUND (
CASE WHEN (SELECT "BalCurr" FROM ODUT WHERE "TermCode" = "DunningCode") = :mainCurncy THEN (SELECT "MinBalance" FROM ODUT WHERE "TermCode" = "DunningCode")
ELSE MAP (:directRate, 'Y', (SELECT "MinBalance" FROM ODUT WHERE "TermCode" = "DunningCode")* (SELECT "Rate" FROM ORTT WHERE "Currency" = (SELECT "BalCurr" FROM ODUT WHERE "TermCode" = "DunningCode") AND "RateDate" = :dunningDate ), 
(SELECT "MinBalance" FROM ODUT WHERE "TermCode" = "DunningCode") / (SELECT "Rate" FROM ORTT WHERE "Currency" = (SELECT "BalCurr" FROM ODUT WHERE "TermCode" = "DunningCode") AND "RateDate" = :dunningDate ))
END,:priceDec )), "MinBalance" ) AS"MinBalance", 

"GrpMethod", "DocEntry", "DocRate", "FeeCurr", "OrigFee", "MinBalCurr", "LevelUpdated", NULL AS "ParentId", 
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"

FROM :tmpresult1
WHERE "LineProperty" is not null
ORDER BY "CardCode", "LetterFrmt","DocCur","DunnLevel", "BPLId", "ObjType" ,"DocNum" ,"InstlmntID" ,"DueDate", "DocAmountLC", "DocAmountFC" ;

-- select * from :tmpresult2 ;

tmpresult3 = 
SELECT "CheckLine", "ExeChkLine", "RowId", "CardCode", 
MAP ("LineProperty", 999, NULL, "DunningCode") AS "DunningCode",
"LetterNum", "DunnLevel", "CardName", "DocCur", "ObjType", "DocNum", "InstlmntID", "DueDate"
, "LastLevelUpdateDate", "LastDunningDate", "NewLvlUpdDate", "DocAmountLC" , "DocAmountFC" , "OpenAmountLC" , "OpenAmountFC" , 	
"InterestDays", "Interest" , "InterestAmountLC" , 	"InterestAmountFC" , 	"TotalInclAmountLC" , 	"TotalInclAmountFC" , 	

MAP("LineProperty", 1, 
MAP("GrpMethod", 'B', 
	MAP((SELECT "HiLtrFrmt" FROM ODUT WHERE "TermCode" = "DunningCode"), 'Y',
		--Use Fee of max level as letter fee if it's highest letter format
		(SELECT "FeeLC" FROM :tmpresult2 WHERE "RowId" = (select MAX("RowId") from :tmpresult2 where "CardCode" = T0."CardCode" AND "BpType" = 'C' GROUP BY "CardCode") ),
		--
		ROUND (
			MAP( (SELECT "FeeCurr" FROM ODUT WHERE "TermCode" = "DunningCode" ), :mainCurncy , (SELECT "TotalFee" FROM ODUT WHERE "TermCode" = "DunningCode" ) ,
				MAP (:directRate, 'Y', ((SELECT "TotalFee" FROM ODUT WHERE "TermCode" = "DunningCode" ) * (SELECT "Rate" FROM ORTT WHERE "Currency" = (SELECT "FeeCurr" FROM ODUT WHERE "TermCode" = "DunningCode" ) AND "RateDate" = :dunningDate )),  
					 ((SELECT "TotalFee" FROM ODUT WHERE "TermCode" = "DunningCode" ) / (SELECT "Rate" FROM ORTT WHERE "Currency" = (SELECT "FeeCurr" FROM ODUT WHERE "TermCode" = "DunningCode" ) AND "RateDate" = :dunningDate )))
				),:priceDec ) ) ,
	"FeeLC"
	)
,"FeeLC")	AS "FeeLC", 

"FeeFC" , 	
"OverallTotalLC" , 	 
"OpenAmountFC" + "InterestAmountFC" + "FeeFC" AS "OverallTotalFC" ,
"AutoPost"  , 	"LineProperty", "YearDays", "YearlyRate" , 	"LetterFrmt", "OrigMinBalance", "MinBalance" , "GrpMethod", "DocEntry", "DocRate", "FeeCurr", "OrigFee", "MinBalCurr", "LevelUpdated",
MAP("LineProperty", 0, NULL, (select MAX("RowId") from :tmpresult2 where "CardCode" = T0."CardCode" and "RowId" < T0."RowId" and "LineProperty" <  T0."LineProperty" ) ) 
AS "ParentId",
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"
FROM :tmpresult2 T0 ORDER BY "RowId";

-- SELECT * FROM :tmpresult3 ; --ORDER BY "RowId";
--SELECT "RowId" FROM :tmpresult3 WHERE "OpenAmountLC" <= "MinBalance" AND "LineProperty" = 1 AND IFNULL ("DunnLevel",1) > 0 ;

IF :dspOpnItms  = 'N' THEN
tmpresult4 =
SELECT "CheckLine", "ExeChkLine", "RowId", "CardCode", "DunningCode","LetterNum", "DunnLevel", "CardName", "DocCur", "ObjType", "DocNum", "InstlmntID", "DueDate"
, "LastLevelUpdateDate", "LastDunningDate", "NewLvlUpdDate", "DocAmountLC" , "DocAmountFC" , "OpenAmountLC" , "OpenAmountFC" , 	
"InterestDays", "Interest" , "InterestAmountLC" , 	"InterestAmountFC" , 	"TotalInclAmountLC" , 	"TotalInclAmountFC" , 	
"FeeLC", 

MAP("LineProperty", 1,
MAP("GrpMethod", 'B',
	MAP((SELECT "HiLtrFrmt" FROM ODUT WHERE "TermCode" = "DunningCode"), 'Y', 
		--Use Fee of max level as letter fee if it's highest letter format
		MAP("DocCur" , :mainCurncy , 0 ,(SELECT "FeeFC" FROM :tmpresult3 WHERE "RowId" = (select MAX("RowId") from :tmpresult3 where "CardCode" = T0."CardCode" GROUP BY "CardCode") ) ),
		--
		ROUND (
			MAP("DocCur" , :mainCurncy , 0 ,
				MAP (:directRate, 'Y', "FeeLC" / (SELECT "Rate" FROM ORTT WHERE "Currency" = "DocCur" AND "RateDate" = :dunningDate ),  "FeeLC" * (SELECT "Rate" FROM ORTT WHERE "Currency" = "DocCur" AND "RateDate" = :dunningDate ))
				),:priceDec ) )
	, "FeeFC" )
, "FeeFC" )	AS "FeeFC" ,

"OverallTotalLC" , 	"OverallTotalFC" ,
"AutoPost", "LineProperty", "YearDays", "YearlyRate","LetterFrmt", "OrigMinBalance", "MinBalance" , "GrpMethod", "DocEntry", "DocRate", "FeeCurr", "OrigFee", "MinBalCurr", "LevelUpdated","ParentId", 
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"
 FROM :tmpresult3 T0
WHERE MAP("LineProperty", 0, 0, 1,"RowId",999,"ParentId") NOT IN ( SELECT "RowId" FROM :tmpresult3 WHERE ("OpenAmountLC" <= "MinBalance") AND "LineProperty" = 1 AND IFNULL ("DunnLevel",1) > 0);

ELSE
-- If display all open items, no need compare with minimum Balance 
tmpresult4 =
SELECT "CheckLine", "ExeChkLine", "RowId", "CardCode", "DunningCode","LetterNum", "DunnLevel", "CardName", "DocCur", "ObjType", "DocNum", "InstlmntID", "DueDate"
, "LastLevelUpdateDate", "LastDunningDate", "NewLvlUpdDate", "DocAmountLC" , "DocAmountFC" , "OpenAmountLC" , "OpenAmountFC" , 	
"InterestDays", "Interest" , "InterestAmountLC" , 	"InterestAmountFC" , 	"TotalInclAmountLC" , 	"TotalInclAmountFC" , 	
"FeeLC", 

MAP("LineProperty", 1,
MAP("GrpMethod", 'B',
	MAP((SELECT "HiLtrFrmt" FROM ODUT WHERE "TermCode" = "DunningCode"), 'Y', 
		--Use Fee of max level as letter fee if it's highest letter format
		MAP("DocCur" , :mainCurncy , 0 ,(SELECT "FeeFC" FROM :tmpresult3 WHERE "RowId" = (select MAX("RowId") from :tmpresult3 where "CardCode" = T0."CardCode" GROUP BY "CardCode") ) ),
		--
		ROUND (
			MAP("DocCur" , :mainCurncy , 0 ,
				MAP (:directRate, 'Y', "FeeLC" / (SELECT "Rate" FROM ORTT WHERE "Currency" = "DocCur" AND "RateDate" = :dunningDate ),  "FeeLC" * (SELECT "Rate" FROM ORTT WHERE "Currency" = "DocCur" AND "RateDate" = :dunningDate ))
				),:priceDec ) )
	, "FeeFC" )
, "FeeFC" )	AS "FeeFC" ,

"OverallTotalLC" , 	"OverallTotalFC" ,
"AutoPost"  , 	"LineProperty", "YearDays", "YearlyRate" , 	"LetterFrmt", "OrigMinBalance", "MinBalance" , "GrpMethod", "DocEntry", "DocRate", "FeeCurr", "OrigFee", "MinBalCurr", "LevelUpdated","ParentId" , 
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"
 FROM :tmpresult3 T0 ;

END IF ;

-- SELECT * FROM :tmpresult4 ORDER BY "RowId";

SELECT "Value" INTO ovrDueOnly FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'OvrDueOnly';
SELECT "Value" INTO allowNegLt FROM "SPPP" WHERE "PagingID" = :pagingID AND "Name" = 'AllowNegLt';

overdueCards = SELECT "CardCode" from :tmpresult4 group by "CardCode" HAVING (min("DueDate") < :dunningDate );
positiveCards = SELECT DISTINCT "CardCode" from :tmpresult4 where "GrpMethod" <> 'B' OR ("GrpMethod" = 'B' and "LineProperty" = 1 and  map (IFNULL ("TotalInclAmountFC",0) , 0, "TotalInclAmountLC", "TotalInclAmountFC") > 0 );
hasChildrenCards = SELECT "CardCode" FROM :tmpresult4 group by  "CardCode" HAVING count(1) > 1 ;

IF :ovrDueOnly = 'Y' THEN  
	IF :allowNegLt = 'N' THEN 	cards = SELECT T0."CardCode" AS "CardCode" FROM :hasChildrenCards T0 INNER JOIN :overdueCards T1 ON T0."CardCode"=T1."CardCode" INNER JOIN :positiveCards T2 ON T1."CardCode"=T2."CardCode";
	ELSE cards =  SELECT T0."CardCode" AS "CardCode" FROM :hasChildrenCards T0 INNER JOIN :overdueCards T1 ON T0."CardCode"=T1."CardCode" ; 
	END IF;
ELSE
	IF :allowNegLt = 'N' THEN 	cards = SELECT T0."CardCode" AS "CardCode" FROM :hasChildrenCards T0 INNER JOIN :positiveCards T1 ON T0."CardCode"=T1."CardCode" ;
	ELSE cards =  SELECT T0."CardCode" AS "CardCode" FROM :hasChildrenCards T0; 
	END IF;
END IF ;

tmpresult5=
SELECT T0.* FROM :tmpresult4 T0 INNER JOIN :cards T1 ON T0."CardCode"=T1."CardCode" ;

tmpresult6=
SELECT 
"CheckLine" ,  "ExeChkLine" , 
row_number () over (order by  "CardCode","LetterFrmt","DocCur", "BPLId" ,"LetterNum" , "DunnLevel", "ObjType" ,"DocNum" ,"InstlmntID" ,"DueDate" 	, "DocAmountLC", "DocAmountFC" ) AS "RowId"  , 
"CardCode" ,  
row_number () over (PARTITION BY "CardCode", "LineProperty" )  AS "LetterNum", 
"DunnLevel" ,  
MAP("LineProperty" ,0 , "CardName", NULL) AS "CardName" ,  
"DocCur" , "ObjType" , "DocNum" , "InstlmntID" , "DueDate" , 
"LastLevelUpdateDate" , "LastDunningDate" ,  "NewLvlUpdDate" , "DocAmountLC" , "DocAmountFC" , "OpenAmountLC" , "OpenAmountFC" , 
"InterestDays" , "Interest" , "InterestAmountLC" ,  "InterestAmountFC" ,   "TotalInclAmountLC" ,  "TotalInclAmountFC"  , 
MAP ("LineProperty" , 1, "FeeLC" ,NULL) AS "FeeLC" ,
MAP ("LineProperty" , 1, "FeeFC" ,NULL) AS "FeeFC" ,
IFNULL ("OpenAmountLC",0) + IFNULL ("InterestAmountLC",0) + IFNULL ("FeeLC",0) AS "OverallTotalLC",
IFNULL ("OpenAmountFC",0) + IFNULL ("InterestAmountFC",0) + IFNULL ("FeeFC",0) AS "OverallTotalFC" ,
MAP("LineProperty" ,1 ,"AutoPost", NULL) AS "AutoPost" , 
"LineProperty" ,  "YearDays" ,  "YearlyRate" ,  
--CASE WHEN "GrpMethod" = 'B' THEN LAG("LetterFrmt",1) over (order by "CardCode","LetterNum" , "DunnLevel", "ObjType" ,"DocNum" ,"InstlmntID" ,"DueDate" 	, "DocAmountLC", "DocAmountFC" )  
--ELSE "LetterFrmt" END AS "LetterFrmt", 
"LetterFrmt" AS "LetterFrmt",
"OrigMinBalance",  "GrpMethod" ,  "DocEntry" , "DocRate", "FeeCurr", "OrigFee", "MinBalCurr",  "LevelUpdated","ParentId", 
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"

FROM :tmpresult5 T0
ORDER BY  "CardCode","LetterFrmt","DocCur", "LetterNum", "BPLId", "DunnLevel", "ObjType" ,"DocNum" ,"InstlmntID" ,"DueDate" 	, "DocAmountLC", "DocAmountFC" ;

--  Fill "InstlmntID" of BP, Parent
tmpresult7=

SELECT 
"CheckLine" ,  "ExeChkLine" , 
"RowId"  , 
"CardCode" , --MAP ("LineProperty", 0, "CardCode", NULL) AS "CardCode" ,  
MAP ("LineProperty", 1, "LetterNum", NULL) AS "LetterNum", 
"DunnLevel",  
MAP ("LineProperty", 0, "CardName", NULL) AS "CardName" ,  
"DocCur" , "ObjType" , "DocNum" , "InstlmntID" , 
"DueDate" , "LastLevelUpdateDate" , "LastDunningDate" ,  "NewLvlUpdDate" , 

MAP("LineProperty" ,0, NULL, CAST ("DocAmountLC" AS NVARCHAR(40))) AS "DocAmountLC" ,  
MAP("LineProperty" ,0, NULL, CAST ("DocAmountFC" AS NVARCHAR(40))) AS "DocAmountFC" ,
MAP("LineProperty" ,0, NULL, CAST ("OpenAmountLC" AS NVARCHAR(40))) AS "OpenAmountLC" ,  
MAP("LineProperty" ,0, NULL, CAST ("OpenAmountFC" AS NVARCHAR(40))) AS "OpenAmountFC" , 
"InterestDays" , "Interest" ,   
MAP("LineProperty" ,0, NULL, CAST ("InterestAmountLC" AS NVARCHAR(40))) AS "InterestAmountLC" ,  
MAP("LineProperty" ,0, NULL, CAST ("InterestAmountFC" AS NVARCHAR(40))) AS "InterestAmountFC" ,
MAP("LineProperty" ,0, NULL, CAST ("TotalInclAmountLC" AS NVARCHAR(40))) AS "TotalInclAmountLC" ,  
MAP("LineProperty" ,0, NULL, CAST ("TotalInclAmountFC" AS NVARCHAR(40))) AS "TotalInclAmountFC" ,  
MAP("LineProperty" ,0, NULL, CAST ("FeeLC" AS NVARCHAR(40))) AS "FeeLC" ,  
MAP("LineProperty" ,0, NULL, CAST ("FeeFC" AS NVARCHAR(40))) AS "FeeFC" ,  
MAP("LineProperty" ,1, CAST ("OverallTotalLC" AS NVARCHAR(40)), NULL) AS "OverallTotalLC" ,  
MAP("LineProperty" ,1, CAST ("OverallTotalFC" AS NVARCHAR(40)), NULL) AS "OverallTotalFC" , 
"AutoPost" , "LineProperty" , "YearDays" ,  "YearlyRate" , "LetterFrmt",
"OrigMinBalance", --CAST ("OrigMinBalance" AS NVARCHAR(40)) AS "OrigMinBalance",  
"GrpMethod" ,  "DocEntry" , 
CAST ("DocRate" AS NVARCHAR(40)) AS "DocRate", 
"FeeCurr", 
"OrigFee",--CAST ("OrigFee" AS NVARCHAR(40)) AS "OrigFee", 
"MinBalCurr", "LevelUpdated", '' AS "DunAddr", '' AS "DocText",
MAP("LineProperty" ,0, NULL, "DocAmountLC"  ) AS "DocAmountLCBackup" ,  
MAP("LineProperty" ,0, NULL, "DocAmountFC"  ) AS "DocAmountFCBackup" ,
MAP("LineProperty" ,0, NULL, "OpenAmountLC" ) AS "OpenAmountLCBackup",  
MAP("LineProperty" ,0, NULL, "OpenAmountFC" ) AS "OpenAmountFCBackup", 
MAP("LineProperty" ,0, NULL, "InterestAmountLC"  ) AS  "InterestAmountLCBackup" ,  
MAP("LineProperty" ,0, NULL, "InterestAmountFC"  ) AS  "InterestAmountFCBackup" ,
MAP("LineProperty" ,0, NULL, "TotalInclAmountLC" ) AS  "TotalInclAmountLCBackup",  
MAP("LineProperty" ,0, NULL, "TotalInclAmountFC" ) AS  "TotalInclAmountFCBackup",  
MAP("LineProperty" ,0, NULL, "FeeLC") AS "FeeLCBackup",  
MAP("LineProperty" ,0, NULL, "FeeFC") AS "FeeFCBackup",  
MAP("LineProperty" ,1, "OverallTotalLC", NULL) AS "OverallTotalLCBackup",  
MAP("LineProperty" ,1, "OverallTotalFC", NULL) AS "OverallTotalFCBackup", 
MAP("LineProperty", 0, NULL, (select MAX("RowId") from :tmpresult6 where "CardCode" = T0."CardCode" and "RowId" < T0."RowId" and "LineProperty" <  T0."LineProperty" ) ) AS "ParentId", 
"BpCode2", "BpType", "CardName2", "Comment", 
MAP("LineProperty" ,0, NULL, "BPLId") AS "BPLId"

FROM :tmpresult6 T0 ORDER BY  "CardCode","LetterFrmt","DocCur", "LetterNum", "BPLId", "DunnLevel", "ObjType" ,"DocNum" ,"InstlmntID" ,"DueDate" 	, "DocAmountLC", "DocAmountFC" ;

-- SELECT * FROM :tmpresult7;

tmpresult8=
SELECT 
"CheckLine" ,  "ExeChkLine" ,  "RowId"  , "CardCode" ,  "LetterNum", "DunnLevel" ,  "CardName" ,  
"DocCur" , "ObjType" , "DocNum" , 
MAP ("LineProperty", 0, 1, 
1, (CASE WHEN (SELECT COUNT(1) FROM :tmpresult7 WHERE "ParentId" = T0."RowId" AND ("DunnLevel" > 0 OR "DocAmountLCBackup">0 OR "DocAmountFCBackup" >0)) > 0 THEN 1 ELSE -1 END ),
"InstlmntID") AS "InstlmntID" , 
"DueDate" , "LastLevelUpdateDate" , "LastDunningDate" ,  "NewLvlUpdDate" , 
"DocAmountLC" , "DocAmountFC" , "OpenAmountLC" , "OpenAmountFC" , 
"InterestDays" , "Interest" , "InterestAmountLC" ,  "InterestAmountFC" ,   "TotalInclAmountLC" ,  "TotalInclAmountFC"  , "FeeLC" , "FeeFC" ,"OverallTotalLC","OverallTotalFC" ,
"AutoPost" , "LineProperty" ,  "YearDays" ,  "YearlyRate" ,  "LetterFrmt",
"OrigMinBalance",  "GrpMethod" ,  "DocEntry" , "DocRate", "FeeCurr", "OrigFee", "MinBalCurr",  "LevelUpdated", "DunAddr", "DocText",
"DocAmountLCBackup" ,"DocAmountFCBackup" ,"OpenAmountLCBackup","OpenAmountFCBackup",
"InterestAmountLCBackup" , "InterestAmountFCBackup" ,"TotalInclAmountLCBackup", "TotalInclAmountFCBackup", 
"FeeLCBackup","FeeFCBackup","OverallTotalLCBackup","OverallTotalFCBackup",
"ParentId", 
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"
FROM :tmpresult7 T0 ORDER BY  "CardCode","LetterFrmt","DocCur", "LetterNum", "BPLId", "DunnLevel", "ObjType" ,"DocNum" ,"InstlmntID" ,"DueDate" 	, "DocAmountLC", "DocAmountFC" ;

INSERT INTO TMP_DWZ_OUT_LINEPROPERTY                                                                                                        
SELECT 
"CheckLine" ,  "ExeChkLine" ,  "RowId"  , "CardCode" ,  "LetterNum", "DunnLevel" ,  "CardName" ,  
"DocCur" , "ObjType" , "DocNum" , 
MAP ("LineProperty", 999, 
(CASE WHEN (SELECT "InstlmntID" FROM :tmpresult8 WHERE "RowId" = T0."ParentId") = -1 THEN -1 ELSE "InstlmntID" END ) , "InstlmntID") AS "InstlmntID" , 
"DueDate" , "LastLevelUpdateDate" , "LastDunningDate" ,  "NewLvlUpdDate" , 
"DocAmountLC" , "DocAmountFC" , "OpenAmountLC" , "OpenAmountFC" , 
"InterestDays" , "Interest" , "InterestAmountLC" ,  "InterestAmountFC" ,   "TotalInclAmountLC" ,  "TotalInclAmountFC"  , "FeeLC" , "FeeFC" ,"OverallTotalLC","OverallTotalFC" ,
"AutoPost" , 
MAP("LineProperty" , 0, 1, 1, 2, 999, 3) AS "LineProperty" ,  
"YearDays" ,  "YearlyRate" ,  "LetterFrmt",
"OrigMinBalance",  "GrpMethod" ,  "DocEntry" , "DocRate", "FeeCurr", "OrigFee", "MinBalCurr",  "LevelUpdated", "DunAddr", "DocText",
"DocAmountLCBackup" ,"DocAmountFCBackup" ,"OpenAmountLCBackup","OpenAmountFCBackup",
"InterestAmountLCBackup" , "InterestAmountFCBackup" ,"TotalInclAmountLCBackup", "TotalInclAmountFCBackup", 
"FeeLCBackup","FeeFCBackup","OverallTotalLCBackup","OverallTotalFCBackup",
"ParentId",
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"
FROM :tmpresult8 T0 ORDER BY  "CardCode","LetterFrmt","DocCur", "LetterNum", "BPLId", "DunnLevel", "ObjType" ,"DocNum" ,"InstlmntID" ,"DueDate" 	, "DocAmountLC", "DocAmountFC" ;

-- Insert tree structure of result into SPPLA 
DELETE FROM SPPLA WHERE "PagingID" = :pagingID;

insert into sppla
select :pagingID ,"RowId","LineProperty"+1,
map("LineProperty", 
1, LEAD("RowId") over (partition by map("LineProperty",0,1,"LineProperty") order by "RowId") - "RowId",
0, LEAD("RowId") over (partition by  "LineProperty" order by "RowId") - "RowId"
) -1 as ChildCount
 from :tmpresult8 where "LineProperty" <>999 order by "RowId" ; 
	-- Calculate children number of last Row 
update sppla set "ChildrenNo" = (select max ("RowId") from TMP_DWZ_OUT_LINEPROPERTY )-"RowID" where "ChildrenNo" is null ;

-- Currency
-- SELECT foreign Currency which doesn't have exchange rate in dunning date into SPPP as output
forCurrTable =	
SELECT T0."Currency" AS "Currency" FROM 
( select "Currency" AS "Currency" from 
	( select "DocCur" as "Currency"  from :tmpresult5 union select "FeeCurr"  from :tmpresult5 union select "MinBalCurr"  from :tmpresult5 ) where "Currency" <> :mainCurncy and "Currency" is not null ) T0 
LEFT JOIN ( select "Currency", "RateDate","Rate" from ORTT where "RateDate" = :dunningDate ) T1 ON T0."Currency" = T1."Currency"
WHERE  T1."Rate" IS NULL ;	

foreignCurrency := '';
SELECT COUNT (1) INTO forCurrNum FROM :forCurrTable ;
IF :forCurrNum > 0 THEN
	DECLARE CURSOR F_C FOR SELECT * FROM :forCurrTable ;
	rowidx := 0;
	FOR F as F_C DO 
		if rowidx > 0 then 
			foreignCurrency := foreignCurrency || ',';
		end if;
		foreignCurrency := foreignCurrency || F."Currency";
		rowidx := rowidx + 1;
	END FOR;
END IF ;
--select :foreignCurrency from dummy;

DELETE FROM SPPP WHERE "PagingID" = :pagingID AND "Name" = 'ForeignCurrency';
IF LENGTH(:foreignCurrency) >0 THEN 
	INSERT INTO SPPP VALUES (:pagingID, 'ForeignCurrency', :foreignCurrency, '2' );
END IF;

END;











