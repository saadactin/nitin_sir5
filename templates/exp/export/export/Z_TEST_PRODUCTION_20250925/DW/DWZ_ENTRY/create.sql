-- B1 DEPENDS: AFTER:SP:DWZ_GETTOTAL AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

CREATE PROCEDURE DWZ_ENTRY (
in_nvc_pagingID NVARCHAR(100),
in_i_dataMgrFlag INTEGER -- 0 Drop, 1 Create, 2 Retrieve
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
loadType NVARCHAR(1);
v_tableName NVARCHAR(200);
cnt INTEGER;
V_SQL NVARCHAR(5000);
vMaxRowId INTEGER;
localCurrency NVARCHAR(3);
wizradId INTEGER;
ovrDueOnly NVARCHAR(1);
allowNegLt NVARCHAR(1);
dunningDate TIMESTAMP;
BEGIN 

--For HANA security issue call procedure "_TmSp_ValidateSpParam"
call _TmSp_ValidateSpParam(:in_nvc_pagingID);  
	
SELECT TO_TIMESTAMP("Value", 'YYYYMMDD') INTO dunningDate FROM "SPPP" WHERE "PagingID" = :in_nvc_pagingID AND "Name" = 'dunningDate';

v_tableName := UPPER('#TMP_' || :in_nvc_pagingID);

IF :in_i_dataMgrFlag = 1 OR :in_i_dataMgrFlag = 0 THEN
    select count (*) into cnt from "PUBLIC"."M_TEMPORARY_TABLES" where TABLE_NAME = :v_tableName AND SCHEMA_NAME = CURRENT_SCHEMA;
    if :cnt > 0 then  EXEC ('drop table ' || :v_tableName);
	end if;
END IF;
    
IF :in_i_dataMgrFlag = 1 THEN
---------------------------Logic---------------------------------------
----------------Create table ' || :v_tableName || --------------------
EXEC 'CREATE LOCAL TEMPORARY ROW TABLE ' || :v_tableName || ' ( 
"CheckLine" NVARCHAR(1), "ExeChkLine" NVARCHAR(1),"RowId" INTEGER, 
"CardCode" NVARCHAR(15), "LetterNum" INTEGER, "DunnLevel" INTEGER, "CardName" NVARCHAR(100), "DocCur" NVARCHAR(3),
"ObjType" NVARCHAR(20),"DocNum" INTEGER,"InstlmntID" INTEGER,
"DueDate" TIMESTAMP,"LastLevelUpdateDate" TIMESTAMP,"LastDunningDate" TIMESTAMP, "NewLvlUpdDate" TIMESTAMP,
"DocAmountLC" NVARCHAR(40),"DocAmountFC" NVARCHAR(40),"OpenAmountLC" NVARCHAR(40),"OpenAmountFC" NVARCHAR(40),
"InterestDays" INTEGER,"Interest" DECIMAL(21,6),
"InterestAmountLC" NVARCHAR(40), "InterestAmountFC" NVARCHAR(40),  "TotalInclAmountLC" NVARCHAR(40), "TotalInclAmountFC" NVARCHAR(40), 
"FeeLC" NVARCHAR(40), "FeeFC" NVARCHAR(40), "OverallTotalLC" NVARCHAR(40), "OverallTotalFC" NVARCHAR(40), 
"AutoPost" NVARCHAR(1),
"LineProperty" INTEGER, "YearDays" SMALLINT, "YearlyRate" DECIMAL(21,6), "LetterFrmt" NVARCHAR(20), "MinBalance" DECIMAL(21,6), "GrpMethod" NVARCHAR(1), "DocEntry" INTEGER, 
"DocRate" NVARCHAR(40), "FeeCurr" NVARCHAR(3), "OrigFee" DECIMAL(21,6), "MinBalCurr" NVARCHAR(3),"LevelUpdated" NVARCHAR(1), "DunAddr" NVARCHAR(254), "DocText" NVARCHAR(254),
"DocAmountLCBackup" DECIMAL(21,6),"DocAmountFCBackup" DECIMAL(21,6),"OpenAmountLCBackup" DECIMAL(21,6),"OpenAmountFCBackup" DECIMAL(21,6),
"InterestAmountLCBackup" DECIMAL(21,6), "InterestAmountFCBackup" DECIMAL(21,6),  "TotalInclAmountLCBackup" DECIMAL(21,6), "TotalInclAmountFCBackup" DECIMAL(21,6), 
"FeeLCBackup" DECIMAL(21,6), "FeeFCBackup" DECIMAL(21,6), "OverallTotalLCBackup" DECIMAL(21,6), "OverallTotalFCBackup" DECIMAL(21,6),
"ParentId" INTEGER, "BpCode2" NVARCHAR(15), "BpType" NVARCHAR(1), "CardName2" NVARCHAR(100), "Comment" NVARCHAR(254), "BPLId" INTEGER
 ) ';
 
 END IF;
 
EXEC ('DELETE from ' || :v_tableName || ' where 1=1');

SELECT "Value" INTO loadType FROM "SPPP" WHERE "PagingID" = :in_nvc_pagingID AND "Name" = 'loadType';
SELECT TO_INT("Value") INTO wizradId FROM "SPPP" WHERE "PagingID" = :in_nvc_pagingID AND "Name" = 'wizardID';

IF :loadType = '2' THEN 
EXEC ('INSERT INTO ' || :v_tableName || ' SELECT "CheckLine" , "ExeChkLine", "RowId" , "CardCode" , "LetterNum" , "DunnLevel" , "CardName" , "DocCur" ,
"DocType" ,"DocNum" ,"InstlmntID" ,
"DueDate" ,"LastLvlDte" ,"LastDunDte" , "NewLvlDate" ,

"DocAmntLC","DocAmntFC","OpenAmtLC" ,"OpenAmtFC" ,
"IntrstDays" ,"IntrstPC" ,
"IntAmntLC" , "IntAmntFC" ,  "InclAmntLC" , "InclAmntFC" , 
"FeeLC" , "FeeFC" , "AllTotalLC" , "AllTotalFC" , 

"AutoPost" ,
"LineProp" , "YearDays" , "YearlyRate" , "LetterFrmt" , "MinBlan"  , "GrpMethod" , "DocEntry" , "DocRate", "FeeCurr", "OrigFee", "MinBalCurr", "LvlUpdated", "DunAddr", "DocText", 
"DocAmntLC" ,"DocAmntFC" ,"OpenAmtLC" ,"OpenAmtFC" ,
"IntAmntLC" , "IntAmntFC" ,  "InclAmntLC" , "InclAmntFC" , 
"FeeLC" , "FeeFC" , "AllTotalLC" , "AllTotalFC" , 
"ParentId",
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"

FROM  DWZ4 WHERE "WizardId" = ' || :wizradId ) ;

ELSE
    
IF :in_i_dataMgrFlag = 1 THEN
CALL DWZ_GETTOTAL(in_nvc_pagingID);
-- Tips: row_number () over (PARTITION BY "CardCode", "LineProperty" )  AS "LetterNum" -- calculate LetterNum
EXEC ( 'INSERT INTO ' || :v_tableName || ' SELECT "CheckLine", "ExeChkLine"  ,"RowId" ,"CardCode"  ,"LetterNum" ,"DunnLevel" ,"CardName"  ,"DocCur"  ,"ObjType"  ,"DocNum" ,"InstlmntID" ,"DueDate"  ,
"LastLevelUpdateDate"  ,"LastDunningDate"  ,"NewLvlUpdDate"  ,
"DocAmountLC" ,"DocAmountFC" ,"OpenAmountLC" ,"OpenAmountFC" ,
"InterestDays" ,"Interest"  ,
"InterestAmountLC" ,"InterestAmountFC" ,"TotalInclAmountLC" ,"TotalInclAmountFC" ,
"FeeLC" ,"FeeFC" ,"OverallTotalLC" ,"OverallTotalFC" ,
"AutoPost"  ,"LineProperty" ,"YearDays" ,"YearlyRate" ,"LetterFrmt"  ,"OrigMinBalance" ,"GrpMethod"  ,"DocEntry" ,	 "DocRate" ,"FeeCurr" ,"OrigFee" ,  "MinBalCurr" ,"LevelUpdated" ,"DunAddr" ,"DocText" ,
"DocAmountLCBackup"  ,"DocAmountFCBackup"  ,"OpenAmountLCBackup" ,"OpenAmountFCBackup" ,
"InterestAmountLCBackup"  , "InterestAmountFCBackup"  ,"TotalInclAmountLCBackup" , "TotalInclAmountFCBackup" , 
"FeeLCBackup" ,"FeeFCBackup" ,"OverallTotalLCBackup" ,"OverallTotalFCBackup" ,
"ParentId", 
"BpCode2", "BpType", "CardName2", "Comment", "BPLId"
FROM TMP_DWZ_OUT_LINEPROPERTY ' );
END IF;

END IF;
 
--EXEC (:V_SQL);
      
END;











