-- B1 DEPENDS: BEFORE:PT:PROCESS_START

CREATE PROCEDURE DWZ_CREATE_OBJECTS ()
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
cnt int;
BEGIN
	SELECT COUNT(*) INTO cnt FROM "PUBLIC".TABLES WHERE TABLE_NAME = 'DWZ_REPORT_OUT' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP TYPE DWZ_REPORT_OUT CASCADE');
	end if;
	
	EXEC('
		CREATE TYPE DWZ_REPORT_OUT AS TABLE(
			"CardCode" NVARCHAR(15),"CardName" NVARCHAR(100),"DunningCode" NVARCHAR(25), "DocCur" NVARCHAR(3), "DunnLevel" INTEGER,"ObjType" NVARCHAR(20),"DocNum" INTEGER,"InstlmntID" INTEGER,
			"DueDate" TIMESTAMP,"LastLevelUpdateDate" TIMESTAMP,"LastDunningDate" TIMESTAMP, "NewLvlUpdDate" TIMESTAMP,
			"DocAmountLC" DECIMAL(21,6),"DocAmountFC" DECIMAL(21,6),"OpenAmountLC" DECIMAL(21,6),"OpenAmountFC" DECIMAL(21,6),"InterestDays" INTEGER,"Interest" DECIMAL(21,6),"InterestAmountLC" DECIMAL(21,6), "InterestAmountFC" DECIMAL(21,6),
			"GrpMethod" NVARCHAR(1) , "FeeLC" DECIMAL(21,6), "FeeFC" DECIMAL(21,6), "AutoPost" NVARCHAR(1) , "YearDays" SMALLINT, "YearlyRate" DECIMAL(21,6), "LetterFrmt" NVARCHAR(8) ,
			"OrigMinBalance" DECIMAL(21,6), "MinBalance" DECIMAL(21,6), "DocEntry" INTEGER, "DocRate" DECIMAL(21,6), "FeeCurr" NVARCHAR(3), "OrigFee" DECIMAL(21,6), "MinBalCurr" NVARCHAR(3), "LevelUpdated" NVARCHAR(1), 	 
			"BpCode2" NVARCHAR(15) CS_STRING, "BpType" NVARCHAR(1) CS_FIXEDSTRING, "CardName2" NVARCHAR(100) CS_STRING,	 "Comment" NVARCHAR(254) CS_STRING, "BPLId" INTEGER)
	');
	
	SELECT COUNT(*) INTO cnt FROM "PUBLIC".TABLES WHERE TABLE_NAME = 'TMP_DWZ_OUT_LINEPROPERTY' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP TABLE TMP_DWZ_OUT_LINEPROPERTY CASCADE');
	end if;

	EXEC('
		CREATE GLOBAL TEMPORARY TABLE "TMP_DWZ_OUT_LINEPROPERTY" ( "CheckLine" NVARCHAR(1) CS_FIXEDSTRING,
	 "ExeChkLine" NVARCHAR(1) CS_FIXEDSTRING,
	 "RowId" INT CS_INT,
	 "CardCode" NVARCHAR(15) CS_STRING,
	 "LetterNum" INT CS_INT,
	 "DunnLevel" INT CS_INT,
	 "CardName" NVARCHAR(100) CS_STRING,
	 "DocCur" NVARCHAR(3) CS_STRING,
	 "ObjType" NVARCHAR(20) CS_STRING,
	 "DocNum" INT CS_INT,
	 "InstlmntID" INT CS_INT,
	 "DueDate" LONGDATE CS_LONGDATE,
	 "LastLevelUpdateDate" LONGDATE CS_LONGDATE,
	 "LastDunningDate" LONGDATE CS_LONGDATE,
	 "NewLvlUpdDate" LONGDATE CS_LONGDATE,
	 "DocAmountLC" NVARCHAR(40),
	 "DocAmountFC" NVARCHAR(40),
	 "OpenAmountLC" NVARCHAR(40),
	 "OpenAmountFC" NVARCHAR(40),
	 "InterestDays" INT CS_INT,
	 "Interest" DECIMAL(21,6),
	 "InterestAmountLC" NVARCHAR(40),
	 "InterestAmountFC" NVARCHAR(40),
	 "TotalInclAmountLC" NVARCHAR(40),
	 "TotalInclAmountFC" NVARCHAR(40),
	 "FeeLC" NVARCHAR(40),
	 "FeeFC" NVARCHAR(40),
	 "OverallTotalLC" NVARCHAR(40),
	 "OverallTotalFC" NVARCHAR(40),
	 "AutoPost" NVARCHAR(1) CS_FIXEDSTRING,
	 "LineProperty" INT CS_INT,
	 "YearDays" SMALLINT CS_INT,
	 "YearlyRate" DECIMAL(21,6),
	 "LetterFrmt" NVARCHAR(20) CS_STRING,
	 "OrigMinBalance" NVARCHAR(40),
	 "GrpMethod" NVARCHAR(1) CS_FIXEDSTRING,
	 "DocEntry" INT CS_INT,	 
	 "DocRate" NVARCHAR(40),
	 "FeeCurr" NVARCHAR(3),
	 "OrigFee" NVARCHAR(40),  
	 "MinBalCurr" NVARCHAR(3),
	 "LevelUpdated" NVARCHAR(1),
	 "DunAddr" NVARCHAR(254),
	 "DocText" NVARCHAR(254),
	 "DocAmountLCBackup"  DECIMAL(21,6),
	 "DocAmountFCBackup"  DECIMAL(21,6),
	 "OpenAmountLCBackup" DECIMAL(21,6),
	 "OpenAmountFCBackup" DECIMAL(21,6),
	 "InterestAmountLCBackup"  DECIMAL(21,6), 
	 "InterestAmountFCBackup"  DECIMAL(21,6),
	 "TotalInclAmountLCBackup" DECIMAL(21,6), 
	 "TotalInclAmountFCBackup" DECIMAL(21,6), 
	 "FeeLCBackup" DECIMAL(21,6),
	 "FeeFCBackup" DECIMAL(21,6),
	 "OverallTotalLCBackup" DECIMAL(21,6),
	 "OverallTotalFCBackup" DECIMAL(21,6),
	 "ParentId" INT CS_INT,
 	 "BpCode2" NVARCHAR(15),
	 "BpType" NVARCHAR(1) CS_FIXEDSTRING,
	 "CardName2" NVARCHAR(100) CS_STRING,
	 "Comment" NVARCHAR(254) CS_STRING,
	 "BPLId" INT CS_INT
	 ) 
	');
	
	SELECT COUNT(*) INTO cnt FROM "PUBLIC".TABLES WHERE TABLE_NAME = 'TMP_DWZ_CRD' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP TABLE TMP_DWZ_CRD CASCADE');
	end if;

	EXEC('
		CREATE GLOBAL TEMPORARY TABLE "TMP_DWZ_CRD"
		(
			"CardCode" NVARCHAR(15)
		)
	');
		
END;











