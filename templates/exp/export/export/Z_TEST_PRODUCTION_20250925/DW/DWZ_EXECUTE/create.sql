-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

CREATE PROCEDURE DWZ_EXECUTE ( 
IN entry NVARCHAR(1),
IN pagingId NVARCHAR(100),
IN dunDateChar	NVARCHAR(10),
IN wizardId INTEGER
)LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
tableName NVARCHAR(200);
V_SQL NVARCHAR(5000);
dunDate TIMESTAMP;
IsExists INTEGER;
recOffset INTEGER;
CURSOR dwz4Cursor FOR SELECT "WizardId", "RowId" FROM DWZ4 WHERE "WizardId"=:wizardId ORDER BY "RowId";
BEGIN 

--For HANA security issue call procedure "_TmSp_ValidateSpParam"
call _TmSp_ValidateSpParam(:entry);
call _TmSp_ValidateSpParam(:pagingId);
call _TmSp_ValidateSpParam(:dunDateChar);

tableName := UPPER('#TMP_' || :pagingId);
dunDate := TO_TIMESTAMP (:dunDateChar, 'YYYYMMDD');

IF :entry = 'L' THEN

-- lock invoice
--UPDATE "INV6" T0 SET T0."DunWizBlck"='Y' FROM "INV6" T0 INNER JOIN tmp_1000 T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE "CheckLine" = 'Y';
--EXEC (' UPDATE "INV6" T0 SET T0."DunWizBlck"=''Y'' FROM "INV6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE "CheckLine" = ''N'' ');

EXEC(' UPDATE "INV6" T0 SET T0."DunWizBlck"=''Y'' FROM "INV6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''13'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "DPI6" T0 SET T0."DunWizBlck"=''Y'' FROM "DPI6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''203'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "RIN6" T0 SET T0."DunWizBlck"=''Y'' FROM "RIN6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''14'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "CSI6" T0 SET T0."DunWizBlck"=''Y'' FROM "CSI6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''165'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "JDT1" T0 SET T0."DunWizBlck"=''Y'' FROM "JDT1" T0 INNER JOIN ' || :tableName || ' T1 ON T0."TransId" = T1."DocEntry" AND T0."Line_ID" = T1."InstlmntID" WHERE T1."ObjType" = ''30'' AND T1."CheckLine" = ''N'' ');

EXEC(' UPDATE "ORCT" T0 SET T0."WizDunBlck"=''Y'' FROM "ORCT" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" 
INNER JOIN "JDT1" T2 ON T2."TransId" = T0."TransId" AND T2."TransType" = ''24'' WHERE T2."Line_ID" = T1."InstlmntID" AND T1."CheckLine" = ''N'' ' );
-- VENDOR DOC
EXEC(' UPDATE "PCH6" T0 SET T0."DunWizBlck"=''Y'' FROM "PCH6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''18'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "DPO6" T0 SET T0."DunWizBlck"=''Y'' FROM "DPO6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''204'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "RPC6" T0 SET T0."DunWizBlck"=''Y'' FROM "RPC6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''19'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "CPI6" T0 SET T0."DunWizBlck"=''Y'' FROM "CPI6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''163'' AND T1."CheckLine" = ''N'' ');

END IF;

IF :entry = 'U' THEN
-- UPDATE "DunnLevel" of INV6
--UPDATE "INV6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", 'N', TO_TIMESTAMP('20131108', 'YYYYMMDD'), T1."NewLvlUpdDate")
--FROM "INV6" T0 INNER JOIN tmp_1000 T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE "CheckLine" = 'Y';
--EXEC (' UPDATE "INV6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ' || :dunDate || ' , T1."NewLvlUpdDate")
--FROM "INV6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE "CheckLine" = ''Y'' ');

EXEC( ' UPDATE "INV6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
FROM "INV6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''13'' AND T1."CheckLine" = ''N'' AND T1."LevelUpdated" = ''Y'' ' );
EXEC( ' UPDATE "DPI6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
FROM "DPI6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''203'' AND T1."CheckLine" = ''N'' AND T1."LevelUpdated" = ''Y'' ' );
EXEC( ' UPDATE "RIN6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
FROM "RIN6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''14'' AND T1."CheckLine" = ''N'' AND T1."LevelUpdated" = ''Y'' ' );
--EXEC( ' UPDATE "RCT6" T0 SET (T0."DunnLevel",T0."WizDunBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
--FROM "RCT6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''24'' AND T1."CheckLine" = ''N'' ' );
EXEC( ' UPDATE "CSI6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
FROM "CSI6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''165'' AND T1."CheckLine" = ''N'' AND T1."LevelUpdated" = ''Y'' ' );
EXEC( ' UPDATE "JDT1" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
FROM "JDT1" T0 INNER JOIN ' || :tableName || ' T1 ON T0."TransId" = T1."DocEntry" AND T0."Line_ID" = T1."InstlmntID" WHERE T1."ObjType" = ''30'' AND T1."CheckLine" = ''N'' AND T1."LevelUpdated" = ''Y'' ' );
--VENDOR DOC
EXEC( ' UPDATE "PCH6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
FROM "PCH6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''18'' AND T1."CheckLine" = ''N'' AND T1."LevelUpdated" = ''Y'' ' );
EXEC( ' UPDATE "DPO6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
FROM "DPO6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''204'' AND T1."CheckLine" = ''N'' AND T1."LevelUpdated" = ''Y'' ' );
EXEC( ' UPDATE "RPC6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
FROM "RPC6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''19'' AND T1."CheckLine" = ''N'' AND T1."LevelUpdated" = ''Y'' ' );
EXEC( ' UPDATE "CPI6" T0 SET (T0."DunnLevel",T0."DunWizBlck",T0."DunDate",T0."LvlUpdDate") = (T1."DunnLevel", ''N'', ''' || :dunDate || ''' ,  IFNULL (T1."NewLvlUpdDate",T0."LvlUpdDate") )
FROM "CPI6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''163'' AND T1."CheckLine" = ''N'' AND T1."LevelUpdated" = ''Y'' ' );

END IF;

IF :entry = 'R' THEN
-- release invoice
-- EXEC(' UPDATE "INV6" T0 SET T0."DunWizBlck"=''N'' FROM "INV6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE "CheckLine" = ''N'' ');

EXEC(' UPDATE "INV6" T0 SET T0."DunWizBlck"=''N'' FROM "INV6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''13'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "DPI6" T0 SET T0."DunWizBlck"=''N'' FROM "DPI6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''203'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "RIN6" T0 SET T0."DunWizBlck"=''N'' FROM "RIN6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''14'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "CSI6" T0 SET T0."DunWizBlck"=''N'' FROM "CSI6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''165'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "JDT1" T0 SET T0."DunWizBlck"=''N'' FROM "JDT1" T0 INNER JOIN ' || :tableName || ' T1 ON T0."TransId" = T1."DocEntry" AND T0."Line_ID" = T1."InstlmntID" WHERE T1."ObjType" = ''30'' AND T1."CheckLine" = ''N'' ');

EXEC(' UPDATE "ORCT" T0 SET T0."WizDunBlck"=''N'' FROM "ORCT" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" 
INNER JOIN "JDT1" T2 ON T2."TransId" = T0."TransId" AND T2."TransType" = ''24'' WHERE T2."Line_ID" = T1."InstlmntID" AND T1."CheckLine" = ''N'' ' );
--VENDOR DOC
EXEC(' UPDATE "PCH6" T0 SET T0."DunWizBlck"=''N'' FROM "PCH6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''18'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "DPO6" T0 SET T0."DunWizBlck"=''N'' FROM "DPO6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''204'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "RPC6" T0 SET T0."DunWizBlck"=''N'' FROM "RPC6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''19'' AND T1."CheckLine" = ''N'' ');
EXEC(' UPDATE "CPI6" T0 SET T0."DunWizBlck"=''N'' FROM "CPI6" T0 INNER JOIN ' || :tableName || ' T1 ON T0."DocEntry" = T1."DocEntry" AND T0."InstlmntID" = T1."InstlmntID" WHERE T1."ObjType" = ''163'' AND T1."CheckLine" = ''N'' ');

END IF;

IF :entry = 'I' THEN
-- insert DWZ4
	SELECT COUNT(*) INTO IsExists FROM "DWZ4" WHERE "WizardId" = :wizardId;
	IF :IsExists = 0 THEN
		EXEC( ' INSERT INTO "DWZ4" ("WizardId" , "CheckLine" , "ExeChkLine" , "RowId" , "CardCode" , "LetterNum" , "DunnLevel" , "CardName" , "DocCur" , "DocType" , "DocNum" , "InstlmntID" , "DueDate" , "LastLvlDte" , "LastDunDte" , "NewLvlDate" , "DocAmntLC" , "DocAmntFC" , "OpenAmtLC" , "OpenAmtFC" , "IntrstDays" , "IntrstPC" , "IntAmntLC" , "IntAmntFC" , "InclAmntLC" , "InclAmntFC" , "FeeLC" , "FeeFC" , "AllTotalLC" , "AllTotalFC" , "AutoPost" , "LineProp" , "YearDays" , "YearlyRate" , "LetterFrmt" , "MinBlan" , "GrpMethod" , "DocEntry" , "DocRate" , "FeeCurr" , "OrigFee" , "MinBalCurr" , "LvlUpdated" , "DunAddr" , "DocText" , "ParentId", "BpCode2", "BpType", "CardName2", "Comment", "BPLId" ) 
		SELECT ' || :wizardId || ' ,
		"CheckLine" , "ExeChkLine" ,"RowId" , 
		"CardCode" , "LetterNum" , "DunnLevel" , "CardName" , "DocCur" ,
		"ObjType" ,"DocNum" ,"InstlmntID" ,
		"DueDate" ,"LastLevelUpdateDate" ,"LastDunningDate" , "NewLvlUpdDate" ,
		"DocAmountLCBackup" ,"DocAmountFCBackup" ,"OpenAmountLCBackup" ,"OpenAmountFCBackup" ,
		"InterestDays" ,"Interest" ,
		"InterestAmountLCBackup" , "InterestAmountFCBackup" ,  "TotalInclAmountLCBackup" , "TotalInclAmountFCBackup" , 
		"FeeLCBackup" , "FeeFCBackup" , "OverallTotalLCBackup" , "OverallTotalFCBackup" ,
		"AutoPost" ,
		"LineProperty" , "YearDays" , "YearlyRate" , "LetterFrmt" , "MinBalance"  , "GrpMethod" , "DocEntry" , 
		"DocRate" , "FeeCurr" , "OrigFee" , "MinBalCurr" ,"LevelUpdated" , "DunAddr" , "DocText" ,
		"ParentId", "BpCode2", "BpType", "CardName2", "Comment", "BPLId"
		FROM ' || :tableName || ' WHERE "CheckLine" = ''N'' ORDER BY "RowId"');
		
INSERT INTO DWZ2 
("WizardId" , "CardCode" , "LetterNum" , "TotalFee" , "FeeCurr" , "TtlopnIntr" , 
"EDunLevel" , "OpnIntrCrr" , "DocAbs" , "DocNum" , "InstlmntID" , "IntrstPC" , 
"IntrstAmnt" , "IntrstCurr" , "ChckLine" , "IntrstDays" , "DocType" , "DueDate" , 
"LetterLvl" , "OpenSum" , "OpenCurr" , "sumIntrClc" , "folioNum" , "LvlUpdated" , 
"TotalFeeFC" , "TotalFeeLC" , "TotalFeeSC" , "TtlopnInFC" , "TtlopnInSC" , "IntrAmtFC" , 
"IntrAmtSC" , "OpenSumFC" , "OpenSumSC" , "ExeChkLine" , "AutoPost" , "DocRate" , 
"UnpaidBoEV" , "BoENumber" , "BoEStatus" , "BoEDate" , "BoEKey", "BPType", "BPLId")
SELECT 
"WizardId", 
"CardCode", 
(SELECT "LetterNum" FROM DWZ4 WHERE "RowId" = T0."ParentId" AND  "WizardId" = :wizardId ) AS "LetterNum" , 
"FeeLC",
"FeeCurr",
"InclAmntLC",
"DunnLevel",
"DocCur",
"DocEntry",
"DocNum",
"InstlmntID",
"IntrstPC",
"IntAmntLC",
"DocCur",
"CheckLine",
"IntrstDays",
"DocType",
"DueDate",
"DunnLevel",
"OpenAmtLC",
"DocCur",
NULL,
NULL,
"LvlUpdated",
(SELECT "FeeFC" FROM DWZ4 WHERE "RowId" = T0."ParentId" AND  "WizardId" = :wizardId ) as "FeeFC",
(SELECT "FeeLC" FROM DWZ4 WHERE "RowId" = T0."ParentId" AND  "WizardId" = :wizardId ) as "FeeLC",
NULL,
"InclAmntFC",
NULL,
"IntAmntFC",
NULL,
"OpenAmtFC",
NULL,
"ExeChkLine",
(SELECT "AutoPost" FROM DWZ4 WHERE "RowId" = T0."ParentId" AND  "WizardId" = :wizardId ) as "AutoPost",
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
T0."BpType",
"BPLId"
FROM DWZ4 T0 WHERE "WizardId" = :wizardId AND "LineProp" = 3 order by "RowId";

	END IF;	
END IF;

IF :entry = 'E' THEN
-- Update DWZ4."ExeChkLine"
-- Need validation 'Update' here !
EXEC( ' UPDATE DWZ4 T0 SET T0."ExeChkLine" = ''Y'' FROM DWZ4 T0 INNER JOIN DWZ3 T1 ON T0."WizardId" = T1."WizardId" AND T0."CardCode" = T1."CardCode"
WHERE T0."WizardId" = ' || :wizardId || ' AND T1."LetterNum" = (SELECT "LetterNum"  FROM DWZ4 WHERE "WizardId" = T0."WizardId" AND "RowId" = T0."ParentId")');
END IF;

-- Fix the disorder of RowIds and ParentIds of DWZ4 due to discontinuous selection of records
recOffset:=1;
FOR e AS dwz4Cursor DO
	UPDATE DWZ4 SET "ParentId"=recOffset WHERE "WizardId"=e."WizardId" AND "ParentId"=e."RowId";
	UPDATE DWZ4 SET "RowId"=recOffset WHERE "WizardId"=e."WizardId" AND "RowId"=e."RowId";
	recOffset:=recOffset+1;
END FOR; 

END;











