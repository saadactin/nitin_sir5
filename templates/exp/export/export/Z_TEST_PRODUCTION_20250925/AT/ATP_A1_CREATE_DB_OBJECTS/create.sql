-- B1 DEPENDS: BEFORE:PT:PROCESS_START AFTER:SP:ATP_A0_CREATE_DB_TYPES
CREATE PROCEDURE ATP_A1_CREATE_DB_OBJECTS ()
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
cnt int;
BEGIN
	SELECT COUNT(*) INTO cnt FROM "PUBLIC".TABLES WHERE TABLE_NAME = 'ATP_CHECKS' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP TABLE ATP_CHECKS CASCADE');
	end if;

	EXEC('
		CREATE COLUMN TABLE ATP_CHECKS(
	       "CheckID" INTEGER,
	       "ItemCode" NVARCHAR(50),
	       "WhsCode" NVARCHAR(8),
	       "Date" TIMESTAMP,
	       "Qty" DECIMAL(21,6),
    	 PRIMARY KEY ("CheckID")
		)
	');

	SELECT COUNT(*) INTO cnt FROM "PUBLIC".SEQUENCES WHERE SEQUENCE_NAME = 'ATP_CHECK_ID' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP SEQUENCE ATP_CHECK_ID CASCADE');
	end if;

	EXEC('
		CREATE SEQUENCE ATP_CHECK_ID
	');

	SELECT COUNT(*) INTO cnt FROM "PUBLIC".TABLES WHERE TABLE_NAME = 'OTQA' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP TABLE OTQA CASCADE');
	end if;

	EXEC('
		CREATE HISTORY COLUMN TABLE OTQA(
			"CheckID" INTEGER,
			"ObjType" NVARCHAR (20),
			"DocEntry" INTEGER,
			"DocLineNum" INTEGER,
			"SchdLine" INTEGER,
			"TQAType" SMALLINT, -- 0 normal TQA, 1 invisible TQA (confirmations are already covered by existing OSLD entries)
			"ItemCode" NVARCHAR(50),
			"WhsCode" NVARCHAR(8),
			"CfmDate" TIMESTAMP,
			"CfmQty" DECIMAL(21,6),
			"ReqQty" DECIMAL(21,6),
			-- it is not necessary to add the transaction time, we can use time travel to get this information
			PRIMARY KEY ("CheckID", "ObjType", "DocEntry", "DocLineNum", "SchdLine","TQAType")
		)
	');

	SELECT COUNT(*) INTO cnt FROM "PUBLIC".TABLES WHERE TABLE_NAME = 'ATP_LOCK' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP TABLE ATP_LOCK CASCADE');
	end if;

	EXEC('
		CREATE ROW TABLE ATP_LOCK(
		    "ItemCode" NVARCHAR(50),
		    "WhsCode" NVARCHAR(8),
		    PRIMARY KEY("ItemCode", "WhsCode")
		)
	');

	SELECT COUNT(*) INTO cnt FROM "PUBLIC".TABLES WHERE TABLE_NAME = 'ODON' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP TABLE ODON CASCADE');
	end if;

	EXEC('
		CREATE GLOBAL TEMPORARY TABLE ODON(
			"ObjType" NVARCHAR (20),
			"DocEntry" INTEGER,
			"DocLineNum" INTEGER,
			"ItemCode" NVARCHAR(50),
			"WhsCode" NVARCHAR(8),
			"CfmDate" TIMESTAMP,
			"DonQty" DECIMAL(21,6),
			"SequenceID" Integer)
	');
	
	SELECT COUNT(*) INTO cnt FROM "PUBLIC".TABLES WHERE TABLE_NAME = 'OCAN' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP TABLE OCAN CASCADE');
	end if;
	
	EXEC('
		CREATE GLOBAL TEMPORARY TABLE OCAN(
			"ObjType" NVARCHAR (20),
			"DocEntry" INTEGER,
			"DocLineNum" INTEGER,
			"CanQty" DECIMAL(21,6),
			"date" DATE,
			"SequenceID" Integer)
	');
	
	SELECT COUNT(*) INTO cnt FROM "PUBLIC".TABLES WHERE TABLE_NAME = 'DOC_LINES_VERSIONS' AND SCHEMA_NAME = CURRENT_SCHEMA;
	if :cnt > 0 then
		exec ('DROP TABLE DOC_LINES_VERSIONS CASCADE');
	end if;
	
	EXEC('
		CREATE GLOBAL TEMPORARY TABLE DOC_LINES_VERSIONS(
			"ObjType" NVARCHAR (20),
			"DocEntry" INTEGER,
			"DocLineNum" INTEGER,
			"Version" INTEGER
			)
	');
END;











