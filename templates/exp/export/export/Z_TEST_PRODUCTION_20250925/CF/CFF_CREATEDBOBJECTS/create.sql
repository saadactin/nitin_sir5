-- B1 DEPENDS: AFTER:PT:PROCESS_END

CREATE PROCEDURE CFF_CREATEDBOBJECTS
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	IsExists INTEGER;
	SqlStr   NVARCHAR(15000);
BEGIN
	-- CFF_TMP_OVERVIEW
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_OVERVIEW' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_OVERVIEW CASCADE;
	END IF;
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_OVERVIEW (
		"Index"				INTEGER,
		"DueDate"			DATE, 
		"Debit"				DECIMAL(21,6), 
		"Credit"			DECIMAL(21,6), 
		"ObjType"			NVARCHAR(20),
		"Group"				NVARCHAR(2),
		"Number"			INTEGER
	);

	-- CFF_TMP_OVERVIEW_BA for Blanket Agreement
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_OVERVIEW_BA' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_OVERVIEW_BA CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_OVERVIEW_BA (
		"DueDate"			DATE, 
		"Debit"				DECIMAL(21,6), 
		"Credit"			DECIMAL(21,6), 
		"ObjType"			NVARCHAR(20),
		"Group"				NVARCHAR(2),
		"Number"			INTEGER
	);	

	-- CFF_TMP_RP for Recurring Posting
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_RP' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_RP CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_RP (
		"DueDate"			DATE, 
		"Debit"				DECIMAL(21,6), 
		"Credit"			DECIMAL(21,6), 
		"AcctCode"			NVARCHAR(15), 
		"CtrlAcct"			NVARCHAR(15) 
	);

	-- CFF_TMP_RP_OB for Openning Balance of Recurring Posting
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_RP_OB' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_RP_OB CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_RP_OB (
		"DueDate"			DATE, 
		"Debit"				DECIMAL(21,6), 
		"Credit"			DECIMAL(21,6), 
		"AcctCode"			NVARCHAR(15), 
		"CtrlAcct"			NVARCHAR(15) 
	);
	
	-- CFF_TMP_DETAIL
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_DETAIL' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_DETAIL CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_DETAIL (
		"DueDate"			DATE,
		"DocType"			NVARCHAR(20),
		"DocEntry"			NVARCHAR(20),
		"CtrlAcct"			NVARCHAR(15),
		"Account"			NVARCHAR(15),
		"Remarks"			NVARCHAR(50),
		"Debit"				DECIMAL(21,6), 
		"Credit"			DECIMAL(21,6), 
		"ObjType"			NVARCHAR(20),
		"Group"				NVARCHAR(2),
		"InstImnt"			INTEGER,
		"DocNum"			NVARCHAR(20)
	);

	-- CFF_TMP_DETAIL_RP for Recurring Posting
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_DETAIL_RP' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_DETAIL_RP CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_DETAIL_RP (
		"DueDate"			DATE, 
		"Debit"				DECIMAL(21,6), 
		"Credit"			DECIMAL(21,6), 
		"AcctCode"			NVARCHAR(15), 
		"CtrlAcct"			NVARCHAR(15), 
		"DocEntry"			NVARCHAR(20), 
		"Remarks"			NVARCHAR(50)
	);
		
	-- CFF_TMP_DETAIL_BA for Blanket Agreement
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_DETAIL_BA' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_DETAIL_BA CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_DETAIL_BA (
		"DueDate"			DATE,
		"DocType"			NVARCHAR(20),
		"DocEntry"			INTEGER,
		"DocNum"			INTEGER,
		"CtrlAcct"			NVARCHAR(15),
		"Account"			NVARCHAR(15),
		"Remarks"			NVARCHAR(50),
		"Debit"				DECIMAL(21,6), 
		"Credit"			DECIMAL(21,6), 
		"ObjType"			NVARCHAR(20),
		"Group"				NVARCHAR(2)
	);

	-- CFF_TMP_OCRD
	-- Purpose to filter out inactive BPs
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_OCRD' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_OCRD CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_OCRD (
		"CardCode"			NVARCHAR(15),
		"CardType"			NVARCHAR(1),
		"AvrageLate"		INTEGER
	);	

	-- CFF_TMP_DATE
	-- Used to calculate date interval
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_DATE' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_DATE CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_DATE (
		"StartDate"			DATE,
		"EndDate"			DATE,
		"Index"				INTEGER
	);	
	
	-- CFF_TMP_BOEACCT
	-- Used to store BOE account which's from OACP and BP's control account
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_BOEACCT' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_BOEACCT CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_BOEACCT (
		"AcctCode" NVARCHAR(15)
	);	

	-- CFF_TMP_OBJLIST
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_OBJLIST' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_OBJLIST CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_OBJLIST (
		"ObjType"			NVARCHAR(20)
	);
	
	-- B1_CashFlowForecastPredictDueDateDocumentsView
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".VIEWS 
	WHERE VIEW_NAME = 'B1_CashFlowForecastPredictDueDateDocumentsView' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		SqlStr := 'DROP VIEW "B1_CashFlowForecastPredictDueDateDocumentsView"';
  	    EXEC(:SqlStr);		
	END IF;	
	SqlStr := 'CREATE VIEW "B1_CashFlowForecastPredictDueDateDocumentsView"
	  AS
	  --1. Sales Order
	  SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate",
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays",
			 IFNULL(T1."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum", 
			 IFNULL(T2."IntsNo", 0)				AS "InstID",
			 IFNULL(T1."InstNum", 0)			AS "InstNum",
			 IFNULL(T2."InstMonth", 0)			AS "InstMonth",
			 IFNULL(T2."InstDays", 0)			AS "InstDays",
			 IFNULL(T2."InstPrcnt", 100)		AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
	         T0."DocTotal" - T0."PaidToDate"	AS "Debit", 
	         0 									AS "Credit",
			 ''SF''								AS "Group",
			 1 									AS "PredictFlag",
			 T3."AvrageLate",
			 IFNULL(T3."HldCode", '''')			AS "HldCode"
	    FROM "ORDR" T0
  INNER JOIN "OCTG" T1 
		  ON T1."GroupNum" = T0."GroupNum"
   LEFT JOIN "CTG1" T2 
		  ON T2."CTGCode" = T1."GroupNum"
  INNER JOIN "OCRD" T3
		  ON T3."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
	UNION ALL
	--2. Delivery
	  SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 IFNULL(T1."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
	         T0."DocTotal" - T0."PaidToDate"	AS "Debit", 
	         0									AS "Credit",
			 ''SF''								AS "Group",
			 1									AS "PredictFlag",
			 T3."AvrageLate",
			 IFNULL(T3."HldCode", '''')			AS "HldCode"
	    FROM "ODLN" T0
  INNER JOIN "OCTG" T1
		  ON T1."GroupNum" = T0."GroupNum"
  INNER JOIN "OCRD" T3
		  ON T3."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
	UNION ALL
	--3. Purchase Order
	  SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 IFNULL(T1."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T2."IntsNo", 0)				AS "InstID",
			 IFNULL(T1."InstNum", 0)			AS "InstNum",
			 IFNULL(T2."InstMonth", 0)			AS "InstMonth",
			 IFNULL(T2."InstDays", 0)			AS "InstDays",
			 IFNULL(T2."InstPrcnt", 100)		AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
	         0									AS "Debit", 
	         T0."DocTotal" - T0."PaidToDate"	AS "Credit",
			 ''EF''								AS "Group",
			 1									AS "PredictFlag",
			 T4."AvrageLate",
			 IFNULL(T3."HldCode", '''')			AS "HldCode"
	    FROM "OPOR" T0
  INNER JOIN "OCTG" T1 
		  ON T1."GroupNum" = T0."GroupNum"
   LEFT JOIN "CTG1" T2 
		  ON T2."CTGCode" = T1."GroupNum"
   LEFT JOIN "OADM" T3
		  ON T3."Code" = T3."Code"
  INNER JOIN "OCRD" T4
		  ON T4."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
	UNION ALL
	-- 4. Draft(Sales Order & Purchase Order)
	  SELECT T0."DocEntry", 
	  		 ''112'' 							AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || ''d''				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 IFNULL(T2."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T3."IntsNo", 0)				AS "InstID",
			 IFNULL(T2."InstNum", 0)			AS "InstNum",
			 IFNULL(T3."InstMonth", 0)			AS "InstMonth",
			 IFNULL(T3."InstDays", 0)			AS "InstDays",
			 IFNULL(T3."InstPrcnt", 100)		AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
			 CASE	T0."ObjType" 
				WHEN ''22'' THEN
					0
				WHEN ''17'' THEN
					(T0."DocTotal" - T0."PaidToDate")
			 END								AS "Debit",
			 CASE	T0."ObjType"
				WHEN ''22'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''17'' THEN
					0
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN ''22'' THEN
					''EF''
				WHEN ''17'' THEN
					''SF''
			 END								AS "Group",
			 1									AS "PredictFlag",
			 T5."AvrageLate",
			 CASE T0."ObjType"
				WHEN ''22'' THEN
					IFNULL(T5."HldCode", '''')
				WHEN ''17'' THEN
					IFNULL(T4."HldCode", '''')
			 END								AS "HldCode"
	    FROM "ODRF" T0
   LEFT JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = ''N''
  INNER JOIN "OCTG" T2
		  ON T2."GroupNum" = T0."GroupNum"
   LEFT JOIN "CTG1" T3
		  ON T3."CTGCode" = T2."GroupNum" 
   LEFT JOIN "OADM" T4
		  ON T4."Code" = T4."Code"
  INNER JOIN "OCRD" T5
		  ON T5."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
		 AND T1."DraftEntry" IS NULL
		 AND T0."ObjType" IN (''22'', ''17'')
	UNION ALL
	-- 4. Draft(Delivery)
	  SELECT T0."DocEntry", 
	  		 ''112'' 							AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || ''d''				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 IFNULL(T2."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt", 
			 T0."Installmnt",
	         T0."DocDueDate", 
			 (T0."DocTotal" - T0."PaidToDate")	AS "Debit",
			 0									AS "Credit",
			 ''SF''								AS "Group",
			 1									AS "PredictFlag",
			 T4."AvrageLate",
			 IFNULL(T4."HldCode", '''')			AS "HldCode"
	    FROM "ODRF" T0
   LEFT JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = ''N''
  INNER JOIN "OCTG" T2
		  ON T2."GroupNum" = T0."GroupNum"
  INNER JOIN "OCRD" T4
		  ON T4."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
		 AND T1."DraftEntry" IS NULL
		 AND T0."ObjType" = ''15''
		 UNION ALL
	  --5.  Return Request
      SELECT 
      T0."DocEntry",
      T0."ObjType"						AS "DocType", 
      T0."DocNum", 
      T0."CardCode", 
      T0."CtlAccount",
      T0."TransId", 
      T0."JrnlMemo", 
      T0."ObjType",
      T0."ObjType"						AS "OriginalType", 
      T0."CreateDate", 
      T0."TaxDate", 
      T0."ClsDate", 
      T0."PayDuMonth", 
      T0."ExtraMonth", 
      T0."ExtraDays",
      IFNULL(T1."TolDays", 0)			AS "TolDays",
      T0."GroupNum", 
	  IFNULL(T0."Installmnt", 0)		AS "InstID", 
	  IFNULL(T0."Installmnt", 0)		AS "InstNum", 
	  0			                        AS "InstMonth",
	  0			                        AS "InstDays",
	  100		                        AS "InstPrcnt",
      T0."Installmnt",
      T0."DocDueDate", 
      CASE T3."NegAmount" 
		WHEN ''N'' THEN
			0
		WHEN ''Y'' THEN
			0 - (T0."DocTotal" - T0."PaidToDate")
		END								AS "Debit", 
	  CASE T3."NegAmount" 
		WHEN ''N'' THEN
			(T0."DocTotal" - T0."PaidToDate")
		WHEN ''Y'' THEN
			0
		END								AS "Credit",
      ''SF''								AS "Group",
      1									AS "PredictFlag",
      IFNULL(T2."AvrageLate", 0) AS "AvrageLate", 
      IFNULL(T2."HldCode", '''')			AS "HldCode"
      FROM  "ORRR" T0 
      INNER JOIN "OCTG" T1 
		  ON T1."GroupNum" = T0."GroupNum"
	  INNER  JOIN "OCRD" T2  ON  T2."CardCode" = T0."CardCode"    
	  LEFT   JOIN "OADM" T3  ON T3."Code" = T3."Code"
      WHERE T0."CANCELED" <> ''Y'' AND  T0."DocStatus" = ''O''
      UNION ALL
     --6. Goods Return Request
      SELECT 
      T0."DocEntry",
      T0."ObjType"						AS "DocType", 
      T0."DocNum", 
      T0."CardCode", 
      T0."CtlAccount",
      T0."TransId", 
      T0."JrnlMemo", 
      T0."ObjType",
      T0."ObjType"						AS "OriginalType", 
      T0."CreateDate", 
      T0."TaxDate", 
      T0."ClsDate", 
      T0."PayDuMonth", 
      T0."ExtraMonth", 
      T0."ExtraDays",
	  IFNULL(T1."TolDays", 0)			AS "TolDays", 
	  T0."GroupNum", 
	  IFNULL(T0."Installmnt", 0)		AS "InstID", 
	  IFNULL(T0."Installmnt", 0)		AS "InstNum", 
	  0			                        AS "InstMonth",
	  0			                        AS "InstDays",
	  100		                        AS "InstPrcnt",
      T0."Installmnt",
      T0."DocDueDate", 
	  CASE T3."NegAmount" 
	    WHEN ''N'' THEN
	    	(T0."DocTotal" - T0."PaidToDate")
	    WHEN ''Y'' THEN
	    	0
	    END								AS "Debit", 
	  CASE T3."NegAmount" 
	    WHEN ''N'' THEN
	    	0
	    WHEN ''Y'' THEN
	    	0 - (T0."DocTotal" - T0."PaidToDate")
	    END								AS "Credit",
      ''EF''								AS "Group",
      1									AS "PredictFlag",
      IFNULL(T2."AvrageLate", 0) AS "AvrageLate", 
      IFNULL(T3."HldCode", '''')			AS "HldCode"
      FROM  "OPRR" T0
      INNER JOIN "OCTG" T1 
		  ON T1."GroupNum" = T0."GroupNum"  
	  INNER  JOIN "OCRD" T2  ON  T2."CardCode" = T0."CardCode"   
	  LEFT   JOIN "OADM" T3  ON T3."Code" = T3."Code"
      WHERE T0."CANCELED" <> ''Y'' AND  T0."DocStatus" = ''O''
	  UNION ALL
	  --7.draft(Return Request, Goods Return Request)
	  SELECT 
      T0."DocEntry",
      ''112''						       AS "DocType", 
      T0."DocNum", 
      T0."CardCode", 
      T0."CtlAccount",
      T0."TransId", 
      T0."JrnlMemo", 
      T0."ObjType" || ''d''               AS "ObjType",
      T0."ObjType"						AS "OriginalType", 
      T0."CreateDate", 
      T0."TaxDate", 
      T0."ClsDate", 
      T0."PayDuMonth", 
      T0."ExtraMonth", 
      T0."ExtraDays",
	  IFNULL(T2."TolDays", 0)			AS "TolDays", 
	  T0."GroupNum",  
	  IFNULL(T0."Installmnt", 0)			AS "InstID", 
	  IFNULL(T0."Installmnt", 0)			AS "InstNum", 
	  0									AS "InstMonth", 
	  0									AS "InstDays", 
	  100								AS "InstPrcnt", 
      T0."Installmnt",
      T0."DocDueDate", 
      CASE	T0."ObjType" 
          WHEN ''234000031'' THEN
            CASE T4."NegAmount" 
		        WHEN ''N'' THEN
			          0
		        WHEN ''Y'' THEN
			          0 - (T0."DocTotal" - T0."PaidToDate")
		    END
		  WHEN ''234000032'' THEN
		     CASE T4."NegAmount" 
		  	    WHEN ''N'' THEN
	    	          (T0."DocTotal" - T0."PaidToDate")
	            WHEN ''Y'' THEN
	    	           0
	    	  END
	   END                                      AS "Debit",  
      CASE	T0."ObjType" 
          WHEN ''234000031'' THEN
            CASE T4."NegAmount" 
		        WHEN ''N'' THEN
			          (T0."DocTotal" - T0."PaidToDate")
		        WHEN ''Y'' THEN
			          0
		    END
		  WHEN ''234000032'' THEN
		     CASE T4."NegAmount" 
		  	    WHEN ''N'' THEN
	    	          0
	            WHEN ''Y'' THEN
	    	          0 - (T0."DocTotal" - T0."PaidToDate")
	    	  END
	   END                                      AS "Credit",
	   CASE T0."ObjType"
		   WHEN ''234000031'' THEN
			    ''SF''
		   WHEN ''234000032'' THEN
				''EF''
	   END								AS "Group",
      1									AS "PredictFlag",
      IFNULL(T3."AvrageLate", 0) AS "AvrageLate",    
	  CASE T0."ObjType"
		  WHEN ''234000031'' THEN
				IFNULL(T3."HldCode", '''')
		  WHEN ''234000032'' THEN
				IFNULL(T4."HldCode", '''')
	  END								AS "HldCode"	  
      FROM "ODRF" T0
 LEFT JOIN "ORCP" T1 
	     ON T0."ObjType" = T1."DocObjType"
		AND T0."DocEntry" = T1."DraftEntry"
		AND T1."IsRemoved" = ''N''
INNER JOIN "OCTG" T2
		 ON T2."GroupNum" = T0."GroupNum"
INNER JOIN "OCRD" T3  
         ON  T3."CardCode" = T0."CardCode"    
LEFT  JOIN "OADM" T4  
         ON T4."Code" = T4."Code"
      WHERE  T0."CANCELED" <> ''Y'' 
        AND  T0."DocStatus" = ''O''
        AND  T1."DraftEntry" IS NULL
        AND  T0."ObjType" IN (''234000031'', ''234000032'')';
		 
	EXEC(:SqlStr);
	
	-- B1_CashFlowForecastRecurringTrasactionView
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".VIEWS 
	WHERE VIEW_NAME = 'B1_CashFlowForecastRecurringTrasactionView' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		SqlStr := 'DROP VIEW "B1_CashFlowForecastRecurringTrasactionView"';
  	    EXEC(:SqlStr);		
	END IF;	
	SqlStr := 'CREATE VIEW "B1_CashFlowForecastRecurringTrasactionView"
	  AS
	-- 1. Recurring Trasaction(have Installments)
	  SELECT T0."DocEntry", 
	  		 ''540000043''						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || ''r''				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays",
			 IFNULL(T2."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T3."IntsNo", 0)				AS "InstID",
			 IFNULL(T2."InstNum", 0)			AS "InstNum",
			 IFNULL(T3."InstMonth", 0)			AS "InstMonth",
			 IFNULL(T3."InstDays", 0)			AS "InstDays",
			 IFNULL(T3."InstPrcnt", 100)		AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
			 CASE	T0."ObjType" 
				WHEN ''22'' THEN
					0
				WHEN ''204'' THEN
					0
				WHEN ''18'' THEN
					0
				WHEN ''163'' THEN
					CASE T4."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''164'' THEN
					0
				WHEN ''17'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''203'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''13'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''165'' THEN
					CASE T4."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN ''166'' THEN
					(T0."DocTotal" - T0."PaidToDate")
			 END								AS "Debit",
			 CASE	T0."ObjType"
				WHEN ''22'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''204'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''18'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''163'' THEN
					CASE T4."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN ''164'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''17'' THEN
					0
				WHEN ''203'' THEN
					0
				WHEN ''13'' THEN
					0
				WHEN ''165'' THEN
					CASE T4."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''166'' THEN
					0
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN ''22'' THEN
					''EF''
				WHEN ''204'' THEN
					''EF''
				WHEN ''18'' THEN
					''EF''
				WHEN ''163'' THEN
					''EF''
				WHEN ''164'' THEN
					''EF''
				WHEN ''17'' THEN
					''SF''
				WHEN ''203'' THEN
					''SF''
				WHEN ''13'' THEN
					''SF''
				WHEN ''165'' THEN
					''SF''
				WHEN ''166'' THEN
					''SF''
			 END								AS "Group",
			 CASE T0."ObjType"
				WHEN ''17'' THEN
					1
				WHEN ''22'' THEN
					1
				ELSE
					0
			 END								AS "PredictFlag",
			 T5."AvrageLate",
			 CASE T0."ObjType"
				WHEN ''22'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''204'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''18'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''163'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''164'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''17'' THEN
					IFNULL(T5."HldCode", '''')
				WHEN ''203'' THEN
					IFNULL(T5."HldCode", '''')
				WHEN ''13'' THEN
					IFNULL(T5."HldCode", '''')
				WHEN ''165'' THEN
					IFNULL(T5."HldCode", '''')
				WHEN ''166'' THEN
					IFNULL(T5."HldCode", '''')
			 END								AS "HldCode"
	    FROM "ODRF" T0
  INNER JOIN "ORCP" T1
		  ON T0."ObjType" = ABS(T1."DocObjType")
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = ''N''
  INNER JOIN "OCTG" T2
		  ON T2."GroupNum" = T0."GroupNum"
   LEFT JOIN "CTG1" T3
		  ON T3."CTGCode" = T2."GroupNum"
   LEFT JOIN "OADM" T4
		  ON T4."Code" = T4."Code"
  INNER JOIN "OCRD" T5
		  ON T5."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
		 AND T1."DraftEntry" IS NOT NULL
		 AND T0."ObjType" IN(''17'', ''203'', ''13'', ''165'', ''166'', ''22'', ''204'', ''18'', ''163'', ''164'')
	UNION ALL
	-- 2. Recurring Trasaction(not have Installments)
	  SELECT T0."DocEntry", 
	  		 ''540000043''						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || ''r''				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays",
			 IFNULL(T2."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
			 CASE	T0."ObjType" 
				WHEN ''20'' THEN
					0
				WHEN ''21'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''19'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''15'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''16'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN ''14'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN ''234000031'' THEN
                    CASE T3."NegAmount" 
		                WHEN ''N'' THEN
			                0
		                WHEN ''Y'' THEN
			                0 - (T0."DocTotal" - T0."PaidToDate")
		            END
		        WHEN ''234000032'' THEN
		            CASE T3."NegAmount" 
		  	            WHEN ''N'' THEN
	    	               (T0."DocTotal" - T0."PaidToDate")
	                    WHEN ''Y'' THEN
	    	                0
	    	        END
			 END								AS "Debit",
			 CASE	T0."ObjType"
				WHEN ''20'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''21'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN ''19'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN ''15'' THEN
					0
				WHEN ''16'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''14'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''234000031'' THEN
                    CASE T3."NegAmount" 
		                WHEN ''N'' THEN
			                (T0."DocTotal" - T0."PaidToDate")
		                WHEN ''Y'' THEN
			                0
		            END
		        WHEN ''234000032'' THEN
		            CASE T3."NegAmount" 
		                WHEN ''N'' THEN
	    	                0
	                    WHEN ''Y'' THEN
	    	                0 - (T0."DocTotal" - T0."PaidToDate")
	    	        END
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN ''20'' THEN
					''EF''
				WHEN ''21'' THEN
					''EF''
				WHEN ''19'' THEN
					''EF''
				WHEN ''15'' THEN
					''SF''
				WHEN ''16'' THEN
					''SF''
				WHEN ''14'' THEN
					''SF''
				WHEN ''234000031'' THEN
			        ''SF''
		        WHEN ''234000032'' THEN
				    ''EF''
			 END								AS "Group",
			 CASE T0."ObjType"
				WHEN ''15'' THEN
					1
				WHEN ''234000031'' THEN
				    1
				WHEN ''234000032'' THEN
				    1
				ELSE
					0
			 END								AS "PredictFlag",
			 T4."AvrageLate",
			 CASE T0."ObjType"
				WHEN ''20'' THEN
					IFNULL(T3."HldCode", '''')
				WHEN ''21'' THEN
					IFNULL(T3."HldCode", '''')
				WHEN ''19'' THEN
					IFNULL(T3."HldCode", '''')
				WHEN ''15'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''16'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''14'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''234000031'' THEN
				    IFNULL(T4."HldCode", '''')
				WHEN ''234000032'' THEN
				    IFNULL(T3."HldCode", '''')
			 END								AS "HldCode"
	    FROM "ODRF" T0
  INNER JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = ''N''
  INNER JOIN "OCTG" T2
		  ON T2."GroupNum" = T0."GroupNum"
   LEFT JOIN "OADM" T3
		  ON T3."Code" = T3."Code"
  INNER JOIN "OCRD" T4
		  ON T4."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
		 AND T1."DraftEntry" IS NOT NULL
		 AND T0."ObjType" IN (''15'', ''16'', ''14'', ''20'', ''21'', ''19'', ''234000031'', ''234000032'')';
		 
	EXEC(:SqlStr); 
		 
	-- B1_CashFlowForecastDueDateDocumentsView
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".VIEWS 
	WHERE VIEW_NAME = 'B1_CashFlowForecastDueDateDocumentsView' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		SqlStr := 'DROP VIEW "B1_CashFlowForecastDueDateDocumentsView"';
  	    EXEC(:SqlStr);		
	END IF;	
	SqlStr := 'CREATE VIEW "B1_CashFlowForecastDueDateDocumentsView"
	  AS
	--1. Revert Delivery
	  SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum", 
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt", 
			 T0."Installmnt",
	         T0."DocDueDate", 
	         CASE T2."NegAmount" 
				WHEN ''N'' THEN
					0
				WHEN ''Y'' THEN
					0 - (T0."DocTotal" - T0."PaidToDate")
			 END								AS "Debit", 
			 CASE T2."NegAmount" 
				WHEN ''N'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''Y'' THEN
					0
			 END								AS "Credit",
			 ''SF''								AS "Group",
			 0									AS "PredictFlag",
			 T1."AvrageLate",
			 IFNULL(T1."HldCode", '''')			AS "HldCode"
	    FROM "ORDN" T0
  INNER JOIN "OCRD" T1
		  ON T1."CardCode" = T0."CardCode" 
   LEFT JOIN "OADM" T2
		  ON T2."Code" = T2."Code"
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
	UNION ALL
	--2. A/R Down Payment Request
	  SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T2."DueDate"						AS "DocDueDate", 
	         T2."InsTotal" - T2."PaidToDate"	AS "Debit", 
	         0									AS "Credit",
			 ''SF''								AS "Group",
			 0									AS "PredictFlag",
			 T1."AvrageLate",
			 IFNULL(T1."HldCode", '''')			AS "HldCode"
	    FROM "ODPI" T0
  INNER JOIN "OCRD" T1
		  ON T1."CardCode" = T0."CardCode" 
  INNER JOIN "DPI6" T2
		  ON T2."DocEntry" = T0."DocEntry"
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
		 AND T0."Posted" = ''N''
	UNION ALL
	--3. Purchase Delivery
	  SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T0."DocDueDate", 
	         0 AS "Debit", 
	         T0."DocTotal" - T0."PaidToDate"	AS "Credit",
			 ''EF''								AS "Group",
			 0									AS "PredictFlag",
			 T2."AvrageLate",
			 IFNULL(T1."HldCode", '''')			AS "HldCode"
	    FROM "OPDN" T0
   LEFT JOIN "OADM" T1
		  ON T1."Code" = T1."Code"
  INNER JOIN "OCRD" T2
		  ON T2."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
	UNION ALL
	--4. Revert Purchase
	  SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T0."DocDueDate",
			 CASE T1."NegAmount" 
				WHEN ''N'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''Y'' THEN
					0
			 END								AS "Debit", 
			 CASE T1."NegAmount" 
				WHEN ''N'' THEN
					0
				WHEN ''Y'' THEN
					0 - (T0."DocTotal" - T0."PaidToDate")
			 END								AS "Credit",
			 ''EF''								AS "Group",
			 0									AS "PredictFlag",
			 T2."AvrageLate",
			 IFNULL(T1."HldCode", '''')			AS "HldCode"
	    FROM "ORPD" T0
   LEFT JOIN "OADM" T1
		  ON T1."Code" = T1."Code"
  INNER JOIN "OCRD" T2
		  ON T2."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
	UNION ALL
	--5. A/P Down Payment Request
	  SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T3."DueDate"						AS "DocDueDate", 
	         0									AS "Debit", 
	         T3."InsTotal" - T3."PaidToDate"	AS "Credit",
			 ''EF''								AS "Group",
			 0									AS "PredictFlag",
			 T2."AvrageLate",
			 IFNULL(T1."HldCode", '''')			AS "HldCode"
	    FROM "ODPO" T0
   LEFT JOIN "OADM" T1
		  ON T1."Code" = T1."Code"
  INNER JOIN "OCRD" T2
		  ON T2."CardCode" = T0."CardCode" 
  INNER JOIN "DPO6" T3
		  ON T3."DocEntry" = T0."DocEntry"
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
		 AND T0."Posted" = ''N''
	UNION ALL
	-- 6. Draft(not have installments)
	  SELECT T0."DocEntry", 
	  		 ''112''							AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || ''d''				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T0."DocDueDate", 
			 CASE	T0."ObjType" 
				WHEN ''20'' THEN
					0
				WHEN ''21'' THEN
					CASE T2."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''19'' THEN
					CASE T2."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''16'' THEN
					CASE T2."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN ''14'' THEN
					CASE T2."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
			 END								AS "Debit",
			 CASE	T0."ObjType"
				WHEN ''20'' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN ''21'' THEN
					CASE T2."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN ''19'' THEN
					CASE T2."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN ''16'' THEN
					CASE T2."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''14'' THEN
					CASE T2."NegAmount" 
						WHEN ''N'' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN ''20'' THEN
					''EF''
				WHEN ''21'' THEN
					''EF''
				WHEN ''19'' THEN
					''EF''
				WHEN ''16'' THEN
					''SF''
				WHEN ''14'' THEN
					''SF''
			 END								AS "Group",
			 0									AS "PredictFlag",
			 T3."AvrageLate",
			 CASE T0."ObjType"
				WHEN ''20'' THEN
					IFNULL(T2."HldCode", '''')
				WHEN ''21'' THEN
					IFNULL(T2."HldCode", '''')
				WHEN ''19'' THEN
					IFNULL(T2."HldCode", '''')
				WHEN ''16'' THEN
					IFNULL(T3."HldCode", '''')
				WHEN ''14'' THEN
					IFNULL(T3."HldCode", '''')
			 END								AS "HldCode"
	    FROM "ODRF" T0
   LEFT JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = ''N''
   LEFT JOIN "OADM" T2
		  ON T2."Code" = T2."Code"
  INNER JOIN "OCRD" T3
		  ON T3."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
		 AND T1."DraftEntry" IS NULL
		 AND T0."ObjType" IN (''16'', ''14'', ''21'', ''20'', ''19'')
	UNION ALL
	-- 7. Draft(have installments)
	  SELECT T0."DocEntry", 
	  		 ''112''							AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || ''d''				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 T2."InstlmntID"					AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0                            		AS "InstMonth", 
			 0                            		AS "InstDays", 
			 100								AS "InstPrcnt", 
			 T0."Installmnt",
	         T2."DueDate"						AS "DocDueDate", 
			 CASE	T0."ObjType" 
				WHEN ''204'' THEN
					0
				WHEN ''18'' THEN
					0
				WHEN ''163'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							(T2."InsTotal" - T2."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''164'' THEN
					0
				WHEN ''203'' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN ''13'' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN ''165'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T2."InsTotal" - T2."PaidToDate")
					END
				WHEN ''166'' THEN
					(T2."InsTotal" - T2."PaidToDate")
			 END								AS	"Debit",
			 CASE	T0."ObjType"
				WHEN ''204'' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN ''18'' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN ''163'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							0
						WHEN ''Y'' THEN
							0 - (T2."InsTotal" - T2."PaidToDate")
					END
				WHEN ''164'' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN ''203'' THEN
					0
				WHEN ''13'' THEN
					0
				WHEN ''165'' THEN
					CASE T3."NegAmount" 
						WHEN ''N'' THEN
							(T2."InsTotal" - T2."PaidToDate")
						WHEN ''Y'' THEN
							0
					END
				WHEN ''166'' THEN
					0
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN ''204'' THEN
					''EF''
				WHEN ''18'' THEN
					''EF''
				WHEN ''163'' THEN
					''EF''
				WHEN ''164'' THEN
					''EF''
				WHEN ''203'' THEN
					''SF''
				WHEN ''13'' THEN
					''SF''
				WHEN ''165'' THEN
					''SF''
				WHEN ''166'' THEN
					''SF''
			 END								AS "Group",
			 0									AS "PredictFlag",
			 T4."AvrageLate",
			 CASE T0."ObjType"
				WHEN ''204'' THEN
					IFNULL(T3."HldCode", '''')
				WHEN ''18'' THEN
					IFNULL(T3."HldCode", '''')
				WHEN ''163'' THEN
					IFNULL(T3."HldCode", '''')
				WHEN ''164'' THEN
					IFNULL(T3."HldCode", '''')
				WHEN ''203'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''13'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''165'' THEN
					IFNULL(T4."HldCode", '''')
				WHEN ''166'' THEN
					IFNULL(T4."HldCode", '''')
			 END								AS "HldCode"
	    FROM "ODRF" T0
   LEFT JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = ''N''
  INNER JOIN "DRF6" T2
		  ON T2."DocEntry" = T0."DocEntry"
   LEFT JOIN "OADM" T3
		  ON T3."Code" = T3."Code"
  INNER JOIN "OCRD" T4
		  ON T4."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> ''Y''
	     AND T0."DocStatus" = ''O''
		 AND T1."DraftEntry" IS NULL
		 AND T0."ObjType" IN (''203'', ''13'', ''165'', ''166'', ''204'', ''18'', ''163'', ''164'')';
     
    EXEC(:SqlStr);
		
	-- CFF_TMP_PAYMENTDATE
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_PAYMENTDATE' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_PAYMENTDATE CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_PAYMENTDATE (
		"CardCodePayment"	NVARCHAR (15),
		"DueDateDay"		SMALLINT, 
		"PmntDate"			SMALLINT,
		"NextMonth"			SMALLINT
	);

	-- CFF_TMP_ORCL
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_ORCL' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_ORCL CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_ORCL (
		"AbsEntry"			INTEGER,
		"RcpEntry"			INTEGER,
		"Instance"			INTEGER  DEFAULT 0,
		"PlanDate"			DATE,
		"Status"			NVARCHAR (1) DEFAULT 'N',
		"DocObjType"		NVARCHAR(20),
		"DocEntry"			INTEGER
	);	
	
	-- CFF_TMP_GROUP
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_GROUP' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_GROUP CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_GROUP (
		"Group"				NVARCHAR(2)
	);
	
	-- CFF_TMP_HOLIDAY
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_TMP_HOLIDAY' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		DROP TABLE CFF_TMP_HOLIDAY CASCADE;
	END IF;	
	CREATE GLOBAL TEMPORARY TABLE CFF_TMP_HOLIDAY
	(
		"HldCode"			NVARCHAR(20),
		"Intervals"			INT
	);
	
	-- CFF_DATE_T
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_DATE_T' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		SqlStr := 'DROP TYPE CFF_DATE_T CASCADE';
		EXEC(:SqlStr);
	END IF;	
	SqlStr := 'CREATE TYPE CFF_DATE_T AS TABLE(
		"AbsEntry"			INTEGER,
		"DocDueDate"		DATE, 
		"AdjustDocDueDate"	TINYINT, 
		"DeltaMonths"		SMALLINT, 
		"AdjustDay"			SMALLINT,
		"HldCode"			NVARCHAR(20),
		"DocType"			NVARCHAR(20),
		"DocEntry"			INTEGER,
		"CardCode"			NVARCHAR (15),
		"CtlAccount"		NVARCHAR (15),
		"Debit"				DECIMAL(21,6),
		"Credit"			DECIMAL(21,6),
		"OriginalType"		NVARCHAR(20),
		"Group"				NVARCHAR(2),
		"InstID"			SMALLINT,
		"DocNum"			INTEGER,
		"AvrageLate"		SMALLINT
	)';
	EXEC(:SqlStr);

	-- CFF_HOLIDAY_T
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_HOLIDAY_T' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		SqlStr := 'DROP TYPE CFF_HOLIDAY_T CASCADE';
		EXEC(:SqlStr);
	END IF;	
	SqlStr := 'CREATE TYPE CFF_HOLIDAY_T AS TABLE(
		"HldCode"			NVARCHAR(20), 
		"WndFrm"			TINYINT, 
		"WndTo"				TINYINT,
		"IsCurYear"			TINYINT,
		"IgnrWnd"			TINYINT,
		"StrDate"			DATE, 
		"EndDate"			DATE
	)';
	EXEC(:SqlStr);
	
	-- CFF_PREDICT_DUEDATE_T
	SELECT COUNT(*) INTO IsExists FROM "PUBLIC".M_TABLES 
	WHERE TABLE_NAME = 'CFF_PREDICT_DUEDATE_T' AND SCHEMA_NAME = CURRENT_SCHEMA;
	IF :IsExists <> 0 THEN
		SqlStr := 'DROP TYPE CFF_PREDICT_DUEDATE_T CASCADE';
		EXEC(:SqlStr);
	END IF;	
	SqlStr := 'CREATE TYPE CFF_PREDICT_DUEDATE_T AS TABLE(
		"AbsEntry"			INTEGER, 
		"PredictDueDate"	DATE
	)';
	EXEC(:SqlStr);
END;











