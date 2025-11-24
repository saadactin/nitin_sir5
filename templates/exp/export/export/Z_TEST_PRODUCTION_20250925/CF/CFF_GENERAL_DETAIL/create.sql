-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS AFTER:SP:CFF_PARSEOBJLIST AFTER:SP:CFF_PARSEOPTIONS AFTER:SP:CFF_FILLBOEACCT AFTER:SP:CFF_BLANKETAGREEMENT_DETAIL
CREATE PROCEDURE CFF_GENERAL_DETAIL (IN	dateFrom	DATE, 
									 IN	dateTo		DATE, 
									 IN	addRP		NVARCHAR(1),
									 IN	addBA		NVARCHAR(1),
									 IN	addJV		NVARCHAR(1),
									 IN	addDP		NVARCHAR(1))
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
	frequency		NVARCHAR(1);
	subFrequency	SMALLINT;
	limitFlag		NVARCHAR(1);
	limitDate		TIMESTAMP;
	currency		NVARCHAR(3);
	ctrlAcct			NVARCHAR(15);
	acctCode		NVARCHAR(15);
	nextDue			TIMESTAMP;
	debit			DECIMAL(21,6);
	credit			DECIMAL(21,6);
	mainInstance	SMALLINT;
	subInstance		SMALLINT;
	rcurCode		NVARCHAR(20);
	rcurDesc		NVARCHAR(50);
	newNextDue		DATE;
	filterInBPFlag	NVARCHAR(1);
CURSOR RecurTransQry(qyDateTo DATE) for 
	SELECT T0."Frequency", T0."Remind", T0."LimitRtrns", T0."LimitDate", T1."Currency", T1."CtrlAcct", T1."AcctCode", T0."Instance" as "MainInstance", T1."Instance" as "SubInstance", T0."NextDeu", T1."Debit", T1."Credit", T1."RcurCode", T0."RcurDesc" 
			FROM "ORCR" T0 INNER JOIN "RCR1" T1 ON T1."RcurCode" = T0."RcurCode" 
			WHERE T0."Frequency" <> 'T' AND T0."NextDeu" <= :qyDateTo AND (T0."LimitRtrns" <> 'Y' OR T0."NextDeu" <= T0."LimitDate") ;
BEGIN 	
	-- Fill BOE table
	CALL CFF_FILLBOEACCT ();
	-- Fill date table
	INSERT INTO "CFF_TMP_DATE" VALUES(:dateFrom, :dateTo, -1);
			
--	SELECT "DspFrznBP" INTO filterInBPFlag FROM "OADM";
	
	-- Filter the inactive BP
--	IF(:filterInBPFlag = 'Y') THEN
--		INSERT INTO "OCRD" SELECT "CardCode", "CardType", "AvrageLate" FROM "OCRD";
--	ELSE
--		INSERT INTO "OCRD" SELECT "CardCode", "CardType", "AvrageLate" FROM "OCRD" 
--			WHERE ("CardCode" IS NULL OR 
--					("validFor"='Y' OR ("frozenFor" = 'Y' AND ("frozenFrom" IS NOT NULL OR "frozenTo" IS NOT NULL )) OR ("validFor" = 'N' AND "frozenFor" = 'N')));
--	END IF;
	
	-- AcctType: 'B' represent BOE account, 'N' represent NOT BOE account
	--BOEAcctResult = SELECT DISTINCT "AcctCode", "CardCode", (CASE WHEN "AcctType" IN ('R', 'P', 'C', 'S', 'Y', 'I', 'U') THEN 'BOE' ELSE 'NBOE' END) AS "AcctType" FROM "CRD3";
	
	--- No OPEN balance
	--- Cash, ObjType='A'
	INSERT INTO "CFF_TMP_DETAIL" 
	SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
		SELECT T0."DueDate" AS "DueDate", T0."TransType" AS "DocType", T0."CreatedBy" AS "DocEntry", T0."BaseRef" AS "DocNum",
				T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
				IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-50' AS "ObjType",(CASE WHEN IFNULL(T0."BalDueDeb",0) <> 0 THEN 'CI' ELSE 'CO' END) AS "Group" 
		FROM "JDT1" T0 
		INNER JOIN "OACT" T1 ON T0."Account" = T1."AcctCode"
		WHERE T0."Account" = T0."ShortName" 
			AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
			AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)
			AND T1."Finanse" = 'Y'
		) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
				INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group"; 
		 
	--- Check, ObjType='C'
	INSERT INTO "CFF_TMP_DETAIL" 
	SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
		SELECT T0."DueDate" AS "DueDate", T0."TransType" AS "DocType", T0."CreatedBy" AS "DocEntry", T0."BaseRef" AS "DocNum",
				T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
				IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-27' AS "ObjType",(CASE WHEN IFNULL(T0."BalDueDeb",0) <> 0 THEN 'CI' ELSE 'CO' END) AS "Group" 
		FROM "JDT1" T0 
		WHERE T0."Account" = T0."ShortName" 
			AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
			AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)					
			AND T0."Account" IN (SELECT DISTINCT "CashCheck" FROM "OCHH")
		) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
				INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";
		 	 
	--- Credit, ObjType='D'
	INSERT INTO "CFF_TMP_DETAIL" 
	SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
		SELECT T0."DueDate" AS "DueDate", T0."TransType" AS "DocType", T0."CreatedBy" AS "DocEntry",  T0."BaseRef" AS "DocNum",
				T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
				IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-72' AS "ObjType", (CASE WHEN IFNULL(T0."BalDueDeb",0) <> 0 THEN 'CI' ELSE 'CO' END) AS "Group" 
		FROM "JDT1" T0 
		INNER JOIN (SELECT DISTINCT "CreditAcct" from "OCRH") T1 ON T0."Account" = T1."CreditAcct"
		WHERE T0."Account" = T0."ShortName" 
			AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
			AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)
		) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
				INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group"; 
		 
	--- BoE, ObjType='E'
	IF (:addDP = 'Y') THEN
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT ADD_DAYS(T0."DueDate", IFNULL(T2."AvrageLate",0)) AS "DueDate", T0."TransType" AS "DocType", T0."CreatedBy" AS "DocEntry", T0."BaseRef" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-182' AS "ObjType", (CASE WHEN T2."CardType" = 'C' THEN 'CI' ELSE 'CO' END) AS "Group" 
			FROM "JDT1" T0 
			INNER JOIN "CFF_TMP_BOEACCT" T1 ON T0."Account" = T1."AcctCode"
			INNER JOIN "OCRD" T2 ON T0."ShortName" = T2."CardCode"
			WHERE T0."Account" <> T0."ShortName"
				AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
				AND (ADD_DAYS(T0."DueDate", IFNULL(T2."AvrageLate",0)) BETWEEN :dateFrom AND :dateTo)	 
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";	
	ELSE
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT T0."DueDate" AS "DueDate", T0."TransType" AS "DocType", T0."CreatedBy" AS "DocEntry", T0."BaseRef" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-182' AS "ObjType", (CASE WHEN T2."CardType" = 'C' THEN 'CI' ELSE 'CO' END) AS "Group" 
			FROM "JDT1" T0 
			INNER JOIN "CFF_TMP_BOEACCT" T1 ON T0."Account" = T1."AcctCode"
			INNER JOIN "OCRD" T2 ON T0."ShortName" = T2."CardCode"
			WHERE T0."Account" <> T0."ShortName"
				AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
				AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo) 
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";	
	END IF;
		 
	--- Customer Liabilities, ObjType='M'
	IF(:addDP = 'Y') THEN
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT ADD_DAYS(T0."DueDate", IFNULL (T1."AvrageLate",0)) AS "DueDate", T0."TransType" AS "DocType", T0."CreatedBy" AS "DocEntry", T0."BaseRef" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", T0."TransType" AS "ObjType", 'CI' AS "Group" 
			FROM "JDT1" T0 
			INNER JOIN "OCRD" T1 ON T0."ShortName" = T1."CardCode"
			WHERE T0."Account" <> T0."ShortName"
				AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
				AND (ADD_DAYS(T0."DueDate", IFNULL (T1."AvrageLate",0)) BETWEEN :dateFrom AND :dateTo)
				AND T1."CardType" = 'C'
				AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."Account" ) 	 			
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";	
	ELSE
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT T0."DueDate" AS "DueDate", T0."TransType" AS "DocType", T0."CreatedBy" AS "DocEntry", T0."BaseRef" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", T0."TransType" AS "ObjType", 'CI' AS "Group" 
			FROM "JDT1" T0 
			INNER JOIN "OCRD" T1 ON T0."ShortName" = T1."CardCode"
			WHERE T0."Account" <> T0."ShortName"
				AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
				AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)
				AND T1."CardType" = 'C'
				AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."Account" ) 	 			
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";	
	END IF;
		 		
	--- Debts to Vendors, ObjType='V'
	IF (:addDP = 'Y') THEN
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT ADD_DAYS(T0."DueDate", IFNULL (T1."AvrageLate",0)) AS "DueDate", T0."TransType" AS "DocType", T0."CreatedBy" AS "DocEntry", T0."BaseRef" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", T0."TransType" AS "ObjType", 'CO' AS "Group" 
			FROM "JDT1" T0
			INNER JOIN "OCRD" T1 ON T0."ShortName" = T1."CardCode"
			WHERE T0."Account" <> T0."ShortName"
				AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
				AND (ADD_DAYS(T0."DueDate", IFNULL (T1."AvrageLate",0)) BETWEEN :dateFrom AND :dateTo)
				AND T1."CardType" = 'S'
				AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."Account" ) 	 			

			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";			
	ELSE
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT T0."DueDate" AS "DueDate", T0."TransType" AS "DocType", T0."CreatedBy" AS "DocEntry", T0."BaseRef" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", T0."TransType" AS "ObjType", 'CO' AS "Group" 
			FROM "JDT1" T0
			INNER JOIN "OCRD" T1 ON T0."ShortName" = T1."CardCode"
			WHERE T0."Account" <> T0."ShortName"
				AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
				AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)
				AND T1."CardType" = 'S'
				AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."Account" ) 	 				
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";			
	END IF;		
	
	IF(:addJV = 'Y') THEN
		--- Journal Voucher, no OPEN balance
		--- Cash, ObjType='A'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT T0."DueDate" AS "DueDate", '28' AS "DocType", T0."BatchNum" AS "DocEntry", T0."BatchNum" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-50' AS "ObjType",(CASE WHEN IFNULL(T0."Debit",0) <> 0 THEN 'SF' ELSE 'EF' END) AS "Group" 
			FROM "BTF1" T0 
			INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
			INNER JOIN "OACT" T3 ON T0."Account" = T3."AcctCode"
			WHERE T0."Account" = T0."ShortName"
				AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)  
				AND T1."BtfStatus" <> 'C'
				AND T3."Finanse" = 'Y' 
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";
			 	
		--- Check, ObjType='C'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT T0."DueDate" AS "DueDate", '28' AS "DocType", T0."BatchNum" AS "DocEntry", T0."BatchNum" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-27' AS "ObjType", (CASE WHEN IFNULL(T0."Debit",0) <> 0 THEN 'SF' ELSE 'EF' END) AS "Group" 
			FROM "BTF1" T0 
			INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
			WHERE T0."Account" = T0."ShortName"
				AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)  
				AND T1."BtfStatus" <> 'C'
				AND T0."Account" IN (SELECT DISTINCT "CashCheck" FROM "OCHH")
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group"; 
			 	
		--- Credit, ObjType='D'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT T0."DueDate" AS "DueDate", '28' AS "DocType", T0."BatchNum" AS "DocEntry", T0."BatchNum" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-72' AS "ObjType", (CASE WHEN IFNULL(T0."Debit",0) <> 0 THEN 'SF' ELSE 'EF' END) AS "Group" 
			FROM "BTF1" T0
			INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
			INNER JOIN (SELECT DISTINCT "CreditAcct" from "OCRH") T3 ON T0."Account" = T3."CreditAcct"
			WHERE T0."Account" = T0."ShortName"
				AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)
				AND T1."BtfStatus" <> 'C'
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group"; 
			 	
		--- BOE, ObjType='E'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT T0."DueDate" AS "DueDate", '28' AS "DocType", T0."BatchNum" AS "DocEntry", T0."BatchNum" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-182' AS "ObjType", (CASE WHEN T2."CardType" = 'C' THEN 'SF' ELSE 'EF' END) AS "Group" 
			FROM "BTF1" T0 
			INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
			INNER JOIN "OCRD" T2 ON T0."ShortName" = T2."CardCode"
			INNER JOIN "CFF_TMP_BOEACCT" T3 ON T0."Account" = T3."AcctCode"
			WHERE T0."Account" <> T0."ShortName"
				AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)
				AND T1."BtfStatus" <> 'C'
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";
			
		--- Customer Liabilities, ObjType='M'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" FROM(
			SELECT T0."DueDate" AS "DueDate", '28' AS "DocType", T0."BatchNum" AS "DocEntry", T0."BatchNum" AS "DocNum",
					T0."ShortName" AS "CtrlAcct", T0."Account" AS "Account", T0."LineMemo" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", 
					CASE WHEN T2."CardType" = 'C' THEN '17' ELSE '22' END AS "ObjType", 
					CASE WHEN T2."CardType" = 'C' THEN 'SF' ELSE 'EF' END AS "Group" 
			FROM "BTF1" T0 
			INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
			INNER JOIN "OCRD" T2 ON T0."ShortName" = T2."CardCode"
			WHERE T0."Account" <> T0."ShortName"
				AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)
				AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."Account" ) 	 				
				AND T1."BtfStatus" <> 'C'
				AND T2."CardType" IN ('C','S')
			) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
					INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";

		--- Debts to Vendors, ObjType='V'

	END IF;
	
	--- Recurring postings
	IF(:addRP = 'Y') THEN
		OPEN RecurTransQry(:dateTo);
		FETCH RecurTransQry INTO frequency, subFrequency, limitFlag, limitDate, currency, ctrlAcct, acctCode, mainInstance, subInstance, nextDue, debit, credit, rcurCode, rcurDesc;
		WHILE NOT RecurTransQry::NOTFOUND DO
			newNextDue := :nextDue;		-- This is a workaround for HANA. Otherwise nextDue will be null when insert into the table.
			WHILE :newNextDue <= :dateTo AND (:limitFlag <> 'Y' OR :nextDue <= :limitDate) DO
				-- Not OPEN balance
				IF(:newNextDue >= :dateFrom AND :mainInstance = :subInstance AND (:frequency <> 'O' OR :mainInstance <> 0)) THEN
					INSERT INTO "CFF_TMP_DETAIL_RP" VALUES(:newNextDue, :debit, :credit, :acctCode, :ctrlAcct, :rcurCode, :rcurDesc);
				END IF;
								
				IF (:frequency = 'D') THEN
					newNextDue := ADD_DAYS(:newNextDue, 1);
				ELSEIF (:frequency = 'W') THEN
					newNextDue := ADD_DAYS(:newNextDue, 7);
				ELSEIF (:frequency = 'A') THEN
					newNextDue := ADD_YEARS(:newNextDue, 1);
				ELSEIF (:frequency = 'M') THEN
					newNextDue := ADD_MONTHS(:newNextDue, 1);
				ELSEIF (:frequency = 'S') THEN
					newNextDue := ADD_MONTHS(:newNextDue, 6);
				ELSEIF (:frequency = 'Q') THEN
					newNextDue := ADD_MONTHS(:newNextDue, 3);
				ELSE
				 	newNextDue := ADD_DAYS(:newNextDue, 7);
				END IF;
			END WHILE;
			FETCH RecurTransQry INTO frequency, subFrequency, limitFlag, limitDate, currency, ctrlAcct, acctCode, mainInstance, subInstance, nextDue, debit, credit, rcurCode, rcurDesc;
		END WHILE;
		close RecurTransQry;
		
		--- No OPEN balance
		--- Cash, ObjType='A'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" 
		FROM (
			SELECT T0."DueDate" AS "DueDate", '34' AS "DocType", T0."DocEntry" AS "DocEntry", T0."DocEntry" AS "DocNum",
					T0."CtrlAcct" AS "CtrlAcct", T0."AcctCode" AS "Account", T0."Remarks" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-50' AS "ObjType",(CASE WHEN IFNULL(T0."Debit",0) <> 0 THEN 'SF' ELSE 'EF' END) AS "Group" 
			FROM "CFF_TMP_DETAIL_RP" T0 
			INNER JOIN "OACT" T1 ON T0."CtrlAcct" = T1."AcctCode"
			WHERE T0."AcctCode" = T0."CtrlAcct"
				AND T1."Finanse" = 'Y' 
		) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
				INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";
			 
		--- Check, ObjType='C'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" 
		FROM (
			SELECT T0."DueDate" AS "DueDate", '34' AS "DocType", T0."DocEntry" AS "DocEntry", T0."DocEntry" AS "DocNum",
					T0."CtrlAcct" AS "CtrlAcct", T0."AcctCode" AS "Account", T0."Remarks" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-27' AS "ObjType",(CASE WHEN IFNULL(T0."Debit",0) <> 0 THEN 'SF' ELSE 'EF' END) AS "Group" 
			FROM "CFF_TMP_DETAIL_RP" T0 
			WHERE T0."AcctCode" = T0."CtrlAcct"
				AND T0."CtrlAcct" IN (SELECT DISTINCT "CashCheck" FROM "OCHH") 
		) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
				INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";
			
		--- Credit, ObjType='D'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" 
		FROM (
			SELECT T0."DueDate" AS "DueDate", '34' AS "DocType", T0."DocEntry" AS "DocEntry", T0."DocEntry" AS "DocNum",
					T0."CtrlAcct" AS "CtrlAcct", T0."AcctCode" AS "Account", T0."Remarks" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-72' AS "ObjType",(CASE WHEN IFNULL(T0."Debit",0) <> 0 THEN 'SF' ELSE 'EF' END) AS "Group" 
			FROM "CFF_TMP_DETAIL_RP" T0 
			INNER JOIN (SELECT DISTINCT "CreditAcct" from "OCRH") T1 ON T0."CtrlAcct" = T1."CreditAcct"
			WHERE T0."AcctCode" = T0."CtrlAcct"
		) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
				INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group"; 
 	
		--- BoE, ObjType='E'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" 
		FROM (
			SELECT T0."DueDate" AS "DueDate", '34' AS "DocType", T0."DocEntry" AS "DocEntry", T0."DocEntry" AS "DocNum",
					T0."CtrlAcct" AS "CtrlAcct", T0."AcctCode" AS "Account", T0."Remarks" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-182' AS "ObjType",(CASE WHEN T2."CardType" = 'C' THEN 'SF' ELSE 'EF' END) AS "Group" 
			FROM "CFF_TMP_DETAIL_RP" T0 
			INNER JOIN "CFF_TMP_BOEACCT" T1 ON T0."CtrlAcct" = T1."AcctCode"
			INNER JOIN "OCRD" T2 ON T0."AcctCode" = T2."CardCode"
			WHERE T0."AcctCode" <> T0."CtrlAcct"
		) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
				INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group"; 
			 	
		--- Customer Liabilities, ObjType='M'
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" 
		FROM (
			SELECT T0."DueDate" AS "DueDate", '34' AS "DocType", T0."DocEntry" AS "DocEntry", T0."DocEntry" AS "DocNum",
					T0."CtrlAcct" AS "CtrlAcct", T0."AcctCode" AS "Account", T0."Remarks" AS "Remarks", 
					IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", 
					CASE WHEN T1."CardType" = 'C' THEN '17' ELSE '22' END AS "ObjType", 
					CASE WHEN T1."CardType" = 'C' THEN 'SF' ELSE 'EF' END AS "Group" 
			FROM "CFF_TMP_DETAIL_RP" T0 
			INNER JOIN "OCRD" T1 ON T0."AcctCode" = T1."CardCode"
			WHERE T0."AcctCode" <> T0."CtrlAcct"
				AND T1."CardType" IN ('C','S')
				AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."CtrlAcct" ) 	 
				
		)AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
				INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";  
		--- Debts to Vendors, ObjType='V'
		
	END IF;	
	
	--- Blanket Agreement
	IF(:addBA = 'Y') THEN
		CALL CFF_BLANKETAGREEMENT_DETAIL(:dateFrom, :dateTo);
		INSERT INTO "CFF_TMP_DETAIL" 
		SELECT T5."DueDate", T5."DocType", T5."DocEntry", T5."CtrlAcct", T5."Account", T5."Remarks", T5."Debit", T5."Credit", T5."ObjType", T5."Group", 1, T5."DocNum" 
		FROM (
			SELECT * FROM "CFF_TMP_DETAIL_BA"
		) AS T5 INNER JOIN "CFF_TMP_OBJLIST" T6 ON T5."ObjType" = T6."ObjType"
				INNER JOIN  "CFF_TMP_GROUP" T7 ON T7."Group" = T5."Group";
	
	END IF;
					
END;











