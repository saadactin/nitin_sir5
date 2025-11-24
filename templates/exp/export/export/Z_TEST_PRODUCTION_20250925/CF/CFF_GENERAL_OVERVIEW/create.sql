-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS AFTER:SP:CFF_PARSEOPTIONS AFTER:SP:CFF_PARSEOBJLIST AFTER:SP:CFF_FILLDATETABLE AFTER:SP:CFF_FILLBOEACCT AFTER:SP:CFF_BLANKETAGREEMENT_OVERVIEW
CREATE PROCEDURE CFF_GENERAL_OVERVIEW (IN	dateFrom	DATE, 
									   IN	dateTo		DATE, 
									   IN	intervalVal	NVARCHAR(1),
									   IN 	addRP 		NVARCHAR(1),
									   IN	addBA 		NVARCHAR(1),
									   IN	addJV 		NVARCHAR(1),
									   IN	addDP 		NVARCHAR(1))
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
	frequency		NVARCHAR(1);
	subFrequency	SMALLINT;
	limitFlag		NVARCHAR(1);
	limitDate		TIMESTAMP;
	currency		NVARCHAR(3);
	ctrlAcct		NVARCHAR(15);
	acctCode		NVARCHAR(15);
	nextDue			TIMESTAMP;
	Debit			DECIMAL(21,6);
	credit			DECIMAL(21,6);
	mainInstance	SMALLINT;
	subInstance		SMALLINT;
	newNextDue		DATE;
	filterInBPFlag	NVARCHAR(1);
	addBB			INTEGER;
	addCB           INTEGER;
CURSOR RecurTransQry(qyDateTo DATE) FOR 
	SELECT T0."Frequency", T0."Remind", T0."LimitRtrns", T0."LimitDate", T1."Currency", T1."CtrlAcct", T1."AcctCode", T0."Instance" as "MainInstance", T1."Instance" as "SubInstance", T0."NextDeu", T1."Debit", T1."Credit" 
			FROM "ORCR" T0 INNER JOIN "RCR1" T1 ON T1."RcurCode" = T0."RcurCode" 
			WHERE T0."Frequency" <> 'T' AND T0."NextDeu" <= :qyDateTo AND (T0."LimitRtrns" <> 'Y' OR T0."NextDeu" <= T0."LimitDate") ;
BEGIN 	
	-- Fill Date table
	CALL CFF_FILLDATETABLE(:dateFrom, :dateTo, :intervalVal);
	-- Fill BOE table
	CALL CFF_FILLBOEACCT ();
	
	SELECT COUNT(*) INTO addBB FROM "CFF_TMP_GROUP" WHERE "Group" = 'BB';
	SELECT COUNT(*) INTO addCB FROM "CFF_TMP_GROUP" WHERE "Group" = 'CB';
	
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
--	BOEAcctResult = SELECT DISTINCT "AcctCode", "CardCode", (CASE WHEN "AcctType" IN ('R', 'P', 'C', 'S', 'Y', 'I', 'U') THEN 'BOE' ELSE 'NBOE' END) AS "AcctType" FROM "CRD3";
	
			
	--- No OPEN balance
	ResultNOB = SELECT T5."EndDate" AS "EndDate", SUM(T5."Debit") AS "Debit", SUM(T5."Credit") AS "Credit", T5."ObjType" AS "ObjType", T5."Group" AS "Group", COUNT(*) AS "Number" FROM (
		--- Cash, ObjType='A'
		SELECT T2."EndDate", IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-50' AS "ObjType", (CASE WHEN IFNULL(T0."BalDueDeb",0) <> 0 THEN 'CI' ELSE 'CO' END) AS "Group" 
			FROM "JDT1" T0 
				INNER JOIN "OACT" T1 ON T0."Account" = T1."AcctCode"
				INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
				WHERE T0."Account" = T0."ShortName" 
					AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
					AND T1."Finanse" = 'Y' 
		UNION ALL 
		--- Check, ObjType='C'
		SELECT T2."EndDate", IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-27' AS "ObjType", (CASE WHEN IFNULL(T0."BalDueDeb",0) <> 0 THEN 'CI' ELSE 'CO' END) AS "Group"
			FROM "JDT1" T0 
				INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
				WHERE T0."Account" = T0."ShortName" 
					AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
					AND T0."Account" IN (SELECT DISTINCT "CashCheck" FROM "OCHH") 
		UNION ALL 
		--- Credit, ObjType='D'
		SELECT T2."EndDate", IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-72' AS "ObjType", (CASE WHEN IFNULL(T0."BalDueDeb",0) <> 0 THEN 'CI' ELSE 'CO' END) AS "Group"
			FROM "JDT1" T0 
				INNER JOIN (SELECT DISTINCT "CreditAcct" from "OCRH") T1 ON T0."Account" = T1."CreditAcct"
				INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
				WHERE T0."Account" = T0."ShortName" 
					AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0) 
		) AS T5 GROUP BY T5."EndDate", T5."ObjType", T5."Group";
		
	IF (:addDP = 'Y') THEN
		ResultNOBDP = SELECT T5."EndDate" AS "EndDate", SUM(T5."Debit") AS "Debit", SUM(T5."Credit") AS "Credit", T5."ObjType" AS "ObjType", T5."Group" AS "Group", COUNT(*) AS "Number" FROM (
		--- BoE, ObjType='E'
			SELECT T2."EndDate", IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-182' AS "ObjType", (CASE WHEN T3."CardType" = 'C' THEN 'CI' ELSE 'CO' END) AS "Group"
				FROM "JDT1" T0 
					INNER JOIN "CFF_TMP_BOEACCT" T1 ON T0."Account" = T1."AcctCode"
					INNER JOIN "OCRD" T3 ON T0."ShortName" = T3."CardCode"
					INNER JOIN "CFF_TMP_DATE" T2 ON ADD_DAYS(T0."DueDate", IFNULL(T3."AvrageLate",0) ) BETWEEN T2."StartDate" AND T2."EndDate"
					WHERE T0."Account" <> T0."ShortName"
						AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)		
			UNION ALL			
			--- Customer Liabilities, ObjType='M'
			SELECT T2."EndDate", IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", T0."TransType" AS "ObjType", 
				CASE WHEN T1."CardType"='C' THEN 'CI' ELSE 'CO' END AS "Group"
				FROM "JDT1" T0 
					INNER JOIN "OCRD" T1 ON T0."ShortName" = T1."CardCode"
					INNER JOIN "CFF_TMP_DATE" T2 ON ADD_DAYS(T0."DueDate", IFNULL( T1."AvrageLate",0) ) BETWEEN T2."StartDate" AND T2."EndDate"
					WHERE T0."Account" <> T0."ShortName"
						AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
						AND T1."CardType" IN ('C','S')
						AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."Account" ) 	 
			) AS T5 GROUP BY T5."EndDate", T5."ObjType", T5."Group";
	ELSE
		ResultNOBDP = SELECT T5."EndDate" AS "EndDate", SUM(T5."Debit") AS "Debit", SUM(T5."Credit") AS "Credit", T5."ObjType" AS "ObjType", T5."Group" AS "Group", COUNT(*) AS "Number" FROM (
			--- BoE, ObjType='E'
			SELECT T2."EndDate", IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", '-182' AS "ObjType", (CASE WHEN T3."CardType" = 'C' THEN 'CI' ELSE 'CO' END) AS "Group"
				FROM "JDT1" T0 
					INNER JOIN "CFF_TMP_BOEACCT" T1 ON T0."Account" = T1."AcctCode"
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					INNER JOIN "OCRD" T3 ON T0."ShortName" = T3."CardCode"
					WHERE T0."Account" <> T0."ShortName"
						AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
			UNION ALL								
			--- Customer Liabilities, ObjType='M'
			SELECT T2."EndDate", IFNULL(T0."BalDueDeb",0) AS "Debit", IFNULL(T0."BalDueCred",0) AS "Credit", T0."TransType" AS "ObjType", 
				CASE WHEN T1."CardType"='C' THEN 'CI' ELSE 'CO' END AS "Group"
				FROM "JDT1" T0 
					INNER JOIN "OCRD" T1 ON T0."ShortName" = T1."CardCode"
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					WHERE T0."Account" <> T0."ShortName"
						AND (IFNULL(T0."BalDueDeb",0) <> 0 OR IFNULL(T0."BalDueCred",0) <> 0)
						AND T1."CardType" IN ('C','S')
						AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."Account" ) 	 	 
			) AS T5 GROUP BY T5."EndDate", T5."ObjType", T5."Group";	
	END IF;
												
	--- Open balance
	IF	(:addBB > 0) THEN
		ResultOB = SELECT :dateFrom AS "EndDate", SUM (T5."OpenBalance") AS "Debit", 0.0 AS "Credit", T5."ObjType" AS "ObjType", T5."Group" AS "Group", COUNT(*) AS "Number" FROM (
			--- Cash, ObjType='A'
			SELECT SUM(IFNULL(T0."BalDueDeb",0) - IFNULL(T0."BalDueCred",0)) AS "OpenBalance", '-50' AS "ObjType", 'BB' AS "Group" 
				FROM "JDT1" T0 
					INNER JOIN "OACT" T1 ON T0."Account" = T1."AcctCode"
					WHERE T0."DueDate" < :dateFrom
						AND (:addCB > 0 OR T0."TransType" <> '-3')
						AND T0."Account" = T0."ShortName"
						AND T1."Finanse" = 'Y'
			UNION ALL 
			--- Check, ObjType='C'
			SELECT SUM(IFNULL(T0."BalDueDeb",0) - IFNULL(T0."BalDueCred",0)) AS "OpenBalance", '-27' AS "ObjType", 'BB' AS "Group" 
				FROM "JDT1" T0 
					WHERE T0."DueDate" < :dateFrom
						AND (:addCB > 0 OR T0."TransType" <> '-3') 
						AND T0."Account" = T0."ShortName"
						AND T0."Account" IN (SELECT DISTINCT "CashCheck" FROM "OCHH") 
			UNION ALL 	
			--- Credit, ObjType='D'
			SELECT SUM(IFNULL(T0."BalDueDeb",0) - IFNULL(T0."BalDueCred",0)) AS "OpenBalance", '-72' AS "ObjType", 'BB' AS "Group" 
				FROM "JDT1" T0 
					INNER JOIN (SELECT DISTINCT "CreditAcct" from "OCRH") T1 ON T0."Account" = T1."CreditAcct"
					WHERE T0."DueDate" < :dateFrom
						AND (:addCB > 0 OR T0."TransType" <> '-3') 
						AND T0."Account" = T0."ShortName" 
			UNION ALL 
			--- BoE, ObjType='E'
			SELECT SUM(IFNULL(T0."BalDueDeb",0) - IFNULL(T0."BalDueCred",0)) AS "OpenBalance", '-182' AS "ObjType", 'BB' AS "Group"
				FROM "JDT1" T0 
					INNER JOIN "CFF_TMP_BOEACCT" T1 ON T0."Account" = T1."AcctCode"
					WHERE T0."DueDate" < :dateFrom 
						AND T0."Account" <> T0."ShortName"
						AND (:addCB > 0 OR T0."TransType" <> '-3')
			UNION ALL 
			--- Customer Liabilities, ObjType='M'
			SELECT SUM(IFNULL(T0."BalDueDeb",0) - IFNULL(T0."BalDueCred",0)) AS "OpenBalance", T0."TransType" AS "ObjType", 'BB' AS "Group"
				FROM "JDT1" T0 
					INNER JOIN "OCRD" T1 ON T0."ShortName" = T1."CardCode"
					WHERE T0."DueDate" < :dateFrom 
						AND T0."Account" <> T0."ShortName"
						AND T1."CardType" IN ('C','S')
						AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."Account" ) 	 
						
					GROUP BY T0."TransType" 
		
			) T5 GROUP BY T5."ObjType", T5."Group" ;	
	ELSE
		ResultOB = SELECT :dateFrom AS "EndDate", 0.0 AS "Debit", 0.0 AS "Credit", 'X' AS "ObjType", 'BB' AS "Group", 0 AS "Number" FROM dummy;
	END IF;
		
	
	IF (:addJV = 'Y') THEN
		--- Journal Voucher, no OPEN balance
		ResultNOBJV = SELECT T5."EndDate" AS "EndDate", SUM(T5."Debit") AS "Debit", SUM(T5."Credit") AS "Credit", T5."ObjType" AS "ObjType", T5."Group" AS "Group", COUNT(*) AS "Number" FROM (
			--- Cash, ObjType='A'
			SELECT T2."EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-50' AS "ObjType", (CASE WHEN IFNULL(T0."Debit",0) <> 0 THEN 'SF' ELSE 'EF' END) AS "Group" 
				FROM "BTF1" T0 
					INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					INNER JOIN "OACT" T3 ON T0."Account" = T3."AcctCode"
					WHERE T0."Account" = T0."ShortName" 
						AND T1."BtfStatus" <> 'C'
						AND T3."Finanse" = 'Y' 
			UNION ALL 	
			--- Check, ObjType='C'
			SELECT T2."EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-27' AS "ObjType", (CASE WHEN IFNULL(T0."Debit",0)  <> 0 THEN 'SF' ELSE 'EF' END) AS "Group"
				FROM "BTF1" T0 
					INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					WHERE T0."Account" = T0."ShortName"
						AND T1."BtfStatus" <> 'C'
						AND T0."Account" IN (SELECT DISTINCT "CashCheck" FROM "OCHH") 
			UNION ALL 	
			--- Credit, ObjType='D'
			SELECT T2."EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-72' AS "ObjType", (CASE WHEN IFNULL(T0."Debit",0)  <> 0 THEN 'SF' ELSE 'EF' END) AS "Group"
				FROM "BTF1" T0
					INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					INNER JOIN (SELECT DISTINCT "CreditAcct" from "OCRH") T3 ON T0."Account" = T3."CreditAcct"
					WHERE T0."Account" = T0."ShortName"
						AND T1."BtfStatus" <> 'C' 
			UNION ALL 	
			--- BOE, ObjType='E'
			SELECT T4."EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-182' AS "ObjType", (CASE WHEN T2."CardType" = 'C' THEN 'SF' ELSE 'EF' END) AS "Group" 
			FROM "BTF1" T0 
				INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
				INNER JOIN "OCRD" T2 ON T0."ShortName" = T2."CardCode"
				INNER JOIN "CFF_TMP_BOEACCT" T3 ON T0."Account" = T3."AcctCode"
				INNER JOIN "CFF_TMP_DATE" T4 ON T0."DueDate" BETWEEN T4."StartDate" AND T4."EndDate"
				WHERE T0."Account" <> T0."ShortName"
					AND (T0."DueDate" BETWEEN :dateFrom AND :dateTo)
					AND T1."BtfStatus" <> 'C'
			UNION ALL			
			--- Customer Liabilities, ObjType='M'
			SELECT T3."EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", 
				CASE WHEN T2."CardType" = 'C' THEN '17' ELSE '22' END AS "ObjType", 
				CASE WHEN T2."CardType" = 'C' THEN 'SF' ELSE 'EF' END AS "Group"
				FROM "BTF1" T0 
					INNER JOIN "OBTF" T1 ON T0."BatchNum" = T1."BatchNum" AND T0."TransId" = T1."TransId"
					INNER JOIN "OCRD" T2 ON T0."ShortName" = T2."CardCode"
					INNER JOIN "CFF_TMP_DATE" T3 ON T0."DueDate" BETWEEN T3."StartDate" AND T3."EndDate"
					WHERE T0."Account" <> T0."ShortName"
						AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."Account" ) 
						AND T1."BtfStatus" <> 'C'
						AND T2."CardType" IN ('C','S')
			) AS T5 GROUP BY T5."EndDate", T5."ObjType", T5."Group";						
	ELSE
		ResultNOBJV = SELECT :dateFrom AS "EndDate", 0.0 AS "Debit", 0.0 AS "Credit", 'X' AS "ObjType", 'XX' AS "Group", 0 AS "Number" FROM dummy;
	END IF;
	
	--- Recurring postings
	IF(:addRP = 'Y') THEN
		OPEN RecurTransQry(:dateTo);
		FETCH RecurTransQry INTO frequency, subFrequency, limitFlag, limitDate, currency, ctrlAcct, acctCode, mainInstance, subInstance, nextDue, debit, credit;
		WHILE NOT RecurTransQry::NOTFOUND DO
			-- This is a workaround for HANA. Otherwise nextDue will be null when insert into the table.
			newNextDue := :nextDue;
			WHILE :newNextDue <= :dateTo AND (:limitFlag <> 'Y' OR :newNextDue <= :limitDate) DO
				-- Not OPEN balance
				IF(:newNextDue >= :dateFrom AND :mainInstance = :subInstance AND (:frequency <> 'O' OR :mainInstance <> 0)) THEN
					INSERT INTO "CFF_TMP_RP" VALUES(:newNextDue, :debit, :credit, :acctCode, :ctrlAcct);					
				END IF;
				
				-- Open balance
				IF(:newNextDue < :dateFrom AND :subInstance = 0 AND :debit <> :Credit) THEN
					INSERT INTO "CFF_TMP_RP_OB" VALUES(:dateFrom, 0, (:debit - :credit), :acctCode, :ctrlAcct);
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
			FETCH RecurTransQry INTO frequency, subFrequency, limitFlag, limitDate, currency, ctrlAcct, acctCode, mainInstance, subInstance, nextDue, debit, credit;
		END WHILE;
		CLOSE RecurTransQry;
		
		--- No OPEN balance
		ResultNOBRP = SELECT T5."EndDate" AS "EndDate", SUM(T5."Debit") AS "Debit", SUM(T5."Credit") AS "Credit", T5."ObjType" AS "ObjType", T5."Group", COUNT(*) AS "Number" FROM (
			--- Cash, ObjType='A'
			SELECT T2."EndDate" AS "EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-50' AS "ObjType", (CASE WHEN IFNULL(T0."Debit",0)  <> 0 THEN 'SF' ELSE 'EF' END) AS "Group" 
				FROM "CFF_TMP_RP" T0 
					INNER JOIN "OACT" T1 ON T0."CtrlAcct" = T1."AcctCode"
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					WHERE T0."AcctCode" = T0."CtrlAcct" 
						AND T1."Finanse" = 'Y' 
			UNION ALL 
			--- Check, ObjType='C'
			SELECT T2."EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-27' AS "ObjType", (CASE WHEN IFNULL(T0."Debit",0)  <> 0 THEN 'SF' ELSE 'EF' END) AS "Group"
				FROM "CFF_TMP_RP" T0 
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					WHERE T0."AcctCode" = T0."CtrlAcct"
					AND T0."CtrlAcct" IN (SELECT DISTINCT "CashCheck" FROM "OCHH") 
			UNION ALL 	
			--- Credit, ObjType='D'
			SELECT T2."EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-72' AS "ObjType", (CASE WHEN IFNULL(T0."Debit",0)  <> 0 THEN 'SF' ELSE 'EF' END) AS "Group"
				FROM "CFF_TMP_RP" T0 
					INNER JOIN (SELECT DISTINCT "CreditAcct" from "OCRH") T1 ON T0."CtrlAcct" = T1."CreditAcct"
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					WHERE T0."AcctCode" = T0."CtrlAcct" 
			UNION ALL 	
			--- BoE, ObjType='E'
			SELECT T2."EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", '-182' AS "ObjType", (CASE WHEN T3."CardType" = 'C' THEN 'SF' ELSE 'EF' END) AS "Group"
				FROM "CFF_TMP_RP" T0 
					INNER JOIN (SELECT DISTINCT "AcctCode", "CardCode" FROM "CRD3" WHERE "AcctType" IN ('R', 'P', 'C', 'S', 'Y', 'I', 'U')) T1 ON T0."CtrlAcct" = T1."AcctCode" AND T0."AcctCode" = T1."CardCode"
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					INNER JOIN "OCRD" T3 ON T0."AcctCode" = T3."CardCode"
					WHERE T0."AcctCode" <> T0."CtrlAcct"
			UNION ALL 	
			--- Customer Liabilities, ObjType='M'
			SELECT T2."EndDate", IFNULL(T0."Debit",0) AS "Debit", IFNULL(T0."Credit",0) AS "Credit", 
				CASE WHEN T1."CardType" = 'C' THEN '17' ELSE '22' END AS "ObjType", 
				CASE WHEN T1."CardType" = 'C' THEN 'SF' ELSE 'EF' END AS "Group"
				FROM "CFF_TMP_RP" T0 
					INNER JOIN "OCRD" T1 ON T0."AcctCode" = T1."CardCode"
					INNER JOIN "CFF_TMP_DATE" T2 ON T0."DueDate" BETWEEN T2."StartDate" AND T2."EndDate"
					WHERE T0."AcctCode" <> T0."CtrlAcct"
						AND T1."CardType" IN ('C','S')
						AND NOT EXISTS ( SELECT 1 FROM "CFF_TMP_BOEACCT" WHERE "AcctCode" = T0."CtrlAcct" ) 
			) AS T5 GROUP BY T5."EndDate", T5."ObjType", T5."Group";
	ELSE
		ResultNOBRP = SELECT :dateFrom AS "EndDate", 0.0 AS "Debit", 0.0 AS "Credit", 'X' AS "ObjType", 'XX' AS "Group", 0 AS "Number" FROM dummy;
	END IF;	
	
	-- Blanket Agreement, the result IS IN CFF_TMP_OVERVIEW_BA table
	IF(:addBA = 'Y') THEN
		CALL CFF_BLANKETAGREEMENT_OVERVIEW(:dateFrom, :dateTo);
	END IF;
	
	INSERT INTO "CFF_TMP_OVERVIEW" 
		SELECT T6."Index", T5."EndDate", SUM(T5."Debit"), SUM(T5."Credit"), T5."ObjType", T5."Group", SUM("Number") FROM (
			SELECT * FROM :ResultNOB
			UNION ALL
			SELECT * FROM :ResultNOBDP 
			UNION ALL 
			SELECT * FROM :ResultNOBJV WHERE "ObjType" <> 'X'
			UNION ALL
			SELECT * FROM :ResultNOBRP WHERE "ObjType" <> 'X'
			UNION ALL
			SELECT * FROM "CFF_TMP_OVERVIEW_BA"
			) AS T5 
			INNER JOIN "CFF_TMP_DATE" T6 ON T5."EndDate" = T6."EndDate"
			INNER JOIN "CFF_TMP_OBJLIST" T7 ON T7."ObjType" = T5."ObjType" 
			GROUP BY T5."EndDate", T5."ObjType", T5."Group", T6."Index";
	
	IF :addBB > 0 THEN
		INSERT INTO "CFF_TMP_OVERVIEW" 
			SELECT -1, :dateFrom, T5."Debit", 0, T5."ObjType", T5."Group", T5."Number" 
			FROM :ResultOB T5
			INNER JOIN "CFF_TMP_OBJLIST" T6 ON T6."ObjType" = T5."ObjType";
	END IF;
END;











