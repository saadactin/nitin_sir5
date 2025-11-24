CREATE PROCEDURE ETL_JOURNAL_CC_DTL_INIT() LANGUAGE SQLSCRIPT AS
BEGIN
	INSERT INTO "ETL_JOURNAL_CC_DTL"	
		SELECT 
				T2."TransId", T2."Line_ID", T2."AcctCode", T2."OcrCode", 
				T3."PrcCode", TO_NVARCHAR(T2."ValidFrom", 'YYYYMMDD'), T2."RefDate", T2."TaxDate", T2."DueDate",
				T2."Debit"*T3."PrcAmount"/T3."OcrTotal" AS "EXPENSE_LC",
				T2."SYSDeb"*T3."PrcAmount"/T3."OcrTotal" AS "EXPENSE_SC",
				T2."Credit"*T3."PrcAmount"/T3."OcrTotal" AS "REVENUE_LC",
				T2."SYSCred"*T3."PrcAmount"/T3."OcrTotal" AS "REVENUE_SC",
				(T2."Credit"-T2."Debit")*T3."PrcAmount"/T3."OcrTotal" AS "CA_ACTUAL_LC",
				(T2."SYSCred"-T2."SYSDeb")*T3."PrcAmount"/T3."OcrTotal" AS "CA_ACTUAL_SC",
				(CASE WHEN T2."Project" IS NULL THEN '-' WHEN Length(T2."Project") = 0 THEN '-' ELSE T2."Project" END) AS "Project",
				IFNULL(T2."BPLId", -1) AS "Branch",
				IFNULL(T2."Location", -1) AS "Location",
				T4."Code" AS "PeriodCode"
		FROM (
			SELECT
				T0."TransId", T0."Line_ID", T1."AcctCode", T0."ProfitCode" AS "OcrCode", T0."RefDate", T2."ValidFrom",
				T0."TaxDate", T0."DueDate", 
				(Case T1."ActType" WHEN 'E' THEN T0."SYSDeb"-T0."SYSCred" WHEN 'I' THEN 0 ELSE T0."SYSDeb" END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."SYSCred"-T0."SYSDeb" ELSE T0."SYSCred" END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN T0."Debit"-T0."Credit" WHEN 'I' THEN 0 ELSE T0."Debit" END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."Credit"-T0."Debit" ELSE T0."Credit" END) AS "Credit",
				T0."Project" AS "Project",
				T0."BPLId" AS "BPLId",
				T0."Location" AS "Location"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."ShortName" = T1."AcctCode"
			INNER JOIN 
			(
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM OCR1
				UNION ALL
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM MDR1
			)T2 ON T2."OcrCode" = T0."ProfitCode" 
			WHERE T0."RefDate" >= T2."ValidFrom" AND (T2."ValidTo" IS NULL OR T0."RefDate" <= T2."ValidTo")
			UNION ALL
			SELECT
				T0."TransId", T0."Line_ID", T1."AcctCode", T0."OcrCode2" AS "OcrCode", T0."RefDate", T2."ValidFrom",
				T0."TaxDate", T0."DueDate", 
				(Case T1."ActType" WHEN 'E' THEN T0."SYSDeb"-T0."SYSCred" WHEN 'I' THEN 0 ELSE T0."SYSDeb" END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."SYSCred"-T0."SYSDeb" ELSE T0."SYSCred" END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN T0."Debit"-T0."Credit" WHEN 'I' THEN 0 ELSE T0."Debit" END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."Credit"-T0."Debit" ELSE T0."Credit" END) AS "Credit",
				T0."Project" AS "Project",
				T0."BPLId" AS "BPLId",
				T0."Location" AS "Location"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."ShortName" = T1."AcctCode"
			INNER JOIN 
			(
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM OCR1
				UNION ALL
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM MDR1
			)T2 ON T2."OcrCode" = T0."OcrCode2" 
			WHERE T0."RefDate" >= T2."ValidFrom" AND (T2."ValidTo" IS NULL OR T0."RefDate" <= T2."ValidTo")
			UNION ALL
			SELECT
				T0."TransId", T0."Line_ID", T1."AcctCode", T0."OcrCode3" AS "OcrCode", T0."RefDate", T2."ValidFrom",
				T0."TaxDate", T0."DueDate", 
				(Case T1."ActType" WHEN 'E' THEN T0."SYSDeb"-T0."SYSCred" WHEN 'I' THEN 0 ELSE T0."SYSDeb" END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."SYSCred"-T0."SYSDeb" ELSE T0."SYSCred" END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN T0."Debit"-T0."Credit" WHEN 'I' THEN 0 ELSE T0."Debit" END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."Credit"-T0."Debit" ELSE T0."Credit" END) AS "Credit",
				T0."Project" AS "Project",
				T0."BPLId" AS "BPLId",
				T0."Location" AS "Location"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."ShortName" = T1."AcctCode"
			INNER JOIN 
			(
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM OCR1
				UNION ALL
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM MDR1
			)T2 ON T2."OcrCode" = T0."OcrCode3" 
			WHERE T0."RefDate" >= T2."ValidFrom" AND (T2."ValidTo" IS NULL OR T0."RefDate" <= T2."ValidTo")
			UNION ALL
			SELECT
				T0."TransId", T0."Line_ID", T1."AcctCode", T0."OcrCode4" AS "OcrCode", T0."RefDate", T2."ValidFrom",
				T0."TaxDate", T0."DueDate", 
				(Case T1."ActType" WHEN 'E' THEN T0."SYSDeb"-T0."SYSCred" WHEN 'I' THEN 0 ELSE T0."SYSDeb" END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."SYSCred"-T0."SYSDeb" ELSE T0."SYSCred" END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN T0."Debit"-T0."Credit" WHEN 'I' THEN 0 ELSE T0."Debit" END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."Credit"-T0."Debit" ELSE T0."Credit" END) AS "Credit",
				T0."Project" AS "Project",
				T0."BPLId" AS "BPLId",
				T0."Location" AS "Location"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."ShortName" = T1."AcctCode"
			INNER JOIN 
			(
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM OCR1
				UNION ALL
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM MDR1
			)T2 ON T2."OcrCode" = T0."OcrCode4"
			WHERE T0."RefDate" >= T2."ValidFrom" AND (T2."ValidTo" IS NULL OR T0."RefDate" <= T2."ValidTo")
			UNION ALL
			SELECT
				T0."TransId", T0."Line_ID", T1."AcctCode", T0."OcrCode5" AS "OcrCode", T0."RefDate", T2."ValidFrom",
				T0."TaxDate", T0."DueDate", 
				(Case T1."ActType" WHEN 'E' THEN T0."SYSDeb"-T0."SYSCred" WHEN 'I' THEN 0 ELSE T0."SYSDeb" END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."SYSCred"-T0."SYSDeb" ELSE T0."SYSCred" END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN T0."Debit"-T0."Credit" WHEN 'I' THEN 0 ELSE T0."Debit" END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN T0."Credit"-T0."Debit" ELSE T0."Credit" END) AS "Credit",
				T0."Project" AS "Project",
				T0."BPLId" AS "BPLId",
				T0."Location" AS "Location"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."ShortName" = T1."AcctCode"
			INNER JOIN 
			(
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM OCR1
				UNION ALL
				SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM MDR1
			)T2 ON T2."OcrCode" = T0."OcrCode5" 
			WHERE T0."RefDate" >= T2."ValidFrom" AND (T2."ValidTo" IS NULL OR T0."RefDate" <= T2."ValidTo")
		) T2
		INNER JOIN (
			SELECT T0."PrcCode", T0."OcrCode", T0."OcrTotal", T0."PrcAmount", T0."ValidFrom"
			FROM "OCR1" T0 WHERE T0."OcrTotal" <> 0
			UNION ALL
			SELECT T0."PrcCode", T0."OcrCode", T0."OcrTotal", T0."PrcAmount", T0."ValidFrom"
			FROM "MDR1" T0 WHERE T0."OcrTotal" <> 0 )T3 
			ON T2."OcrCode" = T3."OcrCode" AND T2."ValidFrom" = T3."ValidFrom"
		INNER JOIN "OFPR" T4 ON T4."F_RefDate" <= T2."RefDate" AND T4."T_RefDate" >= T2."RefDate"
		;
END
