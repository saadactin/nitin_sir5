CREATE PROCEDURE JDT1_JOURNAL_CC_DTL_DELTA() AS 
BEGIN 
DELETE FROM "ETL_JOURNAL_CC_DTL" T0 WHERE EXISTS (SELECT "TransId" FROM JDT1_JOURNAL_CC_DTL_DELTA_TABLE TEMP WHERE TEMP."TransId" = T0."TransId" AND TEMP."Line_ID" = T0."Line_ID") ;
INSERT INTO "ETL_JOURNAL_CC_DTL" ("TransId","Line_ID","AcctCode","OcrCode","PrcCode","ValidFrom","RefDate","TaxDate","DueDate","EXPENSE_LC","EXPENSE_SC","REVENUE_LC","REVENUE_SC","CA_ACTUAL_LC","CA_ACTUAL_SC","Project","Branch","Location","PeriodCode")
SELECT T22."TransId", 
		T22."Line_ID", 
		T22."AcctCode", 
		T22."OcrCode", 
		T3."PrcCode", 
		TO_NVARCHAR(T22."ValidFrom",'YYYYMMDD'),
		T22."RefDate",
		T22."TaxDate",
		T22."DueDate",
		T22."Debit"*T3."PrcAmount"/T3."OcrTotal" AS "EXPENSE_LC",
		T22."SYSDeb"*T3."PrcAmount"/T3."OcrTotal" AS "EXPENSE_SC", 
		T22."Credit"*T3."PrcAmount"/T3."OcrTotal" AS "REVENUE_LC", 
		T22."SYSCred"*T3."PrcAmount"/T3."OcrTotal" AS "REVENUE_SC",
	 (T22."Credit"-T22."Debit")*T3."PrcAmount"/T3."OcrTotal" AS "CA_ACTUAL_LC",
	 (T22."SYSCred"-T22."SYSDeb")*T3."PrcAmount"/T3."OcrTotal" AS "CA_ACTUAL_SC",
	 (CASE WHEN T22."Project" IS NULL THEN '-' 
	   WHEN Length(T22."Project") = 0 THEN '-' 
	  ELSE T22."Project" 
	  END) AS "Project",
	 IFNULL(T22."BPLId",-1) AS "Branch",
	 IFNULL(T22."Location",-1) AS "Location",
	 T4."Code" AS "PeriodCode" 
FROM ( SELECT T0."TransId", T0."Line_ID", T1."AcctCode", T2."OcrCode" AS "OcrCode", T0."RefDate",T2."ValidFrom",T0."TaxDate",T0."DueDate",
	 (Case T1."ActType" WHEN 'E' THEN T0."SYSDeb"-T0."SYSCred" 
	   WHEN 'I' THEN 0 
	  ELSE T0."SYSDeb" 
	  END) AS "SYSDeb",
	 (Case T1."ActType" WHEN 'E' THEN 0 
	   WHEN 'I' THEN T0."SYSCred"-T0."SYSDeb" 
		ELSE T0."SYSCred" 
	  END) AS "SYSCred",
	 (Case T1."ActType" WHEN 'E' THEN T0."Debit"-T0."Credit" 
	   WHEN 'I' THEN 0 
	  ELSE T0."Debit" 
	  END) AS "Debit",
	 (Case T1."ActType" WHEN 'E' THEN 0 
	   WHEN 'I' THEN T0."Credit"-T0."Debit" 
	  ELSE T0."Credit" 
	  END) AS "Credit",
	 T0."Project" AS "Project",
	 T0."BPLId" AS "BPLId",
	 T0."Location" AS "Location" 
	FROM "JDT1" T0 
	INNER JOIN "OACT" T1 ON T0."ShortName" = T1."AcctCode" 
	INNER JOIN ( SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM OCR1 
		          UNION ALL 
				 SELECT DISTINCT "OcrCode", "ValidFrom", "ValidTo" FROM MDR1 ) T2 
		ON (T2."OcrCode" = T0."ProfitCode" OR T2."OcrCode" = T0."OcrCode2" OR T2."OcrCode" = T0."OcrCode3" OR T2."OcrCode" = T0."OcrCode4" OR T2."OcrCode" = T0."OcrCode5" ) 
	AND T0."RefDate" >= T2."ValidFrom" 
	AND (T2."ValidTo" IS NULL OR T0."RefDate" <= T2."ValidTo") 
	AND EXISTS (SELECT 1 FROM JDT1_JOURNAL_CC_DTL_DELTA_TABLE TEMP WHERE T0."TransId" = TEMP."TransId" AND T0."Line_ID" = TEMP."Line_ID" ) 
	) T22 
INNER JOIN ( SELECT T0."PrcCode", T0."OcrCode", T0."OcrTotal", T0."PrcAmount", T0."ValidFrom" AS "ValidFrom" FROM "OCR1" T0 WHERE T0."OcrTotal" <> 0
				UNION ALL 
			 SELECT T0."PrcCode", T0."OcrCode", T0."OcrTotal", T0."PrcAmount", T0."ValidFrom" AS "ValidFrom" FROM "MDR1" T0 WHERE T0."OcrTotal" <> 0) T3 
ON T22."OcrCode" = T3."OcrCode" AND T22."ValidFrom" = T3."ValidFrom" 
INNER JOIN "OFPR" T4 ON T4."F_RefDate" <= T22."RefDate" AND T4."T_RefDate" >= T22."RefDate" ;			
END
