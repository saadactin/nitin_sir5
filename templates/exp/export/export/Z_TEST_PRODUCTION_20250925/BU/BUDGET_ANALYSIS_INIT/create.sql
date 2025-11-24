CREATE PROCEDURE BUDGET_ANALYSIS_INIT() LANGUAGE SQLSCRIPT AS
BEGIN
        JDT_Line=SELECT 
				T2."AcctCode", T2."OcrCode", T2."DimCode" AS "DimCode", T3."GrpCode",
				IFNULL(T3."PrcCode", '-') AS "PrcCode", T2."RefDate", 
				(T2."Credit"-T2."Debit")*IFNULL(T3."PrcAmount",100)/IFNULL(T3."OcrTotal",100) AS "CA_ACTUAL_LC",
				(T2."SYSCred"-T2."SYSDeb")*IFNULL(T3."PrcAmount",100)/IFNULL(T3."OcrTotal",100) AS "CA_ACTUAL_SC"
		FROM (
			SELECT
				T1."AcctCode", T0."ProfitCode" AS "OcrCode", T0."RefDate", 
				1 AS "DimCode",
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."SYSDeb")-SUM(T0."SYSCred") WHEN 'I' THEN 0 ELSE SUM(T0."SYSDeb") END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."SYSCred")-SUM(T0."SYSDeb") ELSE SUM(T0."SYSCred") END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."Debit")-SUM(T0."Credit") WHEN 'I' THEN 0 ELSE SUM(T0."Debit") END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."Credit")-SUM(T0."Debit") ELSE SUM(T0."Credit") END) AS "Credit"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."Account" = T1."AcctCode"
			GROUP BY T1."AcctCode", T1."ActType", T0."ProfitCode", T0."RefDate"
			UNION ALL
			SELECT
				T1."AcctCode", T0."OcrCode2" AS "OcrCode", T0."RefDate", 
				2 AS "DimCode",
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."SYSDeb")-SUM(T0."SYSCred") WHEN 'I' THEN 0 ELSE SUM(T0."SYSDeb") END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."SYSCred")-SUM(T0."SYSDeb") ELSE SUM(T0."SYSCred") END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."Debit")-SUM(T0."Credit") WHEN 'I' THEN 0 ELSE SUM(T0."Debit") END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."Credit")-SUM(T0."Debit") ELSE SUM(T0."Credit") END) AS "Credit"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."Account" = T1."AcctCode"
			GROUP BY T1."AcctCode", T1."ActType", T0."OcrCode2", T0."RefDate"
			UNION ALL
			SELECT
				T1."AcctCode", T0."OcrCode3" AS "OcrCode", T0."RefDate", 
				3 AS "DimCode",
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."SYSDeb")-SUM(T0."SYSCred") WHEN 'I' THEN 0 ELSE SUM(T0."SYSDeb") END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."SYSCred")-SUM(T0."SYSDeb") ELSE SUM(T0."SYSCred") END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."Debit")-SUM(T0."Credit") WHEN 'I' THEN 0 ELSE SUM(T0."Debit") END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."Credit")-SUM(T0."Debit") ELSE SUM(T0."Credit") END) AS "Credit"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."Account" = T1."AcctCode"
			GROUP BY T1."AcctCode", T1."ActType", T0."OcrCode3", T0."RefDate"
			UNION ALL
			SELECT
				T1."AcctCode", T0."OcrCode4" AS "OcrCode", T0."RefDate", 
				4 AS "DimCode",
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."SYSDeb")-SUM(T0."SYSCred") WHEN 'I' THEN 0 ELSE SUM(T0."SYSDeb") END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."SYSCred")-SUM(T0."SYSDeb") ELSE SUM(T0."SYSCred") END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."Debit")-SUM(T0."Credit") WHEN 'I' THEN 0 ELSE SUM(T0."Debit") END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."Credit")-SUM(T0."Debit") ELSE SUM(T0."Credit") END) AS "Credit"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."Account" = T1."AcctCode"
			GROUP BY T1."AcctCode", T1."ActType", T0."OcrCode4", T0."RefDate"
			UNION ALL
			SELECT
				T1."AcctCode", T0."OcrCode5" AS "OcrCode", T0."RefDate", 
				5 AS "DimCode",
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."SYSDeb")-SUM(T0."SYSCred") WHEN 'I' THEN 0 ELSE SUM(T0."SYSDeb") END) AS "SYSDeb", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."SYSCred")-SUM(T0."SYSDeb") ELSE SUM(T0."SYSCred") END) AS "SYSCred", 
				(Case T1."ActType" WHEN 'E' THEN SUM(T0."Debit")-SUM(T0."Credit") WHEN 'I' THEN 0 ELSE SUM(T0."Debit") END) AS "Debit", 
				(Case T1."ActType" WHEN 'E' THEN 0 WHEN 'I' THEN SUM(T0."Credit")-SUM(T0."Debit") ELSE SUM(T0."Credit") END) AS "Credit"
			FROM "JDT1" T0
			INNER JOIN "OACT" T1 ON T0."Account" = T1."AcctCode"
			GROUP BY T1."AcctCode", T1."ActType", T0."OcrCode5", T0."RefDate"
			) T2

		LEFT OUTER JOIN (
				SELECT T1."PrcCode",  T0."OcrCode", T0."OcrTotal", T0."PrcAmount", T1."DimCode", CAST(IFNULL(T1."GrpCode",'-') AS NVARCHAR(4)) AS "GrpCode"
				FROM "OCR1" T0
				INNER JOIN "OPRC" T1 ON T0."PrcCode" = T1."PrcCode" WHERE T0."OcrTotal" <> 0
				UNION ALL
				SELECT T1."PrcCode",  T0."OcrCode", T0."OcrTotal", T0."PrcAmount", T1."DimCode", CAST('-' AS NVARCHAR(4)) AS "GrpCode"
				FROM "MDR1" T0 
				INNER JOIN "OPRC" T1 ON T0."PrcCode" = T1."PrcCode" WHERE T0."OcrTotal" <> 0
			)T3 ON T2."OcrCode" = T3."OcrCode";
	
		Budget_On_Account = SELECT	T0."AcctCode" AS "ACCOUNT_CODE", 
									T0."Instance" AS "BUDGET_SCENARIO_ID",
									ADD_MONTHS(T0."FinancYear", T1."Line_ID") AS "BUDGET_DAY",
									T3."DimCode" AS "DIM_CODE",
									IFNULL(MAX(T1."DebLTotal") - MAX(T1."CredLTotal"),0) AS "BUDGET_AMOUNT_LC", 
									IFNULL(MAX(T1."DebSTotal") - MAX(T1."CredSTotal"),0) AS "BUDGET_AMOUNT_SC",
									IFNULL((CASE WHEN T2."ActType" = 'I' THEN MAX(T1."FtrIDRLSum") - MAX(T1."FtrICRLSum") ELSE MAX(T1."FtrODRLSum") - MAX(T1."FtrOCRLSum") END),0) AS "FUTURE_AMOUNT_LC",
									IFNULL((CASE WHEN T2."ActType" = 'I' THEN MAX(T1."FtrIDRSSum") - MAX(T1."FtrICRSSum") ELSE MAX(T1."FtrODRSSum") - MAX(T1."FtrOCRSSum") END),0) AS "FUTURE_AMOUNT_SC"
							FROM "OBGT" T0
							INNER JOIN "BGT1" T1 ON T0."AbsId" = T1."BudgId" AND T0."AcctCode" = T1."AcctCode"
							INNER JOIN "OACT" T2 ON T2."AcctCode" = T1."AcctCode"
							INNER JOIN "ODIM" T3 ON 1 = 1 AND "DimActive" = 'Y'
							GROUP BY T3."DimCode",T0."AcctCode", T0."Instance", T0."FinancYear", T1."Line_ID", T2."ActType";	
							
	 
		Budget_Details = SELECT	T0."AcctCode" AS "ACCOUNT_CODE", 
							T0."Instance" AS "BUDGET_SCENARIO_ID",
							ADD_MONTHS(T0."FinancYear", T1."Line_ID") AS "BUDGET_DAY",
		
							(CASE IFNULL(MAX(T3."OcrTotal"), 0) WHEN 0 THEN 0 ELSE (IFNULL(MAX(T1."DebLTotal"),0) - IFNULL(MAX(T1."CredLTotal"),0))* IFNULL(MAX(T3."PrcAmount"),0)/MAX(T3."OcrTotal") END) AS "BUDGET_AMOUNT_LC", 
							(CASE IFNULL(MAX(T3."OcrTotal"), 0) WHEN 0 THEN 0 ELSE (IFNULL(MAX(T1."DebSTotal"),0) - IFNULL(MAX(T1."CredSTotal"),0))* IFNULL(MAX(T3."PrcAmount"),0)/MAX(T3."OcrTotal") END) AS "BUDGET_AMOUNT_SC", 
							0 AS "FUTURE_AMOUNT_LC",
							0 AS "FUTURE_AMOUNT_SC",
							--IFNULL(-1*SUM(T4."CA_ACTUAL_LC"),0) AS "ACTUAL_AMOUNT_LC",
							--IFNULL(-1*SUM(T4."CA_ACTUAL_SC"),0) AS "ACTUAL_AMOUNT_SC",
							
							T1."DimCode" AS "DIM_CODE",
							T3."PrcCode" AS "PRC_CODE",
							MAX(T3."GrpCode") AS "SORT_CODE"
							FROM "OBGT" T0
							INNER JOIN "BGT1" T5 ON T0."AbsId" = T5."BudgId" AND T0."AcctCode" = T5."AcctCode"
							INNER JOIN "BGT3" T1 ON T0."AbsId" = T1."BudgId"
							INNER JOIN (
								SELECT T1."PrcCode", T1."PrcName",T0."OcrCode", T0."OcrTotal", T0."PrcAmount", T1."DimCode", CAST(IFNULL(T1."GrpCode",'-') AS NVARCHAR(4)) AS "GrpCode"
								FROM "OCR1" T0 
								INNER JOIN "OPRC" T1 ON T0."PrcCode" = T1."PrcCode"
								UNION ALL
								SELECT T1."PrcCode", T1."PrcName",T0."OcrCode", T0."OcrTotal", T0."PrcAmount", T1."DimCode", CAST('-' AS NVARCHAR(4)) AS "GrpCode"
								FROM "MDR1" T0
								INNER JOIN "OPRC" T1 ON T0."PrcCode" = T1."PrcCode") T3 ON T1."OcrCode" = T3."OcrCode"
							 
							GROUP BY T1."DimCode",T0."AcctCode", T0."Instance", T0."FinancYear", T1."Line_ID", 
									 T1."OcrCode", T3."PrcCode";
			
		Actual_Amount_Details = SELECT	T0."AcctCode" AS "ACCOUNT_CODE", 
							T0."Instance" AS "BUDGET_SCENARIO_ID",
							ADD_MONTHS(T0."FinancYear", T5."Line_ID") AS "BUDGET_DAY",
		
							-1*SUM(IFNULL(T4."CA_ACTUAL_LC",0)) AS "ACTUAL_AMOUNT_LC",
							-1*SUM(IFNULL(T4."CA_ACTUAL_SC",0)) AS "ACTUAL_AMOUNT_SC",
							
							IFNULL(T4."DimCode", 0) AS "DIM_CODE",
							T4."PrcCode" AS "PRC_CODE",
							MAX(T4."GrpCode") AS "SORT_CODE"
							FROM "OBGT" T0
							INNER JOIN "BGT1" T5 ON T0."AbsId" = T5."BudgId" AND T0."AcctCode" = T5."AcctCode"
							INNER JOIN :JDT_Line T4 ON LAST_DAY(T4."RefDate") = LAST_DAY(ADD_MONTHS(T0."FinancYear", T5."Line_ID")) 
															 AND T4."AcctCode" = T0."AcctCode"
							GROUP BY T0."AcctCode", T0."Instance", T0."FinancYear", T5."Line_ID", 
									 T4."DimCode",T4."OcrCode", T4."PrcCode";
										
		Remaining_Summary_On_Account = 	SELECT T0."ACCOUNT_CODE", T0."BUDGET_SCENARIO_ID", T0."BUDGET_DAY", 
										IFNULL(MAX(T0."BUDGET_AMOUNT_LC"), 0) - IFNULL(SUM(T1."BUDGET_AMOUNT_LC"), 0) AS "REMAINING_BUDGET_LC",
										IFNULL(MAX(T0."BUDGET_AMOUNT_SC"), 0) - IFNULL(SUM(T1."BUDGET_AMOUNT_SC"), 0) AS "REMAINING_BUDGET_SC",
										MAX(T0."FUTURE_AMOUNT_LC") AS "FUTURE_AMOUNT_LC",
										MAX(T0."FUTURE_AMOUNT_SC") AS "FUTURE_AMOUNT_SC",
										0 AS "ACTUAL_AMOUNT_LC",
										0 AS "ACTUAL_AMOUNT_SC",
										
										T0."DIM_CODE" AS "DIMCODE",
										'-' AS "PRC_CODE",
										'-' AS "SORT_CODE"
										FROM :Budget_On_Account T0
										LEFT OUTER JOIN :Budget_Details T1
										ON T0."BUDGET_SCENARIO_ID" = T1."BUDGET_SCENARIO_ID"
										AND T0."ACCOUNT_CODE" = T1."ACCOUNT_CODE" 
										AND T0."BUDGET_DAY" = T1."BUDGET_DAY"
										AND T0."DIM_CODE" = T1."DIM_CODE"
										GROUP BY T0."BUDGET_SCENARIO_ID",T0."ACCOUNT_CODE",  T0."BUDGET_DAY", T0."DIM_CODE";
		
	
		INSERT INTO ETL_BUDGET_ANALYSIS
		SELECT T0."ACCOUNT_CODE", T0."BUDGET_SCENARIO_ID", T0."BUDGET_DAY", 
							IFNULL(T1."BUDGET_AMOUNT_LC",0), 
							IFNULL(T1."BUDGET_AMOUNT_SC",0), 
							0 AS "FUTURE_AMOUNT_LC",
							0 AS "FUTURE_AMOUNT_SC",
							0 AS "ACTUAL_AMOUNT_LC",
							0 AS "ACTUAL_AMOUNT_SC",
						
							T0."DIM_CODE" AS "DIMCODE",
							IFNULL(T1."PRC_CODE", '-') AS "PRC_CODE",
							IFNULL(T1."SORT_CODE",'-')
							FROM :Budget_On_Account T0
							LEFT OUTER JOIN :Budget_Details T1
							ON T0."BUDGET_SCENARIO_ID" = T1."BUDGET_SCENARIO_ID"
							AND T0."ACCOUNT_CODE" = T1."ACCOUNT_CODE" 
							AND T0."BUDGET_DAY" = T1."BUDGET_DAY"
							AND T0."DIM_CODE" = T1."DIM_CODE"
							--GROUP BY T0."BUDGET_SCENARIO_ID",T0."ACCOUNT_CODE",  T0."BUDGET_DAY", T0."DIM_CODE", T1."PRC_CODE"
		UNION ALL
		SELECT T0."ACCOUNT_CODE", T0."BUDGET_SCENARIO_ID", T0."BUDGET_DAY", 
							0 AS "BUDGET_AMOUNT_LC", 
							0 AS "BUDGET_AMOUNT_SC", 
							0 AS "FUTURE_AMOUNT_LC",
							0 AS "FUTURE_AMOUNT_SC",
							IFNULL(T1."ACTUAL_AMOUNT_LC",0),
							IFNULL(T1."ACTUAL_AMOUNT_SC",0),
						
							T0."DIM_CODE" AS "DIMCODE",
							IFNULL(T1."PRC_CODE", '-') AS "PRC_CODE",
							IFNULL(T1."SORT_CODE", '-')
							FROM :Budget_On_Account T0
							LEFT OUTER JOIN :Actual_Amount_Details T1
							ON T0."BUDGET_SCENARIO_ID" = T1."BUDGET_SCENARIO_ID"
							AND T0."ACCOUNT_CODE" = T1."ACCOUNT_CODE" 
							AND T0."BUDGET_DAY" = T1."BUDGET_DAY"
							AND T0."DIM_CODE" = T1."DIM_CODE"
							--GROUP BY T0."BUDGET_SCENARIO_ID",T0."ACCOUNT_CODE",  T0."BUDGET_DAY", T0."DIM_CODE", T1."PRC_CODE"
		UNION ALL
		SELECT * FROM :Remaining_Summary_On_Account;
		
END
