-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2
CREATE PROCEDURE CRSP_ProfitAndLostByCC_BR_enUS_cont(
in Currency nvarchar(10),
in IgnoreAdj varchar(1),
in AddVoucher varchar(1),
in DateType varchar(256),
in DimCode nvarchar(10),
in BudgId int,
in FromDate timeStamp,
in ToDate timeStamp,
in BroughtForward varchar(1),
in OcrCode nvarchar(5000),
in PrcCode nvarchar(5000),
in PrjCode nvarchar(5000),
in EndYear varchar(1),
in FinancYear timestamp
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
BEGIN	
	/* Amount Brought Forward */
	INSERT INTO "CRSPProfitLostByCCBR_JDT1"
    SELECT T0."TransId", T0."Line_ID", NULL AS "RefDate", NULL AS "FinancYear", T0."Account" AS "Account", 
        'BFJDT1' AS "Typ", IFNULL(T2."PrcCode", '') AS "PrcCode", IFNULL(T2."OcrCode", '') AS "OcrCode", 
        IFNULL(T0."Project", '') AS "PrjCode", 0 AS "Credit", 0 AS "CreditPreviousYear", 0 AS "Debit", 
        0 AS "DebitPreviousYear", 0 AS "CreditBudget", 0 AS "DebitBudget", 0 AS "CreditBudgetPrevYear", 
        0 AS "DebitBudgetPrevYear", 0 AS "CreditActualMonth", 0 AS "CreditMonthPreviousYear", 
        0 AS "DebitActualMonth", 0 AS "DebitMonthPreviousYear", 0 AS "CreditActualMonthBudget", 
        0 AS "DebitActualMonthBudget", 0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 
        0 AS "CreditFinancYearBudget", 0 AS "DebitFinancYearBudget", 0 AS "CreditPrevFinancYear", 
        0 AS "DebitPrevFinancYear", 
		/* Case for different Date Types depending on the Parameter @DateType */
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END < :FromDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "BFDebit", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END < :FromDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "BFCredit",
		/* Opening Balance for selected Period */
		0 AS "OBDebit", 0 AS "OBCredit",
		/* Opening Balance Brought Forward This is still included in the BFCredit / BFDebit for compatibility reason */
        CASE 
            WHEN T0."TransType" = '-2' AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END < :FromDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "BFOBDebit", 
        CASE 
            WHEN T0."TransType" = '-2' AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END < :FromDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "BFOBCredit" 
    FROM JDT1 T0 
	/* Link the Distribution Rule and Amounts*/
	LEFT OUTER JOIN "CRSPProfitLostByCCBR_OCR1DistRule" T2 ON T2."OcrCode" = 
		CASE :DimCode 
			WHEN '2' THEN T0."OcrCode2" 
			WHEN '3' THEN T0."OcrCode3" 
			WHEN '4' THEN T0."OcrCode4" 
			WHEN '5' THEN T0."OcrCode5" 
			ELSE T0."ProfitCode" 
		END AND 
		CASE :DateType 
			WHEN 'DueDate' THEN T0."DueDate" 
			WHEN 'TaxDate' THEN T0."TaxDate" 
			ELSE T0."RefDate" 
		END >= T2."ValidFrom" AND 
		CASE :DateType 
			WHEN 'DueDate' THEN T0."DueDate" 
			WHEN 'TaxDate' THEN T0."TaxDate" 
			ELSE T0."RefDate" 
		END <= T2."ValidTo" 
	INNER JOIN "CRSPProfitLostByCCBR_Plan" T4 ON T0."Account" = T4."AcctCode" 
    WHERE 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END < :FromDate
		/* Exclude Adj. Journals */
		AND ((:IgnoreAdj = 'Y' AND T0."AdjTran" = 'N') OR (:IgnoreAdj = 'N') OR (:IgnoreAdj = 'O' AND
         T0."AdjTran" = 'Y'))
		/* Exclude Endyear Closings */ 
		 AND ((T4."DocType" = 'B' AND NOT (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END = ADD_YEARS(ADD_DAYS(:FinancYear, -1), 1) AND T0."TransType" = -3) OR (T4."DocType" = 'P' AND
         NOT T0."TransType" = -3)) OR :EndYear = 'Y') 
		 /* TODO: Check if this can be implemented with Crystal */
		 AND (T2."OcrCode" IN (:OcrCode) OR :OcrCode = '') 
		 AND (T2."PrcCode" IN (:PrcCode) OR :PrcCode = '') 
		 AND (T0."Project" IN (:PrjCode) OR :PrjCode = '') 
		 AND :BroughtForward = 'Y'

	/* Journals (JDT1) Financ Year*/
    UNION
    SELECT T0."TransId", T0."Line_ID", 
		/* Create Date (01.MM.YYYY) */
		CAST('01.' || CAST(MONTH(
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END) AS varchar) || '.' || CAST(YEAR(
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END) AS varchar) AS timestamp) AS "RefDate", :FinancYear AS "FinancYear", T0."Account" AS "Account", 
        'JDT1' AS "Typ", 
		/* Actual Periode FormDate - ToDate */
		IFNULL(T2."PrcCode", '') AS "PrcCode", 
		IFNULL(T2."OcrCode", '') AS "OcrCode", 
        IFNULL(T0."Project", '') AS "PrjCode", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FromDate AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= :ToDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "Credit",
		/* Lastyear Periode FormDate - ToDate */
		0 AS "CreditPreviousYear", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FromDate AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= :ToDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "Debit", 
		0 AS "DebitPreviousYear", 0 AS "CreditBudget", 0 AS "DebitBudget", 
        0 AS "CreditBudgetPrevYear", 0 AS "DebitBudgetPrevYear", 
		/* Actual Month Month(ToDate) */
        CASE 
            WHEN MONTH(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = MONTH(:ToDate) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditActualMonth", 
		/* Credit Month Previous Year(ToDate) */
		0 AS "CreditMonthPreviousYear", 
        CASE 
            WHEN MONTH(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = MONTH(:ToDate) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitActualMonth", 0 AS "DebitMonthPreviousYear", 0 AS "CreditActualMonthBudget", 
        0 AS "DebitActualMonthBudget",
		/* Full Finance Year */
		/* muf: Modified to allow selection of a period greater than a Financial Year */
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FinancYear AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= ADD_DAYS(ADD_YEARS(:FinancYear, 1), -1) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditFinancYear", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FinancYear AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= ADD_DAYS(ADD_YEARS(:FinancYear, 1), -1) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitFinancYear", 0 AS "CreditFinancYearBudget", 
		/* Prev. Year */
		0 AS "DebitFinancYearBudget", 
        0 AS "CreditPrevFinancYear", 0 AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit", 
		/* Opening Balance for selected Period 
		   This is still included in the Credit / Debit for compatibility reason */
        CASE 
            WHEN T0."TransType" = '-2' AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FromDate AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= :ToDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "OBDebit", 
        CASE 
            WHEN T0."TransType" = '-2' AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FromDate AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= :ToDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "OBCredit",
		/* Opening Balance Brought Forward */
		0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM JDT1 T0 
		/* Link the Distribution Rule and Amounts*/
        LEFT OUTER JOIN "CRSPProfitLostByCCBR_OCR1DistRule" T2 ON T2."OcrCode" = 
            CASE :DimCode 
                WHEN '2' THEN T0."OcrCode2" 
                WHEN '3' THEN T0."OcrCode3" 
                WHEN '4' THEN T0."OcrCode4" 
                WHEN '5' THEN T0."OcrCode5" 
                ELSE T0."ProfitCode" 
            END AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= T2."ValidFrom" AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= T2."ValidTo"
			/* Link the Chart of Account Structure */
        INNER JOIN "CRSPProfitLostByCCBR_Plan" T4 ON T0."Account" = T4."AcctCode" 
    WHERE (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END >= :FromDate OR 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END >= :FinancYear) AND (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END <= :ToDate OR 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END <= ADD_DAYS(ADD_YEARS(:FinancYear, 1), -1)) 
		/* Exclude Adj. Journals */
		AND ((:IgnoreAdj = 'Y' AND T0."AdjTran" = 'N') OR
         (:IgnoreAdj = 'N') OR (:IgnoreAdj = 'O' AND T0."AdjTran" = 'Y')) 
		/* Exclude Endyear Closings */
		 AND ((T4."DocType" = 'B' AND NOT (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END = ADD_YEARS(ADD_DAYS(:FinancYear, -1), 1) AND T0."TransType" = -3) OR (T4."DocType" = 'P' AND
         NOT T0."TransType" = -3)) OR :EndYear = 'Y')
		/* TODO: Check if this can be implemented with Crystal */ 
		 AND (T2."OcrCode" IN (:OcrCode) OR :OcrCode = '') 
		 AND (T2."PrcCode" IN (:PrcCode) OR :PrcCode = '') 
		 AND (T0."Project" IN (:PrjCode) OR :PrjCode = '') 
		 
    UNION ALL 
	
	/* Journal Prev Year*/
    SELECT T0."TransId", T0."Line_ID",
		/* Create Date (01.MM.YYYY) */
		CAST('01.' || CAST(MONTH(
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END) AS varchar) || '.' || CAST(YEAR(
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END) AS varchar) AS timestamp) AS "RefDate", ADD_YEARS(:FinancYear, -1) AS "FinancYear", 
        T0."Account" AS "Account", 'JDT1' AS "Typ", IFNULL(T2."PrcCode", '') AS "PrcCode", 
        IFNULL(T2."OcrCode", '') AS "OcrCode", IFNULL(T0."Project", '') AS "PrjCode", 
		/* Actual Periode FormDate - ToDate */
		0 AS "Credit", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= ADD_YEARS(:FromDate, -1) AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= ADD_YEARS(:ToDate, -1) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditPreviousYear", 0 AS "Debit", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= ADD_YEARS(:FromDate, -1) AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= ADD_YEARS(:ToDate, -1) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitPreviousYear", 0 AS "CreditBudget", 0 AS "DebitBudget", 0 AS "CreditBudgetPrevYear", 
        0 AS "DebitBudgetPrevYear", 
		/* Actual Month Month(ToDate) */
		0 AS "CreditActualMonth", 
        CASE 
            WHEN MONTH(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = MONTH(:ToDate) AND YEAR(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = YEAR(ADD_YEARS(:ToDate, -1)) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditMonthPreviousYear", 0 AS "DebitActualMonth", 
        CASE 
            WHEN MONTH(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = MONTH(:ToDate) AND YEAR(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = YEAR(ADD_YEARS(:ToDate, -1)) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitMonthPreviousYear", 0 AS "CreditActualMonthBudget", 0 AS "DebitActualMonthBudget",
		/* Full Finance Year */
        0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 0 AS "CreditFinancYearBudget", 0 AS "DebitFinancYearBudget",
		/* Preview Year */
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= ADD_YEARS(:FinancYear, -1) AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END < :FinancYear THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditPrevFinancYear", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= ADD_YEARS(:FinancYear, -1) AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END < :FinancYear THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit", 
		/* Opening Balance for selected Period */
		0 AS "OBDebit", 0 AS "OBCredit", 
		/* Opening Balance Brought Forward */
        0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM JDT1 T0
		/* Link the Distribution Rule and Amounts*/
        LEFT OUTER JOIN "CRSPProfitLostByCCBR_OCR1DistRule" T2 ON T2."OcrCode" = 
            CASE :DimCode 
                WHEN '2' THEN T0."OcrCode2" 
                WHEN '3' THEN T0."OcrCode3" 
                WHEN '4' THEN T0."OcrCode4" 
                WHEN '5' THEN T0."OcrCode5" 
                ELSE T0."ProfitCode" 
            END AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= T2."ValidFrom" AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= T2."ValidTo" 
        INNER JOIN "CRSPProfitLostByCCBR_Plan" T4 ON T0."Account" = T4."AcctCode" 
    WHERE (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END >= ADD_YEARS(:FinancYear, -1) OR 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END >= ADD_YEARS(:FromDate, -1)) AND (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END < :FinancYear OR 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END < ADD_YEARS(:ToDate, -1)) 
		/* Exclude Adj. Journals */
		AND ((:IgnoreAdj = 'Y' AND T0."AdjTran" = 'N') OR (:IgnoreAdj = 'N') OR
         (:IgnoreAdj = 'O' AND T0."AdjTran" = 'Y')) 
		/* Exclude Endyear Closings */  
		AND ((T4."DocType" = 'B' AND NOT (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END = ADD_YEARS(ADD_DAYS(:FinancYear, -1), 1) AND T0."TransType" = -3) OR (T4."DocType" = 'P' AND
         NOT T0."TransType" = -3)) OR :EndYear = 'Y') 
		/* TODO: Check if this can be implemented with Crystal */ 
		 AND (T2."OcrCode" IN (:OcrCode) OR :OcrCode = '') 
		 AND (T2."PrcCode" IN (:PrcCode) OR :PrcCode = '') 
		 AND (T0."Project" IN (:PrjCode) OR :PrjCode = '')
		 
    UNION ALL 
	
	/* Journal Vouchers */
    SELECT T0."TransId", T0."Line_ID", 
		/* Create Date (01.MM.YYYY) */
		CAST('01.' || CAST(MONTH(
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END) AS varchar) || '.' || CAST(YEAR(
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END) AS varchar) AS timestamp) AS "RefDate", :FinancYear AS "FinancYear", T0."Account" AS "Account", 
        'BTF1' AS "Typ", IFNULL(T2."PrcCode", '') AS "PrcCode", IFNULL(T2."OcrCode", '') AS "OcrCode", 
        IFNULL(T0."Project", '') AS "PrjCode",
		/* From - ToDate */
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FromDate AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= :ToDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "Credit", 0 AS "CreditPreviousYear", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FromDate AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= :ToDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "Debit", 0 AS "DebitPreviousYear", 0 AS "CreditBudget", 0 AS "DebitBudget", 
        0 AS "CreditBudgetPrevYear", 0 AS "DebitBudgetPrevYear", 
		/* Month(ToDate) */
        CASE 
            WHEN MONTH(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = MONTH(:ToDate) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditActualMonth", 
		/* Month(ToDate) */
		0 AS "CreditMonthPreviousYear", 
        CASE 
            WHEN MONTH(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = MONTH(:ToDate) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitActualMonth", 0 AS "DebitMonthPreviousYear", 0 AS "CreditActualMonthBudget", 
        0 AS "DebitActualMonthBudget", 
		/* Full Finance Year */
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FinancYear AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= ADD_DAYS(ADD_YEARS(:FinancYear, 1), -1) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditFinancYear", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FinancYear AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= ADD_DAYS(ADD_YEARS(:FinancYear, 1), -1) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitFinancYear", 0 AS "CreditFinancYearBudget", 0 AS "DebitFinancYearBudget",
		/* Preview Year */
        0 AS "CreditPrevFinancYear", 0 AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit",
		/* Opening Balance for selected Period */
		/* Opening Balance is still included in the Debit / Credit for compatibility reason */
        CASE 
            WHEN T0."TransType" = '-2' AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FromDate AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= :ToDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "OBDebit", 
        CASE 
            WHEN T0."TransType" = '-2' AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= :FromDate AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= :ToDate THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "OBCredit", 
		/* Opening Balance Brought Forward */
		0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM BTF1 T0 
        INNER JOIN OBTF T10 ON T0."TransId" = T10."TransId" AND T0."BatchNum" = T10."BatchNum" 
        LEFT OUTER JOIN "CRSPProfitLostByCCBR_OCR1DistRule" T2 ON T2."OcrCode" = 
            CASE :DimCode 
                WHEN '2' THEN T0."OcrCode2" 
                WHEN '3' THEN T0."OcrCode3" 
                WHEN '4' THEN T0."OcrCode4" 
                WHEN '5' THEN T0."OcrCode5" 
                ELSE T0."ProfitCode" 
            END AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= T2."ValidFrom" AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= T2."ValidTo" 
        INNER JOIN "CRSPProfitLostByCCBR_Plan" T4 ON T0."Account" = T4."AcctCode" 
    WHERE (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END >= :FinancYear OR 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END >= :FromDate) AND (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END <= ADD_DAYS(ADD_YEARS(:FinancYear, 1), -1) OR 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END <= :ToDate) AND ((:IgnoreAdj = 'Y' AND T0."AdjTran" = 'N') OR (:IgnoreAdj = 'N') OR (:IgnoreAdj = 'O' AND
         T0."AdjTran" = 'Y')) AND ((T4."DocType" = 'B' AND NOT (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END = ADD_YEARS(ADD_DAYS(:FinancYear, -1), 1) AND T0."TransType" = -3) OR (T4."DocType" = 'P' AND
         NOT T0."TransType" = -3)) OR :EndYear = 'Y') 
		 AND (T2."OcrCode" IN (:OcrCode) OR :OcrCode = '') 
		 AND (T2."PrcCode" IN (:PrcCode) OR :PrcCode = '') 
		 AND (T0."Project" IN (:PrjCode) OR :PrjCode = '') 
		 AND T10."BtfStatus" = 'O' AND :AddVoucher = 'Y' 
		 
    UNION ALL 
	
	/* Journal Vouchers Prev Year*/
    SELECT T0."TransId", T0."Line_ID", 
		/* Create Date (01.MM.YYYY) */
		CAST('01.' || CAST(MONTH(
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END) AS varchar) || '.' || CAST(YEAR(
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END) AS varchar) AS timestamp) AS "RefDate", ADD_YEARS(:FinancYear, -1) AS "FinancYear", 
        T0."Account" AS "Account", 'BTF1' AS "Typ", 
		IFNULL(T2."PrcCode", '') AS "PrcCode", 
        IFNULL(T2."OcrCode", '') AS "OcrCode", 
		IFNULL(T0."Project", '') AS "PrjCode", 
		/* From - ToDate */
		0 AS "Credit", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= ADD_YEARS(:FromDate, -1) AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= ADD_YEARS(:ToDate, -1) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditPreviousYear", 0 AS "Debit", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= ADD_YEARS(:FromDate, -1) AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= ADD_YEARS(:ToDate, -1) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitPreviousYear", 0 AS "CreditBudget", 0 AS "DebitBudget", 
		0 AS "CreditBudgetPrevYear", 0 AS "DebitBudgetPrevYear", 
		/* Month(ToDate) */
		0 AS "CreditActualMonth", 
        CASE 
            WHEN MONTH(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = MONTH(:ToDate) AND YEAR(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = YEAR(ADD_YEARS(:ToDate, -1)) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditMonthPreviousYear", 0 AS "DebitActualMonth", 
        CASE 
            WHEN MONTH(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = MONTH(:ToDate) AND YEAR(
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END) = YEAR(ADD_YEARS(:ToDate, -1)) THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitMonthPreviousYear", 0 AS "CreditActualMonthBudget", 0 AS "DebitActualMonthBudget",
		/* Full Finance Year */	
        0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 0 AS "CreditFinancYearBudget", 0 AS "DebitFinancYearBudget", 
		/* Prev. Year */
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= ADD_YEARS(:FinancYear, -1) AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END < :FinancYear THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSCred" 
                ELSE T0."Credit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditPrevFinancYear", 
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= ADD_YEARS(:FinancYear, -1) AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END < :FinancYear THEN 
            CASE 
                WHEN :Currency = 'Sys' THEN T0."SYSDeb" 
                ELSE T0."Debit" 
            END * IFNULL(T2."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit",
		/* Opening Balance for selected Period */
		0 AS "OBDebit", 0 AS "OBCredit", 
		/* Opening Balance Brought Forward */
        0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM BTF1 T0 
        INNER JOIN OBTF T10 ON T0."TransId" = T10."TransId" AND T0."BatchNum" = T10."BatchNum" 
        LEFT OUTER JOIN "CRSPProfitLostByCCBR_OCR1DistRule" T2 ON T2."OcrCode" = 
            CASE :DimCode 
                WHEN '2' THEN T0."OcrCode2" 
                WHEN '3' THEN T0."OcrCode3" 
                WHEN '4' THEN T0."OcrCode4" 
                WHEN '5' THEN T0."OcrCode5" 
                ELSE T0."ProfitCode" 
            END AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END >= T2."ValidFrom" AND 
            CASE :DateType 
                WHEN 'DueDate' THEN T0."DueDate" 
                WHEN 'TaxDate' THEN T0."TaxDate" 
                ELSE T0."RefDate" 
            END <= T2."ValidTo" 
        INNER JOIN "CRSPProfitLostByCCBR_Plan" T4 ON T0."Account" = T4."AcctCode" 
    WHERE (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END >= ADD_YEARS(:FinancYear, -1) OR 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END >= ADD_YEARS(:FromDate, -1)) AND (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END < :FinancYear OR 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END < ADD_YEARS(:ToDate, -1)) AND ((:IgnoreAdj = 'Y' AND T0."AdjTran" = 'N') OR (:IgnoreAdj = 'N') OR
         (:IgnoreAdj = 'O' AND T0."AdjTran" = 'Y')) AND ((T4."DocType" = 'B' AND NOT (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END = ADD_YEARS(ADD_DAYS(:FinancYear, -1), 1) AND T0."TransType" = -3) OR (T4."DocType" = 'P' AND
         NOT T0."TransType" = -3)) OR :EndYear = 'Y') 
		 AND (T2."OcrCode" IN (:OcrCode) OR :OcrCode = '') 
		 AND (T2."PrcCode" IN (:PrcCode) OR :PrcCode = '') 
		 AND (T0."Project" IN (:PrjCode) OR :PrjCode = '')
         AND T10."BtfStatus" = 'O' AND :AddVoucher = 'Y' 
	
	/* Budget (BGT1) Actual Year*/
    UNION ALL 
	
    SELECT
		/* Calc the Budget Month by LineID from BGT1 and FinancYear from OBGT*/
		T0."Instance" AS "TransID", T0."Line_ID",
    	ADD_MONTHS(T1."FinancYear", T0."Line_ID") AS "RefDate", T1."FinancYear" AS "FinancYear", 
        T0."AcctCode" AS "Account", 'BGT1' AS "Typ", 
		IFNULL(T4."PrcCode", '') AS "PrcCode", 
        IFNULL(T4."OcrCode", '') AS "OcrCode", 
		IFNULL(T2."PrjCode", '') AS "PrjCode", 
		/* From - ToDate */
		0 AS "Credit", 0 AS "CreditPreviousYear", 0 AS "Debit", 0 AS "DebitPreviousYear", 
        CASE 
            WHEN ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FromDate AND ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= :ToDate THEN T0."CredLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditBudget", 
        CASE 
            WHEN ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FromDate AND ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= :ToDate THEN T0."DebLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitBudget", 0 AS "CreditBudgetPrevYear", 0 AS "DebitBudgetPrevYear",
		/* Month(ToDate) */
		0 AS "CreditActualMonth", 0 AS "CreditMonthPreviousYear", 0 AS "DebitActualMonth", 0 AS "DebitMonthPreviousYear", 
        CASE 
            WHEN MONTH(ADD_MONTHS(T1."FinancYear", T0."Line_ID")) = MONTH(:ToDate) THEN T0."CredLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditActualMonthBudget", 
        CASE 
            WHEN MONTH(ADD_MONTHS(T1."FinancYear", T0."Line_ID")) = MONTH(:ToDate) THEN T0."DebLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitActualMonthBudget", 
		/* Full Finance Year */
		0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 
		/* muf: Modified to allow selection of period greater than 1 year */
        CASE 
            WHEN ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FinancYear AND ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(:FinancYear, 12) THEN T0."CredLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditFinancYearBudget", 
        CASE 
            WHEN ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FinancYear AND ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(:FinancYear, 12) THEN T0."DebLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitFinancYearBudget", 
		/* Prev Year */
		0 AS "CreditPrevFinancYear", 0 AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit", 
		/* Opening Balance for selected Period */
		0 AS "OBDebit", 0 AS "OBCredit", 
		/* Opening Balance Brought Forward */
		0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM BGT1 T0 
        LEFT OUTER JOIN OBGT T1 ON T0."BudgId" = T1."AbsId" 
        LEFT OUTER JOIN OBGS T2 ON T0."Instance" = T2."AbsId" 
		/* Link Distribution Rules*/
        LEFT OUTER JOIN "CRSPProfitLostByCCBR_OCR1DistRule" T4 ON T4."OcrCode" = 
            CASE :DimCode 
                WHEN '2' THEN T2."OcrCode2" 
                WHEN '3' THEN T2."OcrCode3" 
                WHEN '4' THEN T2."OcrCode4" 
                WHEN '5' THEN T2."OcrCode5" 
                ELSE T2."OcrCode" 
            END AND (ADD_MONTHS(T1."FinancYear", T0."Line_ID")) >= T4."ValidFrom" 
			AND (ADD_MONTHS(T1."FinancYear", T0."Line_ID")) <= T4."ValidTo" 
        INNER JOIN "CRSPProfitLostByCCBR_Plan" T5 ON T0."AcctCode" = T5."AcctCode" 
    WHERE :BudgId <> -9 AND ( ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FinancYear OR ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FromDate) AND ( ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(:FinancYear, 12) OR ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(:ToDate, 12)) AND 
        CASE 
            WHEN :BudgId = -1 THEN T0."Instance" 
            ELSE :BudgId 
        END = T0."Instance" 
		AND (T4."OcrCode" IN (:OcrCode) OR :OcrCode = '') 
		AND (T4."PrcCode" IN (:PrcCode) OR :PrcCode = '') 
		AND (T2."PrjCode" IN (:PrjCode) OR :PrjCode = '') 
		
	/* Budget (BGT1) Previous Year*/	
    UNION ALL 
    SELECT 
		/* Calc the Budget Month by LineID from BGT1 and FinancYear from OBGT*/
		T0."Instance" AS "TransID", T0."Line_ID", ADD_MONTHS(T1."FinancYear", T0."Line_ID") AS "RefDate", T1."FinancYear" AS "FinancYear", 
        T0."AcctCode" AS "Account", 'BGT1' AS "Typ", 
		IFNULL(T4."PrcCode", '') AS "PrcCode", 
        IFNULL(T4."OcrCode", '') AS "OcrCode", 
		IFNULL(T2."PrjCode", '') AS "PrjCode", 
		/* From - ToDate */
		0 AS "Credit", 0 AS "CreditPreviousYear", 0 AS "Debit", 0 AS "DebitPreviousYear", 0 AS "CreditBudget", 0 AS "DebitBudget", 
		/* Budget Previous Year */
        CASE 
            WHEN ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= ADD_YEARS(:FromDate, -1) AND
            ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_YEARS(:ToDate, -1) THEN T0."CredLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditBudgetPrevYear", 
        CASE 
            WHEN ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= ADD_YEARS(:FromDate, -1) AND
            ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_YEARS(:ToDate, -1) THEN T0."DebLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitBudgetPrevYear", 
		/* Month(ToDate) */
		0 AS "CreditActualMonth", 0 AS "CreditMonthPreviousYear", 
        0 AS "DebitActualMonth", 0 AS "DebitMonthPreviousYear", 0 AS "CreditActualMonthBudget", 
        0 AS "DebitActualMonthBudget", 
		/* Full Finance Year */
		0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 
        0 AS "CreditFinancYearBudget", 0 AS "DebitFinancYearBudget", 
		/* Prev Year */
		0 AS "CreditPrevFinancYear", 0 AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit", 
		/* Opening Balance for selected Period */
		0 AS "OBDebit", 0 AS "OBCredit",
		/* Opening Balance Brought Forward */
        0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM BGT1 T0 
        LEFT OUTER JOIN OBGT T1 ON T0."BudgId" = T1."AbsId" 
        LEFT OUTER JOIN OBGS T2 ON T0."Instance" = T2."AbsId"
		/* Link Distribution Rules*/
        LEFT OUTER JOIN "CRSPProfitLostByCCBR_OCR1DistRule" T4 ON T4."OcrCode" = 
            CASE :DimCode 
                WHEN '2' THEN T2."OcrCode2" 
                WHEN '3' THEN T2."OcrCode3" 
                WHEN '4' THEN T2."OcrCode4" 
                WHEN '5' THEN T2."OcrCode5" 
                ELSE T2."OcrCode" 
            END AND (ADD_MONTHS(T1."FinancYear", T0."Line_ID")) >= T4."ValidFrom" AND (ADD_MONTHS(T1."FinancYear", T0."Line_ID")) <= T4."ValidTo" 
        INNER JOIN "CRSPProfitLostByCCBR_Plan" T5 ON T0."AcctCode" = T5."AcctCode" 
    WHERE :BudgId <> -9 AND ( ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FinancYear OR ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= ADD_YEARS(:FromDate, -1)) AND (ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(:FinancYear, 12) OR
        ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(ADD_YEARS(:ToDate, -1), 12)) AND 
        CASE 
            WHEN :BudgId = -1 THEN T0."Instance" 
            ELSE :BudgId 
        END = T0."Instance" 
		AND (T4."OcrCode" IN (:OcrCode) OR :OcrCode = '') 
		AND (T4."PrcCode" IN (:PrcCode) OR :PrcCode = '') 
		AND (T2."PrjCode" IN (:PrjCode) OR :PrjCode = '');
    
	
    /* SUM all Results */
	
	/* CAREFUL: Including the TransID and Line_ID in this summary might cause performance issue. Take this out if it is not required 
    */
END;











