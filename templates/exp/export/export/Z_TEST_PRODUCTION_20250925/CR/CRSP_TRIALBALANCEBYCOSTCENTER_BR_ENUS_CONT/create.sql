-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:_TmSp_BootCreateGlobalTempTables2
CREATE PROCEDURE CRSP_TrialBalanceByCostCenter_BR_enUS_cont
(
in Currency VARCHAR(10),
in IgnoreAdj varchar(1),
in AddVoucher varchar(1),
in DateType varchar(256),
in FromDate timestamp,
in ToDate timestamp,
in DimCode nvarchar(10),
in BroughtForward varchar(1),
in EndYear varchar(1)
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
FinancYear timestamp;
BEGIN
	SELECT 
    (
    	SELECT T0."FinancYear" FROM OACP T0 
        INNER JOIN OFPR T1 ON T0."PeriodCat" = T1."Category" 
    	WHERE T1."F_RefDate" <= :ToDate AND T1."T_RefDate" >= :ToDate
    ) INTO FinancYear FROM DUMMY;

	INSERT INTO CRSP_TEMPJDT1 (
    SELECT T0."TransId", T0."Line_ID", NULL AS "RefDate", NULL AS "FinancYear", T0."Account" AS "Account", 
        'BFJDT1' AS "Typ", IFNULL(T2."PrcCode", '') AS "PrcCode", IFNULL(T2."OcrCode", '') AS "OcrCode", 
        IFNULL(T0."Project", '') AS "PrjCode", 0 AS "Credit", 0 AS "CreditPreviousYear", 0 AS "Debit", 
        0 AS "DebitPreviousYear", 0 AS "CreditBudget", 0 AS "DebitBudget", 0 AS "CreditBudgetPrevYear", 
        0 AS "DebitBudgetPrevYear", 0 AS "CreditActualMonth", 0 AS "CreditMonthPreviousYear", 
        0 AS "DebitActualMonth", 0 AS "DebitMonthPreviousYear", 0 AS "CreditActualMonthBudget", 
        0 AS "DebitActualMonthBudget", 0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 
        0 AS "CreditFinancYearBudget", 0 AS "DebitFinancYearBudget", 0 AS "CreditPrevFinancYear", 
        0 AS "DebitPrevFinancYear", 
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
        0 AS "OBDebit", 0 AS "OBCredit", 
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
        --into "CRSP_TEMPJDT1"
    FROM JDT1 T0 
        LEFT OUTER JOIN "CRSP_OCR1DistRule" T2 ON T2."OcrCode" = 
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
        INNER JOIN "CRSP_Plan" T4 ON T0."Account" = T4."AcctCode" 
    WHERE 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END < :FromDate AND ((:IgnoreAdj = 'Y' AND T0."AdjTran" = 'N') OR (:IgnoreAdj = 'N') OR (:IgnoreAdj = 'O' AND
         T0."AdjTran" = 'Y')) AND ((T4."DocType" = 'B' AND NOT (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END = ADD_YEARS(ADD_DAYS(:FinancYear, -1), 1) AND T0."TransType" = -3) OR (T4."DocType" = 'P' AND
         NOT T0."TransType" = -3)) OR :EndYear = 'Y') AND :BroughtForward = 'Y' 
    UNION 
    SELECT T0."TransId", T0."Line_ID", CAST('01.' || CAST(MONTH(
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
        'JDT1' AS "Typ", IFNULL(T2."PrcCode", '') AS "PrcCode", IFNULL(T2."OcrCode", '') AS "OcrCode", 
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
        END AS "CreditActualMonth", 0 AS "CreditMonthPreviousYear", 
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
        0 AS "CreditPrevFinancYear", 0 AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit", 
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
        END AS "OBCredit", 0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM JDT1 T0 
        LEFT OUTER JOIN "CRSP_OCR1DistRule" T2 ON T2."OcrCode" = 
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
        INNER JOIN "CRSP_Plan" T4 ON T0."Account" = T4."AcctCode" 
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
        END <= ADD_DAYS(ADD_YEARS(:FinancYear, 1), -1)) AND ((:IgnoreAdj = 'Y' AND T0."AdjTran" = 'N') OR
         (:IgnoreAdj = 'N') OR (:IgnoreAdj = 'O' AND T0."AdjTran" = 'Y')) AND ((T4."DocType" = 'B' AND NOT (
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END = ADD_YEARS(ADD_DAYS(:FinancYear, -1), 1) AND T0."TransType" = -3) OR (T4."DocType" = 'P' AND
         NOT T0."TransType" = -3)) OR :EndYear = 'Y') 
    UNION ALL 
    SELECT T0."TransId", T0."Line_ID", CAST('01.' || CAST(MONTH(
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
        IFNULL(T2."OcrCode", '') AS "OcrCode", IFNULL(T0."Project", '') AS "PrjCode", 0 AS "Credit", 
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
        0 AS "DebitBudgetPrevYear", 0 AS "CreditActualMonth", 
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
        0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 0 AS "CreditFinancYearBudget", 0 AS "DebitFinancYearBudget", 
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
        END AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit", 0 AS "OBDebit", 0 AS "OBCredit", 
        0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM JDT1 T0 
        LEFT OUTER JOIN "CRSP_OCR1DistRule" T2 ON T2."OcrCode" = 
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
        INNER JOIN "CRSP_Plan" T4 ON T0."Account" = T4."AcctCode" 
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
    UNION ALL 
    SELECT T0."TransId", T0."Line_ID", CAST('01.' || CAST(MONTH(
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
        END AS "CreditActualMonth", 0 AS "CreditMonthPreviousYear", 
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
        0 AS "CreditPrevFinancYear", 0 AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit", 
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
        END AS "OBCredit", 0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM BTF1 T0 
        INNER JOIN OBTF T10 ON T0."TransId" = T10."TransId" AND T0."BatchNum" = T10."BatchNum" 
        LEFT OUTER JOIN "CRSP_OCR1DistRule" T2 ON T2."OcrCode" = 
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
        INNER JOIN "CRSP_Plan" T4 ON T0."Account" = T4."AcctCode" 
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
         NOT T0."TransType" = -3)) OR :EndYear = 'Y') AND T10."BtfStatus" = 'O' AND :AddVoucher = 'Y' 
    UNION ALL 
    SELECT T0."TransId", T0."Line_ID", CAST('01.' || CAST(MONTH(
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
        T0."Account" AS "Account", 'BTF1' AS "Typ", IFNULL(T2."PrcCode", '') AS "PrcCode", 
        IFNULL(T2."OcrCode", '') AS "OcrCode", IFNULL(T0."Project", '') AS "PrjCode", 0 AS "Credit", 
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
        0 AS "DebitBudgetPrevYear", 0 AS "CreditActualMonth", 
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
        0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 0 AS "CreditFinancYearBudget", 0 AS "DebitFinancYearBudget", 
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
        END AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit", 0 AS "OBDebit", 0 AS "OBCredit", 
        0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM BTF1 T0 
        INNER JOIN OBTF T10 ON T0."TransId" = T10."TransId" AND T0."BatchNum" = T10."BatchNum" 
        LEFT OUTER JOIN "CRSP_OCR1DistRule" T2 ON T2."OcrCode" = 
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
        INNER JOIN "CRSP_Plan" T4 ON T0."Account" = T4."AcctCode" 
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
         NOT T0."TransType" = -3)) OR :EndYear = 'Y') AND T10."BtfStatus" = 'O' AND :AddVoucher = 'Y' 
    UNION ALL 
    SELECT T0."Instance" AS "TransID", T0."Line_ID", ADD_MONTHS(T1."FinancYear", T0."Line_ID") AS "RefDate", 
    	T1."FinancYear" AS "FinancYear", 
        T0."AcctCode" AS "Account", 'BGT1' AS "Typ", IFNULL(T4."PrcCode", '') AS "PrcCode", 
        IFNULL(T4."OcrCode", '') AS "OcrCode", IFNULL(T2."PrjCode", '') AS "PrjCode", 0 AS "Credit", 
        0 AS "CreditPreviousYear", 0 AS "Debit", 0 AS "DebitPreviousYear", 
        CASE 
            WHEN ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FromDate AND 
            	 ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= :ToDate 
           	THEN T0."CredLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditBudget", 
        CASE 
            WHEN  ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FromDate AND
            	  ADD_MONTHS(T1."FinancYear", T0."Line_ID")  <= :ToDate 
           	THEN T0."DebLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitBudget", 0 AS "CreditBudgetPrevYear", 0 AS "DebitBudgetPrevYear", 0 AS "CreditActualMonth", 
        0 AS "CreditMonthPreviousYear", 0 AS "DebitActualMonth", 0 AS "DebitMonthPreviousYear", 
        CASE 
            WHEN MONTH(ADD_MONTHS(T1."FinancYear", T0."Line_ID")) = MONTH(:ToDate) 
            THEN T0."CredLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditActualMonthBudget", 
        CASE 
            WHEN MONTH(ADD_MONTHS(T1."FinancYear", T0."Line_ID")) = MONTH(:ToDate) 
            THEN T0."DebLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitActualMonthBudget", 0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 
        CASE 
            WHEN  ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FinancYear AND
            	  ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(:FinancYear, 12) 
           	THEN T0."CredLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditFinancYearBudget", 
        CASE 
            WHEN  ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FinancYear AND  
            	  ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(:FinancYear, 12) 
            THEN T0."DebLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitFinancYearBudget", 0 AS "CreditPrevFinancYear", 0 AS "DebitPrevFinancYear", 0 AS "BFDebit", 
        0 AS "BFCredit", 0 AS "OBDebit", 0 AS "OBCredit", 0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM BGT1 T0 
        LEFT OUTER JOIN OBGT T1 ON T0."BudgId" = T1."AbsId" 
        LEFT OUTER JOIN OBGS T2 ON T0."Instance" = T2."AbsId" 
        LEFT OUTER JOIN "CRSP_OCR1DistRule" T4 ON T4."OcrCode" = 
            CASE :DimCode 
                WHEN '2' THEN T2."OcrCode2" 
                WHEN '3' THEN T2."OcrCode3" 
                WHEN '4' THEN T2."OcrCode4" 
                WHEN '5' THEN T2."OcrCode5" 
                ELSE T2."OcrCode" 
            END AND ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= T4."ValidFrom" AND 
                    ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= T4."ValidTo" 
        INNER JOIN "CRSP_Plan" T5 ON T0."AcctCode" = T5."AcctCode" 
    WHERE ( ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FinancYear 
    		OR  ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= :FromDate) 
    		AND ( ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(:FinancYear, 12) 
    		OR ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(:ToDate, 12)) 
    UNION ALL 
    SELECT T0."Instance" AS "TransID", T0."Line_ID", ADD_MONTHS(T1."FinancYear", T0."Line_ID") AS "RefDate", 
    	T1."FinancYear" AS "FinancYear", 
        T0."AcctCode" AS "Account", 'BGT1' AS "Typ", IFNULL(T4."PrcCode", '') AS "PrcCode", 
        IFNULL(T4."OcrCode", '') AS "OcrCode", IFNULL(T2."PrjCode", '') AS "PrjCode", 0 AS "Credit", 
        0 AS "CreditPreviousYear", 0 AS "Debit", 0 AS "DebitPreviousYear", 0 AS "CreditBudget", 0 AS "DebitBudget", 
        CASE 
            WHEN ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= ADD_YEARS(:FromDate, -1) AND
            	 ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_YEARS(:ToDate, -1) 
            THEN T0."CredLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "CreditBudgetPrevYear", 
        CASE 
            WHEN  ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= ADD_YEARS(:FromDate, -1) AND
              	  ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_YEARS(:ToDate, -1) 
            THEN T0."DebLTotal" * IFNULL(T4."OcrFactor", 1) 
            ELSE 0 
        END AS "DebitBudgetPrevYear", 0 AS "CreditActualMonth", 0 AS "CreditMonthPreviousYear", 
        0 AS "DebitActualMonth", 0 AS "DebitMonthPreviousYear", 0 AS "CreditActualMonthBudget", 
        0 AS "DebitActualMonthBudget", 0 AS "CreditFinancYear", 0 AS "DebitFinancYear", 
        0 AS "CreditFinancYearBudget", 0 AS "DebitFinancYearBudget", 0 AS "CreditPrevFinancYear", 
        0 AS "DebitPrevFinancYear", 0 AS "BFDebit", 0 AS "BFCredit", 0 AS "OBDebit", 0 AS "OBCredit", 
        0 AS "BFOBDebit", 0 AS "BFOBCredit" 
    FROM BGT1 T0 
        LEFT OUTER JOIN OBGT T1 ON T0."BudgId" = T1."AbsId" 
        LEFT OUTER JOIN OBGS T2 ON T0."Instance" = T2."AbsId" 
        LEFT OUTER JOIN "CRSP_OCR1DistRule" T4 ON T4."OcrCode" = 
            CASE :DimCode 
                WHEN '2' THEN T2."OcrCode2" 
                WHEN '3' THEN T2."OcrCode3" 
                WHEN '4' THEN T2."OcrCode4" 
                WHEN '5' THEN T2."OcrCode5" 
                ELSE T2."OcrCode" 
            END AND (ADD_MONTHS(T1."FinancYear", T0."Line_ID")) >= T4."ValidFrom" AND 
            	(ADD_MONTHS(T1."FinancYear", T0."Line_ID")) <= T4."ValidTo" 
        INNER JOIN "CRSP_Plan" T5 ON T0."AcctCode" = T5."AcctCode" 
    WHERE ( ADD_MONTHS(T1."FinancYear", T0."Line_ID")>= :FinancYear OR  
    		ADD_MONTHS(T1."FinancYear", T0."Line_ID") >= ADD_YEARS(:FromDate, -1)) AND 
    	  ( ADD_MONTHS(T1."FinancYear", T0."Line_ID")<= ADD_MONTHS(:FinancYear, 12) OR 
    	  	ADD_MONTHS(T1."FinancYear", T0."Line_ID") <= ADD_MONTHS(ADD_YEARS(:ToDate, -1), 12)
   		  ));
          
    /* SUM all Results */
    /*
     CAREFUL: Including the TransID and Line_ID in this summary might cause performance issue. Take this out if it is not required 
    */
    /* Budget (BGT1) Previous Year*/
    /* Budget (BGT1) Actual Year*/
    /* Journal Vouchers Prev Year*/
    /* Journal Vouchers */
    /* Journal Prev Year*/
    /* Journals (JDT1) Financ Year*/
    /*,IFNULL(CAST(T0.BPLId AS NVARCHAR(10)),'') As BPLId*/
    /* Exclude Endyear Closings */
    /*,IFNULL(CAST(T0.BPLId AS NVARCHAR(10)),'') As BPLId*/
    /*,IFNULL(CAST(T0.BPLId AS NVARCHAR(10)),'') As BPLId*/
    /*,IFNULL(CAST(T0.BPLId AS NVARCHAR(10)),'') As BPLId*/
    /*,IFNULL(CAST(T0.BPLId AS NVARCHAR(10)),'') As BPLId*/
    /*,'' As BPLId*/
    /*,'' As BPLId*/
    /* Link Distribution Rules*/
    /*,T0.BPLId*/
    /*,IFNULL(CAST(T0.BPLId AS NVARCHAR(10)),'') As BPLId*/
   
	
    INSERT INTO CRSP_TEMPRES (SELECT T0."Typ", T0."TransId", T0."Line_ID", T0."RefDate", T0."FinancYear", T0."Account", 
        IFNULL(T0."PrcCode", '') AS "PrcCode", IFNULL(T0."OcrCode", '') AS "OcrCode", 
        IFNULL(T0."PrjCode", '') AS "PrjCode", 
        SUM(T0."Credit") AS "Credit", 
        SUM(T0."CreditPreviousYear") AS "CreditPreviousYear", 
        SUM(T0."Debit") AS "Debit", 
        SUM(T0."DebitPreviousYear") AS "DebitPreviousYear", 
        SUM(T0."CreditBudget") AS "CreditBudget", 
        SUM(T0."DebitBudget") AS "DebitBudget", 
        
        SUM(T0."CreditBudgetPrevYear") AS "CreditBudgetPrevYear", 
        SUM(T0."DebitBudgetPrevYear") AS "DebitBudgetPrevYear", 
        
        SUM(T0."CreditActualMonth") AS "CreditActualMonth", 
        SUM(T0."CreditMonthPreviousYear") AS "CreditMonthPreviousYear", 
        SUM(T0."DebitActualMonth") AS "DebitActualMonth", 
        SUM(T0."DebitMonthPreviousYear") AS "DebitMonthPreviousYear", 
        
        SUM(T0."CreditActualMonthBudget") AS "CreditActualMonthBudget", 
        SUM(T0."DebitActualMonthBudget") AS "DebitActualMonthBudget", 
        SUM(T0."CreditFinancYear") AS "CreditFinancYear", 
        SUM(T0."DebitFinancYear") AS "DebitFinancYear", 
        SUM(T0."CreditFinancYearBudget") AS "CreditFinancYearBudget", 
        SUM(T0."DebitFinancYearBudget") AS "DebitFinancYearBudget", 
        SUM(T0."CreditPrevFinancYear") AS "CreditPrevFinancYear", 
        SUM(T0."DebitPrevFinancYear") AS "DebitPrevFinancYear", 
        SUM(T0."BFDebit") AS "BFDebit", 
        SUM(T0."BFCredit") AS "BFCredit", 
        SUM(T0."OBDebit") AS "OBDebit", 
        SUM(T0."OBCredit") AS "OBCredit", 
        SUM(T0."BFOBDebit") AS "BFOBDebit", 
        SUM(T0."BFOBCredit") AS "BFOBCredit" 
    FROM CRSP_TEMPJDT1 T0 
    GROUP BY T0."Typ", T0."TransId", T0."Line_ID", T0."RefDate", T0."FinancYear", T0."Account", T0."PrcCode", T0."OcrCode", T0."PrjCode");
    
END;











