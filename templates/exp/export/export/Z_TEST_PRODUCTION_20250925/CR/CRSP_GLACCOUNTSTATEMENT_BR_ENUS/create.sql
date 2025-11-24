-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_GLAccountStatement_BR_enUS (in FromDate timestamp, in ToDate timestamp) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
CompanyName nvarchar(100);
BEGIN /*
    
***** General Information *****
Name: OJDT_AccountJournal
Description: Return A/R Invoices and A/R Credit Memos in a certain Date Range

Creator: coresystems ag for SAP AG, muf@coresystems.ch
Create Date: 2011-06-22
*/
    /* Internal parameters */
    SELECT 
    (SELECT "CompnyName" 
    FROM OADM) INTO CompanyName FROM DUMMY;
    /*********************************/
    /*Months before (Opening Balance) */
    SELECT T1."Account", T2."AcctName", :CompanyName AS "CompanyName", NULL AS "RefDate", NULL AS "TaxDate", 
        '' AS "Ref1", '' AS "ContraAct", 'Previous Month(s)' AS "LineMemo", -1 AS "TransID", NULL AS "FCCurrency", 
        ifnull(SUM(T1."Debit"), 0) - ifnull(SUM(T1."Credit"), 0) AS "OpeningBalance", ifnull(SUM(T1."FCDebit"), 0) -
         ifnull(SUM(T1."FCCredit"), 0) AS "FCOpeningBalance", 0 AS "Debit", 0 AS "Credit", 0 AS "FCDebit", 
        0 AS "FCCredit" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN OACT T2 ON T1."Account" = T2."AcctCode" 
    WHERE T0."RefDate" < :FromDate 
    GROUP BY T1."Account", T2."AcctName" 
    UNION ALL 
    SELECT T1."Account", T2."AcctName", :CompanyName AS "CompanyName", T0."RefDate", T0."TaxDate", T0."Ref1", 
        T1."ContraAct", T1."LineMemo", T0."TransId", T1."FCCurrency", NULL AS "OpeningBalance", 
        NULL AS "FCOpeningBalance", T1."Debit", T1."Credit", T1."FCDebit", T1."FCCredit" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN OACT T2 ON T1."Account" = T2."AcctCode" 
    WHERE T0."RefDate" BETWEEN :FromDate AND :ToDate;
    /* Journal Rows */
END











