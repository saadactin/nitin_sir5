-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_OpenItemsList_BR_enUS (in CardType nvarchar(1), in DateType varchar(256), in FromDate timestamp, in ToDate timestamp) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
CompanyName nvarchar(100);
MainCurncyVar varchar(3);
SysCurrncy varchar(3);
BEGIN 
/*    
***** General Information *****
Name: OJDT_BPCalc 
Description: Return Journal Entries related to Invoices, Credit Memos and Payments for Business Partners

Creator: coresystems ag for SAP AG, muf@coresystems.ch
Create Date: 2011-06-22
*/
	
    /* Internal Variables */
    SELECT 
    (SELECT "CompnyName" 
    FROM OADM) INTO CompanyName FROM DUMMY;
	
    SELECT 
    (SELECT TOP 1 T2."MainCurncy" 
    FROM OADM T2) INTO MainCurncyVar FROM DUMMY;
	
    SELECT 
    (SELECT TOP 1 T2."SysCurrncy" 
    FROM OADM T2) INTO SysCurrncy FROM DUMMY;
	
	
    SELECT :CompanyName AS "CompanyName", T1."ShortName", T2."CardName", T2."CardCode", T2."CardFName", 
        T2."CardType", T1."Ref1", T0."Number", T0."DocSeries", T0."BaseRef", 
        CASE T1."TransType" 
            WHEN 13 THEN T3."DocNum" 
            WHEN 14 THEN T4."DocNum" 
            WHEN 18 THEN T7."DocNum" 
            WHEN 19 THEN T8."DocNum" 
            ELSE T1."BaseRef" 
        END AS "BaseDocNum", 
        CASE T1."TransType" 
            WHEN 13 THEN T3."DocEntry" 
            WHEN 14 THEN T4."DocEntry" 
            WHEN 18 THEN T7."DocEntry" 
            WHEN 19 THEN T8."DocEntry" 
            ELSE T1."BaseRef" 
        END AS "BaseDocEntry", T0."Memo", T1."TransId", T1."TransType", T1."DueDate", T1."RefDate", T1."TaxDate", 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END AS "SelectionDate", T1."Project", :MainCurncyVar AS "MainCurncy", :SysCurrncy AS "SysCurrncy", 
        IFNULL(T1."FCCurrency", '') AS "FCCurrency", 
        CASE T2."CardType" 
            WHEN 'C' THEN CAST(T1."Debit" AS DECIMAL(21,6)) 
            ELSE 
            CASE 
                WHEN CAST(T1."Debit" AS DECIMAL(21,6)) <> 0 THEN CAST(T1."Debit" AS DECIMAL(21,6)) -
                 CAST(T1."BalDueDeb" AS DECIMAL(21,6)) 
                ELSE CAST(T1."BalDueCred" AS DECIMAL(21,6)) 
            END 
        END AS "Debit", 
        CASE T2."CardType" 
            WHEN 'S' THEN CAST(T1."Debit" AS DECIMAL(21,6)) 
            ELSE 
            CASE 
                WHEN CAST(T1."Debit" AS DECIMAL(21,6)) <> 0 THEN CAST(T1."Debit" AS DECIMAL(21,6)) -
                 CAST(T1."BalDueDeb" AS DECIMAL(21,6)) 
                ELSE CAST(T1."BalDueCred" AS DECIMAL(21,6)) 
            END 
        END AS "Credit", 
        CASE T2."CardType" 
            WHEN 'C' THEN 1 
            ELSE -1 
        END * (CAST(T1."Debit" AS DECIMAL(21,6)) - 
        CASE 
            WHEN CAST(T1."Debit" AS DECIMAL(21,6)) <> 0 THEN CAST(T1."Debit" AS DECIMAL(21,6)) - CAST(T1."BalDueDeb" AS DECIMAL(21,6)) 
            ELSE CAST(T1."BalDueCred" AS DECIMAL(21,6)) 
        END) AS "Balance", 
        CASE T2."CardType" 
            WHEN 'C' THEN CAST(T1."FCDebit" AS DECIMAL(21,6)) 
            ELSE 
            CASE 
                WHEN CAST(T1."FCDebit" AS DECIMAL(21,6)) <> 0 THEN CAST(T1."FCDebit" AS DECIMAL(21,6)) -
                 CAST(T1."BalFcDeb" AS DECIMAL(21,6)) 
                ELSE CAST(T1."BalFcCred" AS DECIMAL(21,6)) 
            END 
        END AS "FcDebit", 
        CASE T2."CardType" 
            WHEN 'S' THEN CAST(T1."FCDebit" AS DECIMAL(21,6)) 
            ELSE 
            CASE 
                WHEN CAST(T1."FCDebit" AS DECIMAL(21,6)) <> 0 THEN CAST(T1."FCDebit" AS DECIMAL(21,6)) -
                 CAST(T1."BalFcDeb" AS DECIMAL(21,6)) 
                ELSE CAST(T1."BalFcCred" AS DECIMAL(21,6)) 
            END 
        END AS "FcCredit", 
        CASE T2."CardType" 
            WHEN 'C' THEN 1 
            ELSE -1 
        END * (CAST(T1."FCDebit" AS DECIMAL(21,6)) - 
        CASE 
            WHEN CAST(T1."FCDebit" AS DECIMAL(21,6)) <> 0 THEN CAST(T1."FCDebit" AS DECIMAL(21,6)) -
             CAST(T1."BalFcDeb" AS DECIMAL(21,6)) 
            ELSE CAST(T1."BalFcCred" AS DECIMAL(21,6)) 
        END) AS "FcBalance", 
        CASE T2."CardType" 
            WHEN 'C' THEN CAST(T1."SYSDeb" AS DECIMAL(21,6)) 
            ELSE 
            CASE 
                WHEN CAST(T1."SYSDeb" AS DECIMAL(21,6)) <> 0 THEN CAST(T1."SYSDeb" AS DECIMAL(21,6)) -
                 CAST(T1."BalScDeb" AS DECIMAL(21,6)) 
                ELSE CAST(T1."BalScCred" AS DECIMAL(21,6)) 
            END 
        END AS "SysDeb", 
        CASE T2."CardType" 
            WHEN 'S' THEN CAST(T1."SYSDeb" AS DECIMAL(21,6)) 
            ELSE 
            CASE 
                WHEN CAST(T1."SYSDeb" AS DECIMAL(21,6)) <> 0 THEN CAST(T1."SYSDeb" AS DECIMAL(21,6)) -
                 CAST(T1."BalScDeb" AS DECIMAL(21,6)) 
                ELSE CAST(T1."BalScCred" AS DECIMAL(21,6)) 
            END 
        END AS "SysCred", 
        CASE T2."CardType" 
            WHEN 'C' THEN 1 
            ELSE -1 
        END * (CAST(T1."SYSDeb" AS DECIMAL(21,6)) - 
        CASE 
            WHEN CAST(T1."SYSDeb" AS DECIMAL(21,6)) <> 0 THEN CAST(T1."SYSDeb" AS DECIMAL(21,6)) - CAST(T1."BalScDeb" AS DECIMAL(21,6)) 
            ELSE CAST(T1."BalScCred" AS DECIMAL(21,6)) 
        END) AS "SysBal", 
        CASE T1."TransType" 
            WHEN 13 THEN T3."PayBlock" 
            WHEN 14 THEN T4."PayBlock" 
            WHEN 18 THEN T7."PayBlock" 
            WHEN 19 THEN T8."PayBlock" 
            ELSE T1."PayBlock" 
        END AS "PayBlock", 
        CASE T1."TransType" 
            WHEN 13 THEN T3."Comments" 
            WHEN 14 THEN T4."Comments" 
            WHEN 18 THEN T7."Comments" 
            WHEN 19 THEN T8."Comments" 
            WHEN 24 THEN T11."Comments" 
            WHEN 46 THEN T13."Comments" 
            ELSE T0."Memo" 
        END AS "Comments" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN OCRD T2 ON T2."CardCode" = T1."ShortName" 
        LEFT OUTER JOIN OINV T3 ON T1."BaseRef" = T3."DocNum" AND T0."DocSeries" = T3."Series" 
        LEFT OUTER JOIN ORIN T4 ON T1."BaseRef" = T4."DocNum" AND T0."DocSeries" = T4."Series" 
        LEFT OUTER JOIN OPCH T7 ON T1."BaseRef" = T7."DocNum" AND T0."DocSeries" = T7."Series" 
        LEFT OUTER JOIN ORPC T8 ON T1."BaseRef" = T8."DocNum" AND T0."DocSeries" = T8."Series" 
        LEFT OUTER JOIN ORCT T11 ON T1."BaseRef" = T11."DocNum" AND T0."DocSeries" = T11."Series" 
        LEFT OUTER JOIN OVPM T13 ON T1."BaseRef" = T13."DocNum" AND T0."DocSeries" = T13."Series" 
    WHERE T2."CardType" = :CardType AND (T1."BalDueDeb" <> 0 OR T1."BalDueCred" <> 0 OR T1."BalFcDeb" <> 0 OR
         T1."BalFcCred" <> 0) AND 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END >= :FromDate AND 
        CASE :DateType 
            WHEN 'DueDate' THEN T0."DueDate" 
            WHEN 'TaxDate' THEN T0."TaxDate" 
            ELSE T0."RefDate" 
        END <= :ToDate AND T1."TransType" IN ('13','14','18','19','24','46') 
    ORDER BY T2."CardCode", T1."TransType", 
        CASE :DateType 
            WHEN 'DueDate' THEN T1."DueDate" 
            WHEN 'TaxDate' THEN T1."TaxDate" 
            ELSE T1."RefDate" 
        END, T1."TransId", T1."Line_ID";
    --[Note:Modifier] HANA doesn't have CONVERT you may you see CAST or implicit casting or TO_... functions
END











