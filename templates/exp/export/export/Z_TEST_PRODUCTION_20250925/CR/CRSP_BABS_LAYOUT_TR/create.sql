-- B1 DEPENDS: AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_BABS_Layout_TR (in DateTypeIn varchar(256), in FromDateIn timestamp, in ToDateIn timestamp, in MinAmountIn integer) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
DateType varchar(256);
DateFrom timestamp;
DateTo timestamp;
MinAmount integer;
Category nchar(4);
BEGIN 
    /* Default values in case parameters are not provided */
    DateType := 'TaxDate';
    SELECT 
    (SELECT MIN(T0."TaxDate") 
    FROM "OJDT" T0) INTO DateFrom FROM DUMMY;
    SELECT 
    (SELECT MAX(T0."TaxDate") 
    FROM "OJDT" T0) INTO DateTo FROM DUMMY;
    MinAmount := 5000;
    Category := 'BABS';/* Possible values are: BA, BS, BABS (both)*/
    /* Parameters (Comment this to test query) */
    DateType := DateTypeIn;
    DateFrom := FromDateIn;
    --DateFrom := '2016-01-01 00:00:00.000';
    DateTo := ToDateIn;
    --DateTo := '2016-01-12 00:00:00.000';
    MinAmount := MinAmountIn;
    --Category := CategoryIn;

SELECT T10."Category", ROW_NUMBER() OVER ( ORDER BY T10."CardName") AS "Row", T10."CardCode", T10."CardName", 
        T10."Country", IFNULL(T10."TaxOffice", '') AS "TaxOffice", T10."LicTradNum" AS "TaxCode", (
        CASE 
            WHEN T10."CmpPrivate" = 'C' THEN T10."LicTradNum" 
            ELSE '' 
        END) AS "LicTradNum", (
        CASE 
            WHEN T10."CmpPrivate" = 'I' THEN T10."LicTradNum" 
            ELSE '' 
        END) AS "IDNumber", SUM(T10."Transactions") AS "SumTransactions", 
    ROUND(SUM(T10."BaseSum"), 0) AS "SumBaseSum" 
    FROM 
    (
        SELECT 'BA' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", COUNT(DISTINCT (T0."TransId")) AS "Transactions", 
        SUM(T1."BaseSum") AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN OPCH T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BA' OR :Category = 'BABS') AND 1 = 1 AND T4."Category" = 'I' AND T4."Correction" = 'N' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '18' AND T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum"
    /* AP credit note, Input tax code, correction tax (return invoice to supplier) */ 
    UNION ALL 
    SELECT 'BA' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", 
        CASE 
            WHEN T2."DocStatus" = 'O' THEN COUNT(DISTINCT (T0."TransId")) 
            WHEN T2."DocTotal" <> 
            (SELECT MAX(IFNULL(T8."DocTotal", 0.0)) 
            FROM RPC1 T6 
                INNER JOIN PCH1 T7 ON T6."BaseEntry" = T7."DocEntry" AND T6."BaseType" = '18' 
                INNER JOIN OPCH T8 ON T7."DocEntry" = T8."DocEntry" 
            WHERE T6."DocEntry" = T2."DocEntry") THEN 0 
            ELSE COUNT(DISTINCT (T0."TransId")) * (-1) 
        END AS "Transactions", SUM(T1."BaseSum") * (-1) AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN ORPC T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BA' OR :Category = 'BABS') AND T4."Category" = 'I' AND T4."Correction" = 'N' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '19' AND
         T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum", T2."DocStatus", T2."DocEntry", T2."DocTotal" 
    /*AR credit note, Output tax code, correction tax (return invoice from customer) */
    UNION ALL 
    SELECT 'BA' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", COUNT(DISTINCT (T0."TransId")) AS "Transactions", 
        SUM(T1."BaseSum") AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN ORIN T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BA' OR :Category = 'BABS') AND T4."Category" = 'O' AND T4."Correction" = 'Y' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '14' AND T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum" 
/* Category BS */
/* Selection of relevant postings for BS into temporary table*/
/* AR invoice, Output tax code, no correction tax */
    UNION ALL 
    SELECT 'BS' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", COUNT(DISTINCT (T0."TransId")) AS "Transactions", 
        SUM(T1."BaseSum") AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN OINV T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BS' OR :Category = 'BABS') AND T4."Category" = 'O' AND T4."Correction" = 'N' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '13' AND T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum" 
    /* AR credit note, Output tax code, no correction tax (AR invoice cancellation) */
    UNION ALL 
    SELECT 'BS' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", 
        CASE 
            WHEN T2."DocStatus" = 'O' THEN COUNT(DISTINCT (T0."TransId")) 
            WHEN T2."DocTotal" <> 
            (SELECT MAX(IFNULL(T8."DocTotal", 0.0)) 
            FROM RIN1 T6 
                INNER JOIN INV1 T7 ON T6."BaseEntry" = T7."DocEntry" AND T6."BaseType" = '13' 
                INNER JOIN OINV T8 ON T7."DocEntry" = T8."DocEntry" 
            WHERE T6."DocEntry" = T2."DocEntry") THEN 0 
            ELSE COUNT(DISTINCT (T0."TransId")) * (-1) 
        END AS "Transactions", SUM(T1."BaseSum") * (-1) AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN ORIN T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BS' OR :Category = 'BABS') AND T4."Category" = 'O' AND T4."Correction" = 'N' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '14' AND
         T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum", T2."DocStatus", T2."DocEntry", T2."DocTotal" 
    /* AP credit note, Input tax code, correction tax (return invoice to supplier) */
    UNION ALL 
    SELECT 'BS' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", COUNT(DISTINCT (T0."TransId")) AS "Transactions", 
        SUM(T1."BaseSum") AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN ORPC T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BS' OR :Category = 'BABS') AND T4."Category" = 'I' AND T4."Correction" = 'Y' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '19' AND T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum"
    )
    T10 
    WHERE 
        (SELECT ROUND(SUM(T11."BaseSum"), 0) 
        FROM 
        (
            SELECT 'BA' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", COUNT(DISTINCT (T0."TransId")) AS "Transactions", 
        SUM(T1."BaseSum") AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN OPCH T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BA' OR :Category = 'BABS') AND 1 = 1 AND T4."Category" = 'I' AND T4."Correction" = 'N' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '18' AND T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum"
    /* AP credit note, Input tax code, correction tax (return invoice to supplier) */ 
    UNION ALL 
    SELECT 'BA' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", 
        CASE 
            WHEN T2."DocStatus" = 'O' THEN COUNT(DISTINCT (T0."TransId")) 
            WHEN T2."DocTotal" <> 
            (SELECT MAX(IFNULL(T8."DocTotal", 0.0)) 
            FROM RPC1 T6 
                INNER JOIN PCH1 T7 ON T6."BaseEntry" = T7."DocEntry" AND T6."BaseType" = '18' 
                INNER JOIN OPCH T8 ON T7."DocEntry" = T8."DocEntry" 
            WHERE T6."DocEntry" = T2."DocEntry") THEN 0 
            ELSE COUNT(DISTINCT (T0."TransId")) * (-1) 
        END AS "Transactions", SUM(T1."BaseSum") * (-1) AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN ORPC T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BA' OR :Category = 'BABS') AND T4."Category" = 'I' AND T4."Correction" = 'N' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '19' AND
         T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum", T2."DocStatus", T2."DocEntry", T2."DocTotal" 
    /*AR credit note, Output tax code, correction tax (return invoice from customer) */
    UNION ALL 
    SELECT 'BA' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", COUNT(DISTINCT (T0."TransId")) AS "Transactions", 
        SUM(T1."BaseSum") AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN ORIN T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BA' OR :Category = 'BABS') AND T4."Category" = 'O' AND T4."Correction" = 'Y' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '14' AND T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum" 
/* Category BS */
/* Selection of relevant postings for BS into temporary table*/
/* AR invoice, Output tax code, no correction tax */
    UNION ALL 
    SELECT 'BS' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", COUNT(DISTINCT (T0."TransId")) AS "Transactions", 
        SUM(T1."BaseSum") AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN OINV T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BS' OR :Category = 'BABS') AND T4."Category" = 'O' AND T4."Correction" = 'N' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '13' AND T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum" 
    /* AR credit note, Output tax code, no correction tax (AR invoice cancellation) */
    UNION ALL 
    SELECT 'BS' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", 
        CASE 
            WHEN T2."DocStatus" = 'O' THEN COUNT(DISTINCT (T0."TransId")) 
            WHEN T2."DocTotal" <> 
            (SELECT MAX(IFNULL(T8."DocTotal", 0.0)) 
            FROM RIN1 T6 
                INNER JOIN INV1 T7 ON T6."BaseEntry" = T7."DocEntry" AND T6."BaseType" = '13' 
                INNER JOIN OINV T8 ON T7."DocEntry" = T8."DocEntry" 
            WHERE T6."DocEntry" = T2."DocEntry") THEN 0 
            ELSE COUNT(DISTINCT (T0."TransId")) * (-1) 
        END AS "Transactions", SUM(T1."BaseSum") * (-1) AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN ORIN T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BS' OR :Category = 'BABS') AND T4."Category" = 'O' AND T4."Correction" = 'N' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '14' AND
         T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum", T2."DocStatus", T2."DocEntry", T2."DocTotal" 
    /* AP credit note, Input tax code, correction tax (return invoice to supplier) */
    UNION ALL 
    SELECT 'BS' AS "Category", T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", 
        IFNULL(T3."LicTradNum", '') AS "LicTradNum", COUNT(DISTINCT (T0."TransId")) AS "Transactions", 
        SUM(T1."BaseSum") AS "BaseSum" 
    FROM OJDT T0 
        INNER JOIN JDT1 T1 ON T0."TransId" = T1."TransId" 
        INNER JOIN ORPC T2 ON T0."TransId" = T2."TransId" 
        INNER JOIN OCRD T3 ON T2."CardCode" = T3."CardCode" 
        INNER JOIN OVTG T4 ON T1."VatGroup" = T4."Code" 
        LEFT OUTER JOIN CRD1 T5 ON T3."CardCode" = T5."CardCode" AND T3."ShipToDef" = T5."Address" AND
             T5."AdresType" = 'S' 
    WHERE (:Category = 'BS' OR :Category = 'BABS') AND T4."Category" = 'I' AND T4."Correction" = 'Y' AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END >= :DateFrom AND 
        CASE :DateType 
            WHEN 'RefDate' THEN T0."RefDate" 
            WHEN 'DueDate' THEN T0."DueDate" 
            ELSE T0."TaxDate" 
        END <= :DateTo AND T0."TransType" = '19' AND T2.CANCELED = 'N' 
    GROUP BY T3."CmpPrivate", T3."CardCode", T3."CardName", T3."Country", T5."TaxOffice", T3."LicTradNum"
        )
        T11 
        WHERE T11."CardName" = T10."CardName" AND
         T11."Category" = T10."Category") >= :MinAmount 
    GROUP BY T10."Category", T10."CardCode", T10."CardName", T10."Country", T10."TaxOffice", T10."LicTradNum", T10."CmpPrivate" 
    ORDER BY T10."CardName";
END;











