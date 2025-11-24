-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_InvoiceLedger_BR_enUS (in DateTypeIn varchar(256), in FromDateIn timestamp, in ToDateIn timestamp) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
CompnyName nvarchar(100);
MainCurncy nvarchar(3);
Country nvarchar(3);

DateType varchar(256);
FromDate timestamp;
ToDate timestamp;
BEGIN /*
    
***** General Information *****
Name: OINV_InvoiceLedger 
Description: Return A/R Invoices and A/R Credit Memos in a certain Date Range

Creator: coresystems ag for SAP AG, muf@coresystems.ch
Create Date: 2011-06-22
*/
    /* Internal Variables */
    SELECT 
    (SELECT "CompnyName" 
    FROM OADM) INTO CompnyName FROM DUMMY;
    
    SELECT 
    (SELECT UPPER("MainCurncy") 
    FROM OADM) INTO MainCurncy FROM DUMMY;
    
    
    SELECT 
    (SELECT "LawsSet" 
    FROM CINF) INTO Country FROM DUMMY;
    
    
    /* Date Type to set the filter and sort by. Possible values are DocDate, TaxDate and DocDueDate. */
    DateType := DateTypeIn;
    /* Date Filter From and To Dates. Format 'YYYYMMDD' */
    FromDate := FromDateIn;
    ToDate := ToDateIn;
    
    SELECT :CompnyName AS "CompanyName", :Country AS "Country", 
        CASE :Country 
            WHEN 'BR' THEN 
            (SELECT COUNT(DISTINCT ("TaxCode")) 
            FROM INV1 
            WHERE "DocEntry" = OINV."DocEntry") 
            ELSE 
            (SELECT COUNT(DISTINCT ("VatGroup")) 
            FROM INV1 
            WHERE "DocEntry" = OINV."DocEntry") 
        END AS "VatCount", OINV."DocEntry", OINV."DocNum", OINV."ObjType", OINV."CardCode", OCRD."CardName", 
        OINV."DocDate", OINV."TaxDate", OINV."DocDueDate", 
        CASE :MainCurncy 
            WHEN UPPER(OINV."DocCur") THEN UPPER(OINV."DocCur") 
            ELSE :MainCurncy 
        END AS "DocCur", OINV."DocTotal" - OINV."VatSum" AS "DocTotalNet", OINV."VatSum", OINV."DocTotal", 
        CASE :MainCurncy 
            WHEN UPPER(OINV."DocCur") THEN '' 
            ELSE UPPER(OINV."DocCur") 
        END AS "DocCurFC", OINV."DocTotalFC" - OINV."VatSumFC" AS "DocTotalFCNet", OINV."VatSumFC", 
        OINV."DocTotalFC", OINV."DiscPrcnt", OINV."DiscSum", OINV."DiscSumFC", OINV."DpmAmnt", OINV."DpmAmntFC", 
        OINV."TotalExpns", OINV."TotalExpFC", OINV."TaxOnExp", OINV."TaxOnExpFc", OINV."RoundDif", OINV."RoundDifFC", 
        OINV."FolioPref", OINV."FolioNum", OINV."Model", OINV."SeqCode", OINV."Serial", OINV."SeriesStr", 
        OINV."SubStr" 
    FROM OINV 
        INNER JOIN OCRD ON OINV."CardCode" = OCRD."CardCode" 
    WHERE 
        CASE :DateType 
            WHEN 'DocDueDate' THEN OINV."DocDueDate" 
            WHEN 'TaxDate' THEN OINV."TaxDate" 
            ELSE OINV."DocDate" 
        END >= :FromDate AND 
        CASE :DateType 
            WHEN 'DocDueDate' THEN OINV."DocDueDate" 
            WHEN 'TaxDate' THEN OINV."TaxDate" 
            ELSE OINV."DocDate" 
        END <= :ToDate 
    UNION 
    SELECT :CompnyName AS "CompanyName", :Country AS "Country", 
        CASE :Country 
            WHEN 'BR' THEN 
            (SELECT COUNT(DISTINCT ("TaxCode")) 
            FROM RIN1 
            WHERE "DocEntry" = ORIN."DocEntry") 
            ELSE 
            (SELECT COUNT(DISTINCT ("VatGroup")) 
            FROM RIN1 
            WHERE "DocEntry" = ORIN."DocEntry") 
        END AS "VatCount", ORIN."DocEntry", ORIN."DocNum", ORIN."ObjType", ORIN."CardCode", OCRD."CardName", 
        ORIN."DocDate", ORIN."TaxDate", ORIN."DocDueDate", 
        CASE :MainCurncy 
            WHEN UPPER(ORIN."DocCur") THEN UPPER(ORIN."DocCur") 
            ELSE :MainCurncy 
        END AS "DocCur", (ORIN."DocTotal" - ORIN."VatSum") * -1 AS "DocTotalNet", ORIN."VatSum" * -1, 
        ORIN."DocTotal" * -1, 
        CASE :MainCurncy 
            WHEN UPPER(ORIN."DocCur") THEN '' 
            ELSE UPPER(ORIN."DocCur") 
        END AS "DocCurFC", (ORIN."DocTotalFC" - ORIN."VatSumFC") * -1 AS "DocTotalFCNet", ORIN."VatSumFC" * -1, 
        ORIN."DocTotalFC" * -1, ORIN."DiscPrcnt", ORIN."DiscSum" * -1, ORIN."DiscSumFC" * -1, ORIN."DpmAmnt" * -1, 
        ORIN."DpmAmntFC" * -1, ORIN."TotalExpns" * -1, ORIN."TotalExpFC" * -1, ORIN."TaxOnExp" * -1, 
        ORIN."TaxOnExpFc", ORIN."RoundDif" * -1, ORIN."RoundDifFC" * -1, ORIN."FolioPref", ORIN."FolioNum", 
        ORIN."Model", ORIN."SeqCode", ORIN."Serial", ORIN."SeriesStr", ORIN."SubStr" 
    FROM ORIN 
        INNER JOIN OCRD ON ORIN."CardCode" = OCRD."CardCode" 
    WHERE 
        CASE :DateType 
            WHEN 'DocDueDate' THEN ORIN."DocDueDate" 
            WHEN 'TaxDate' THEN ORIN."TaxDate" 
            ELSE ORIN."DocDate" 
        END >= :FromDate AND 
        CASE :DateType 
            WHEN 'DocDueDate' THEN ORIN."DocDueDate" 
            WHEN 'TaxDate' THEN ORIN."TaxDate" 
            ELSE ORIN."DocDate" 
        END <= :ToDate;
    /* Fields specific for CL, MX */
    /* Fields Specific for BR */
    /* BR - Document Header */
    /* Fields specific for CL, MX */
    /* Fields Specific for BR */
    /* BR - Document Header */
END











