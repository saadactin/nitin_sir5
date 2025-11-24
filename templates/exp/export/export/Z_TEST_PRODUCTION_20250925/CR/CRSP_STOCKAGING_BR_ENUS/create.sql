-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_StockAging_BR_enUS (in ValuationDateParam timestamp, in DateTypeParam varchar(32), in ItemCodeFromParam varchar(50), ItemCodeToParam varchar(50)) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
ValuationDate timestamp;
DateType varchar(32);
ItemCodeFrom varchar(50);
ItemCodeTo varchar(50);
BEGIN /*
    
***** General Information *****
Name: OINM_StockAging
Description: Return inventory postings within 2 years before the selected valuation date
Creator: coresystems ag for SAP AG, muf@coresystems.ch
Create Date: 2011-06-22
*/
    /* Define parameters */
    --SELECT (SELECT CAST(FLOOR(CAST(GETDATE() AS decimal(12, 5))) AS timestamp)) INTO ValuationDate FROM DUMMY;
    /* Parameters to enter */
    ValuationDate := ValuationDateParam;
    DateType := DateTypeParam;
    ItemCodeFrom := ItemCodeFromParam;
    ItemCodeTo := ItemCodeToParam;
	
	
	IF ItemCodeFrom = '' THEN
		SELECT  (SELECT MIN("ItemCode")  FROM OITM) INTO ItemCodeFrom FROM DUMMY;
	END IF;
	
	IF ItemCodeTo = '' THEN
		SELECT  (SELECT MAX("ItemCode")  FROM OITM) INTO ItemCodeTo FROM DUMMY;
	END IF;
	
	
    SELECT T0."ItemCode", IFNULL(T1."ItemName", '') AS "ItemName", 
        CAST(T2."ItmsGrpCod" AS nvarchar(20)) AS "ItmsGrpCod", IFNULL(T2."ItmsGrpNam", '') AS "ItmsGrpNam", 
        T0."Warehouse", T1."CreateDate", SUM(T0."InQty") - SUM(T0."OutQty") AS "InStock", 
        SUM(T0."TransValue") AS "TransValue", SUM(
        CASE 
            WHEN 
            CASE :DateType 
                WHEN 'CreateDate' THEN T0."CreateDate" 
                ELSE T0."DocDate" 
            END >= ADD_YEARS(:ValuationDate, -2) THEN IFNULL(T0."OutQty", 0.0) 
            ELSE 0.0 
        END) AS "ConsumptionL2Y", 
        CASE 
            WHEN SUM(
            CASE 
                WHEN 
                CASE :DateType 
                    WHEN 'CreateDate' THEN T0."CreateDate" 
                    ELSE T0."DocDate" 
                END >= ADD_YEARS(:ValuationDate, -2) THEN IFNULL(T0."OutQty", 0.0) 
                ELSE 0.0 
            END) = 0.0 THEN 0.0 
            ELSE 24 * (SUM(T0."InQty") - SUM(T0."OutQty")) / SUM(
            CASE 
                WHEN 
                CASE :DateType 
                    WHEN 'CreateDate' THEN T0."CreateDate" 
                    ELSE T0."DocDate" 
                END >= ADD_YEARS(:ValuationDate, -2) THEN IFNULL(T0."OutQty", 0.0) 
                ELSE 0.0 
            END) 
        END AS "StockReach", MAX(
        CASE 
            WHEN T0."TransType" = 67 OR T0."InQty" <= 0.0 THEN '19000101' 
            ELSE (
            CASE :DateType 
                WHEN 'CreateDate' THEN T0."CreateDate" 
                ELSE T0."DocDate" 
            END) 
        END) AS "LastReceiptDate", IFNULL(
        (SELECT SUM(IFNULL(T31."InQty", 0.0)) 
        FROM OINM T31 
        WHERE T31."InQty" > 0 AND T31."TransType" <> 67 AND 
            (CASE :DateType WHEN 'CreateDate' THEN T31."CreateDate" ELSE T31."DocDate" END) = 
            ( SELECT MAX(CASE WHEN T131."TransType" = 67 OR T131."InQty" <= 0.0 THEN '19000101' ELSE (
                CASE :DateType WHEN 'CreateDate' THEN T131."CreateDate" ELSE T131."DocDate" END) END)
              FROM OINM T131 WHERE T131."InQty" > 0 AND T131."TransType" <> 67 AND T131."ItemCode" = T0."ItemCode" AND T131."Warehouse" = T0."Warehouse"
            	AND (CASE :DateType WHEN 'CreateDate' THEN T31."CreateDate" ELSE T31."DocDate" END) <= :ValuationDate ) 
            AND T31."ItemCode" = T0."ItemCode" AND T31."Warehouse" = T0."Warehouse" AND (
            CASE :DateType 
                WHEN 'CreateDate' THEN T31."CreateDate" 
                ELSE T31."DocDate" 
            END) <= :ValuationDate), 0.0) AS "LastReceiptQuant", MAX(
        CASE 
            WHEN T0."TransType" = 67 OR T0."OutQty" <= 0.0 THEN '19000101' 
            ELSE (
            CASE :DateType 
                WHEN 'CreateDate' THEN T0."CreateDate" 
                ELSE T0."DocDate" 
            END) 
        END) AS "LastIssueDate", IFNULL(
        (SELECT SUM(IFNULL(T41."OutQty", 0.0)) 
        FROM OINM T41 
        WHERE T41."OutQty" > 0 AND T41."TransType" <> 67 AND 
        	(CASE :DateType WHEN 'CreateDate' THEN T41."CreateDate" ELSE T41."DocDate" END) = 
        	( SELECT MAX(CASE WHEN T141."TransType" = 67 OR T141."OutQty" <= 0.0 THEN '19000101' ELSE (
                CASE :DateType WHEN 'CreateDate' THEN T141."CreateDate" ELSE T141."DocDate" END) END)  
            FROM OINM T141 WHERE T141."OutQty" > 0 AND T141."TransType" <> 67 AND T141."ItemCode" = T0."ItemCode" AND T141."Warehouse" = T0."Warehouse" 
            	AND ( CASE :DateType WHEN 'CreateDate' THEN T141."CreateDate" ELSE T141."DocDate" END) <= :ValuationDate)
            AND T41."ItemCode" = T0."ItemCode" AND T41."Warehouse" = T0."Warehouse" AND (
            CASE :DateType 
                WHEN 'CreateDate' THEN T41."CreateDate" 
                ELSE T41."DocDate" 
            END) <= :ValuationDate), 0.0) AS "LastIssueQuant" 
    FROM OINM T0 
        INNER JOIN OITM T1 ON T0."ItemCode" = T1."ItemCode" 
        INNER JOIN OITB T2 ON T1."ItmsGrpCod" = T2."ItmsGrpCod" 
    WHERE (
        CASE :DateType 
            WHEN 'CreateDate' THEN T0."CreateDate" 
            ELSE T0."DocDate" 
        END) <= :ValuationDate AND T0."ItemCode" >= :ItemCodeFrom AND
     T0."ItemCode" <= :ItemCodeTo 
    GROUP BY T0."ItemCode", T1."ItemName", T2."ItmsGrpCod", T2."ItmsGrpNam", T0."Warehouse", T1."CreateDate" 
    ORDER BY T0."ItemCode";
    --[Note:Modifier] HANA doesn't have CONVERT you may you see CAST or implicit casting or TO_... functions
    /* Total Outgoing Quantity within 2 years before valuation date */
    /* Stock Availability in Months */
    /* Zero for inactive and new items (consumption last 2 years = 0.0) */
    /* Last Receipt Date before Valuation Date */
    /* 24 * (Total Quantity in Stock at valuation date) / (Total Outgoing Quantity within 2 years before valuation date) 
    */
    /* Last Receipt Quantity before Valuation Date */
    /* Last Issue Date before Valuation Date */
    /* Last Issue Quantity before Valuation Date */
END











