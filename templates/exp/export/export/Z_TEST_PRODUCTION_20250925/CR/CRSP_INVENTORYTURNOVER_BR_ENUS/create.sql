-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_InventoryTurnover_BR_enUS (in DateFromParam timestamp, in DateToParam timestamp, in DateTypeParam varchar(32), in ItemCodeFromParam varchar(50), in ItemCodeToParam varchar(50)) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
DateFrom timestamp;
DateTo timestamp;
DateType varchar(32);
ItemCodeFrom varchar(50);
ItemCodeTo varchar(50);
BEGIN 
    DateFrom := DateFromParam;
    DateTo := DateToParam;
    DateType := DateTypeParam;
    ItemCodeFrom := ItemCodeFromParam;
    ItemCodeTo := ItemCodeToParam;
		
	
	IF ItemCodeFrom = '' THEN
		SELECT  (SELECT MIN("ItemCode")  FROM OITM) INTO ItemCodeFrom FROM DUMMY;
	END IF;
	
	IF ItemCodeTo = '' THEN
		SELECT  (SELECT MAX("ItemCode")  FROM OITM) INTO ItemCodeTo FROM DUMMY;
	END IF;

    SELECT T0."ItemCode", T1."ItemName", CAST(T2."ItmsGrpCod" AS nvarchar(20)) AS "ItmsGrpCod", T2."ItmsGrpNam", 
        T0."Warehouse", T1."CreateDate", T0."Currency", ifnull(
        (SELECT SUM(T10."InQty") - SUM(T10."OutQty") 
        FROM OINM T10 
        WHERE T10."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T10."DocDate" 
                WHEN 'CreateDate' THEN T10."CreateDate" 
                ELSE T10."DocDate" 
            END) < :DateFrom AND T10."Warehouse" =T0."Warehouse" AND T10."ItemCode" = T0."ItemCode"), 0) AS "InStockBegin", ifnull(
        (SELECT SUM(T20."TransValue") 
        FROM OINM T20 
        WHERE T20."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T20."DocDate" 
                WHEN 'CreateDate' THEN T20."CreateDate" 
                ELSE T20."DocDate" 
            END) < :DateFrom AND T20."Warehouse" =T0."Warehouse" AND T20."ItemCode" = T0."ItemCode"), 0) AS "StockValueBegin", ifnull(
        (SELECT SUM(T30."TransValue") 
        FROM OINM T30 
        WHERE T30."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T30."DocDate" 
                WHEN 'CreateDate' THEN T30."CreateDate" 
                ELSE T30."DocDate" 
            END) >= :DateFrom AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T30."DocDate" 
                WHEN 'CreateDate' THEN T30."CreateDate" 
                ELSE T30."DocDate" 
            END) <= :DateTo AND T30."Warehouse" =T0."Warehouse" AND T30."ItemCode" = T0."ItemCode" AND T30."TransValue" > 0), 0) AS "Receipts", ifnull(
        (SELECT SUM(T31."InQty") 
        FROM OINM T31 
        WHERE T31."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T31."DocDate" 
                WHEN 'CreateDate' THEN T31."CreateDate" 
                ELSE T31."DocDate" 
            END) >= :DateFrom AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T31."DocDate" 
                WHEN 'CreateDate' THEN T31."CreateDate" 
                ELSE T31."DocDate" 
            END) <= :DateTo AND T31."Warehouse" =T0."Warehouse" AND T31."ItemCode" = T0."ItemCode"), 0) AS "ReceiptsQuant", ifnull(
        (SELECT SUM(T40."TransValue") * -1 
        FROM OINM T40 
        WHERE T40."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T40."DocDate" 
                WHEN 'CreateDate' THEN T40."CreateDate" 
                ELSE T40."DocDate" 
            END) >= :DateFrom AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T40."DocDate" 
                WHEN 'CreateDate' THEN T40."CreateDate" 
                ELSE T40."DocDate" 
            END) <= :DateTo AND T40."Warehouse" =T0."Warehouse" AND T40."ItemCode" = T0."ItemCode" AND T40."TransValue" < 0), 0) AS "Issues", ifnull(
        (SELECT SUM(T41."OutQty") 
        FROM OINM T41 
        WHERE T41."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T41."DocDate" 
                WHEN 'CreateDate' THEN T41."CreateDate" 
                ELSE T41."DocDate" 
            END) >= :DateFrom AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T41."DocDate" 
                WHEN 'CreateDate' THEN T41."CreateDate" 
                ELSE T41."DocDate" 
            END) <= :DateTo AND T41."Warehouse" =T0."Warehouse" AND T41."ItemCode" = T0."ItemCode"), 0) AS "IssuesQuant", ifnull(
        (SELECT SUM(T50."InQty") - SUM(T50."OutQty") 
        FROM OINM T50 
        WHERE T50."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T50."DocDate" 
                WHEN 'CreateDate' THEN T50."CreateDate" 
                ELSE T50."DocDate" 
            END) <= :DateTo AND T50."Warehouse" =T0."Warehouse" AND T50."ItemCode" = T0."ItemCode"), 0) AS "InStockEnd", ifnull(
        (SELECT SUM(T60."TransValue") 
        FROM OINM T60 
        WHERE T60."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T60."DocDate" 
                WHEN 'CreateDate' THEN T60."CreateDate" 
                ELSE T60."DocDate" 
            END) <= :DateTo AND T60."Warehouse" =T0."Warehouse" AND T60."ItemCode" = T0."ItemCode"), 0) AS "StockValueEnd", ifnull(
        (SELECT SUM(T60."TransValue") 
        FROM OINM T60 
        WHERE T60."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T60."DocDate" 
                WHEN 'CreateDate' THEN T60."CreateDate" 
                ELSE T60."DocDate" 
            END) <= :DateTo AND T60."Warehouse" =T0."Warehouse" AND T60."ItemCode" = T0."ItemCode"), 0) - ifnull(
        (SELECT SUM(T20."TransValue") 
        FROM OINM T20 
        WHERE T20."TransType" <> 67 AND (
            CASE :DateType 
                WHEN 'PostingDate' THEN T20."DocDate" 
                WHEN 'CreateDate' THEN T20."CreateDate" 
                ELSE T20."DocDate" 
            END) < :DateFrom AND T20."Warehouse" =T0."Warehouse" AND T20."ItemCode" = T0."ItemCode"), 0) AS "Turnover" 
    FROM OINM T0 
        INNER JOIN OITM T1 ON T0."ItemCode" = T1."ItemCode" 
        INNER JOIN OITB T2 ON T1."ItmsGrpCod" = T2."ItmsGrpCod" 
    WHERE T0."TransType" <> 67 AND (
        CASE :DateType 
            WHEN 'PostingDate' THEN T0."DocDate" 
            WHEN 'CreateDate' THEN T0."CreateDate" 
            ELSE T0."DocDate" 
        END) >= :DateFrom AND (
        CASE :DateType 
            WHEN 'PostingDate' THEN T0."DocDate" 
            WHEN 'CreateDate' THEN T0."CreateDate" 
            ELSE T0."DocDate" 
        END) <= :DateTo AND 
        T0."ItemCode" >= :ItemCodeFrom AND T0."ItemCode" <= :ItemCodeTo 
    GROUP BY T0."ItemCode", T1."ItemName", T2."ItmsGrpCod", T2."ItmsGrpNam", T0."Warehouse", T1."CreateDate", T0."Currency" 
    ORDER BY T0."ItemCode";
    --[Note:Modifier] HANA doesn't have CONVERT you may you see CAST or implicit casting or TO_... functions
END











