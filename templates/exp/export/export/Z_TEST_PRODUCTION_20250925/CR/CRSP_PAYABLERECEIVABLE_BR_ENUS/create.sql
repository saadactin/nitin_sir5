-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2
CREATE PROCEDURE CRSP_PayableReceivable_BR_enUS
(
in PayDateType nvarchar(150),
in PayDateFrom timestamp,
in PayDateTo timestamp
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
AS 
DateType nvarchar(150);
FromDate timestamp;
ToDate timestamp;
FinancYear timestamp;
Country nvarchar(3);
sqlSelect nvarchar(30000);
ObjectID nvarchar(20);
TableName nvarchar(3);
ObjDesc nvarchar(100);
Factor nvarchar(2);
invoice_DocDueDate integer;
temp_var_0 integer;
next INT;
columnName nvarchar(100);
columnName2 nvarchar(100);--for other CASE WHEN THEN END statements
columnName3 nvarchar(100);--for other CASE WHEN THEN END statements
canceledColumn nvarchar(100);
CURSOR doctables_Cursor FOR SELECT "ObjectID", "TableName", "ObjDesc", 
    "Factor" 
FROM "CRSP_PaymentsOnInvoice" 
WHERE "ObjectID" IN ('13','14','18','19','203','204','24','46','30');
BEGIN /*
***** General Information *****
Name: ODOC_PayableReceivable
Description:
Returns the Relevant information of Sales and Purchasing documents
and payments

Creator: coresystems ag, muf@coresystems.ch
Create Date: 2011-20-10

***** Updates *****

*/
    /* ***** PARAMETERS FOR STANDARD FILTERS ***** */
    /* ***** DEFAULT PARAMETER VALUES ***** */
    DateType := 'DocDate';
    /* Get the current Date without the time */
    SELECT TO_TIMESTAMP(CURRENT_DATE, 'YYYY-MM-DD') into ToDate FROM DUMMY;
    /* Get the current Finance Year */
    SELECT 
    (SELECT T0."FinancYear" 
    FROM OACP T0 
        INNER JOIN OFPR T1 ON T0."PeriodCat" = T1."Category" 
    WHERE T1."F_RefDate" <= :ToDate AND T1."T_RefDate" >= :ToDate) INTO FinancYear FROM DUMMY;
    FromDate := ADD_YEARS(:FinancYear, -1);
    ToDate := ADD_DAYS(ADD_YEARS(:FinancYear, 1), -1);
    /* Set the parameter values (comment for debugging) */
    DateType := :PayDateType;
    FromDate := :PayDateFrom;
    ToDate := :PayDateTo;
    /* ***** INTERNAL PARAMETERS ***** */
    SELECT 
    (SELECT "LawsSet" 
    FROM CINF) INTO Country FROM DUMMY;
    sqlSelect := '';
    /* ***** CREATE A TEMP TABLE TO STORE THE RELEVANT INFORMATION FOR ALL SUPPORTED MARKETING DOCUMENTS ***** */
    insert into "CRSP_PaymentsOnInvoice" values('13', 'INV', 'AR Invoice', '1');
	insert into "CRSP_PaymentsOnInvoice" values('14', 'RIN', 'AR Credit Note', '-1');
	insert into "CRSP_PaymentsOnInvoice" values('15', 'DLN', 'Delivery', '1');
	insert into "CRSP_PaymentsOnInvoice" values('17', 'RDR', 'Sales Order', '1');
	insert into "CRSP_PaymentsOnInvoice" values('18', 'PCH', 'AP Invoice', '-1');
	insert into "CRSP_PaymentsOnInvoice" values('19', 'RPC', 'AP Credit Note', '1');
	insert into "CRSP_PaymentsOnInvoice" values('20', 'PDN', 'Goods Receipt PO', '-1');
	insert into "CRSP_PaymentsOnInvoice" values('203', 'DPI', 'AR Down Payment', '1');
	insert into "CRSP_PaymentsOnInvoice" values('204', 'DPO', 'AP Down Payment', '-1');
	insert into "CRSP_PaymentsOnInvoice" values('22', 'POR', 'Purchase Order', '-1');
	insert into "CRSP_PaymentsOnInvoice" values('23', 'QUT', 'Sales Quotation', '1');
	insert into "CRSP_PaymentsOnInvoice" values('24', 'RCT', 'Incoming Payment', '1');
	insert into "CRSP_PaymentsOnInvoice" values('46', 'VPM', 'Outgoing Payment', '-1');
	insert into "CRSP_PaymentsOnInvoice" values('30', 'JDT', 'Journal Posting', '1');
	
    /* ***** CREATE THE CURSOR FOR THE TABLES TO USE. 
 Define here which tables are interesting for the report
***** */
    OPEN doctables_Cursor;
    
    next := 1;
	WHILE :next > 0 DO
    	FETCH doctables_Cursor INTO ObjectID, TableName, ObjDesc, Factor; 
        if doctables_Cursor::NOTFOUND THEN
        	next := -1;
    		continue;
    	end if;
    	if (:DateType = 'DocDueDate') then
    		columnName := '"DocDueDate"';
    	elseif (:DateType = 'TaxDate') then
    		columnName := '"TaxDate"';
    	else
    		columnName := '"DocDate"';
    	end if;
    	--
		--exec('select count("ObjType") into temp_var_0 from O'||:TableName ||' where "ObjType" IN (''13'', ''14'')');
    	if (:DateType = 'DocDueDate') then
    		columnName2 := '"DocDueDate"';
    	elseif (:DateType = 'TaxDate') then
    		columnName2 := '"TaxDate"';
    	else
    		columnName2 := '"DocDate"';
    	end if;
    	--
		if (:DateType = 'DocDueDate') then
			  columnName3 := '"DueDate"';
        elseif (:DateType = 'TaxDate') then
    		columnName3 := '"TaxDate"';
        else
    		columnName3 := '"RefDate"';
        end if;
    	--
		if (:TableName = 'VPM' OR :TableName = 'RCT') then
			canceledColumn := '"Canceled"';
		else
			canceledColumn := 'CANCELED';
		end if;	 
		--
        IF :sqlSelect <> '' THEN 
    		sqlSelect := :sqlSelect || ' UNION ALL ';
        /* PART 1: DOCUMENTS */
        END IF;
        IF (:ObjectID = '13' OR  :ObjectID = '14' OR :ObjectID = '18' OR  :ObjectID = '19' OR :ObjectID = '203' OR  :ObjectID = '204') THEN 
        
        	IF ( (:ObjectID = '13' OR :ObjectID = '14' OR :ObjectID = '18' OR :ObjectID = '19' ) and (:DateType = 'DocDueDate') ) THEN 
				invoice_DocDueDate := 1;
	     	ELSE invoice_DocDueDate := 0; 
			END IF;
				
            sqlSelect := :sqlSelect || 'SELECT 
 /* Unique IDs*/
 ''ObjType''||CAST(T1."ObjType" AS NVARCHAR(100))||''DocEntry''||CAST(T0."DocEntry" AS NVARCHAR(100)) AS DocID
 ,''ObjType''||CAST(T1."ObjType" AS NVARCHAR(100))||''DocEntry''||CAST(T0."DocEntry" AS NVARCHAR(100))||''InstID''||CAST(T0."InstlmntID" AS NVARCHAR(100)) AS DocInstID
 /* Object */
 ,''' || :ObjectID || ''' AS ObjectID
 ,''' || :TableName || ''' AS TableName
 ,''' || :ObjDesc || ''' AS ObjDesc
 ,' || :Factor || ' AS Factor
 /* Company */
 ,''' || :Country || ''' AS Country
 /* ODOC */
 ,T1.'|| :columnName2 || ' AS FilterDate
 ,T1."DocDate"
 ,T1."DocDueDate"
 ,T1."TaxDate"
 ,T1."CardCode"
 ,T1."CardName"
 ,T1."DocCur"
 ,T1."DocEntry"
 ,T1."DocNum"
 ,IFNULL(T1."DocStatus",''O'') AS DocStatus
 ,T1."FolioNum"
 ,T1."FolioPref"
 ,T1."IsICT"
 ,T1."isIns"
 ,T1."ObjType"
 ,T1."PeyMethod"
 ,T1."Posted"
 ,T1."Model"
 ,T1."Serial"
 ,T1."SeriesStr"
 ,T1."SubStr"
 ,T1."SeqCode"
 /* DOC6 */
 ,T0."DueDate"
 ,T0."InstlmntID"
 ,T0."InsTotal"
 ,T0."InsTotalFC"
 ,T0."InsTotalSy"
 ,T0."Paid"
 ,T0."PaidFC"
 ,T0."PaidSc"
 ,IFNULL(T0."Status",''O'') AS InsStatus
 /* OPAY */
 ,0.0 AS BoeSum
 ,0.0 AS BoeSumFc
 ,0.0 AS BoeSumSc
 ,0.0 AS CashSum
 ,0.0 AS CashSumFC
 ,0.0 AS CashSumSy
 ,0.0 AS CheckSum
 ,0.0 AS CheckSumFC
 ,0.0 AS CheckSumSy
 ,0.0 AS CreditSum
 ,0.0 AS CredSumFC
 ,0.0 AS CredSumSy
 ,0.0 AS TrsfrSum
 ,0.0 AS TrsfrSumFC
 ,0.0 AS TrsfrSumSy
 FROM ' || :TableName || '6 T0
 INNER JOIN O' || :TableName || ' T1 ON T0."DocEntry" = T1."DocEntry"
 WHERE T1.'|| :canceledColumn ||' = ''N'' AND (T0."InsTotal" <> 0 OR T0."InsTotalFC" <> 0 OR T0."InsTotalSy" <> 0)
AND CASE('||:invoice_DocDueDate||') WHEN 1 THEN T0."DueDate" ELSE T1.'|| :columnName2 || ' END >= ''' || :FromDate || ''' 
AND CASE('||:invoice_DocDueDate||') WHEN 1 THEN T0."DueDate" ELSE T1.'|| :columnName2 || ' END <= ''' || :ToDate || ''' 
 ';
 ELSE 
            IF :ObjectID = '24' OR  :ObjectID = '46' THEN 
                sqlSelect := :sqlSelect || '
                
 SELECT
 /* Unique IDs*/
 ''ObjType''||CAST(IFNULL(T2."InvType",T1."ObjType") AS NVARCHAR(100))||''DocEntry''||CAST(IFNULL(T2."DocEntry",T1."DocEntry") AS NVARCHAR(100)) AS DocID
 ,''ObjType''||CAST(IFNULL(T2."InvType",T1."ObjType") AS NVARCHAR(100))||''DocEntry''||CAST(IFNULL(T2."DocEntry",T1."DocEntry") AS NVARCHAR(100))||''InstID''||CAST(IFNULL(T2."DocLine",-1) || 1 AS NVARCHAR(100)) AS DocInstID
 /* Object */
 ,''' || :ObjectID || ''' AS ObjectID
 ,''' || :TableName || ''' AS TableName
 ,''' || :ObjDesc || ''' AS ObjDesc
 ,' || :Factor || ' AS Factor
 /* Company */
 ,''' || :Country || ''' AS Country
 /* OPAY / ODOC */
 ,T1.'|| :columnName || ' AS FilterDate
 ,T1."DocDate"
 ,T1."DocDueDate"
 ,T1."TaxDate"
 ,T1."CardCode"
 ,IFNULL(T1."CardName",T1."JrnlMemo") AS CardName
 ,T1."DocCurr" AS DocCur
 ,T1."DocEntry"
 ,T1."DocNum"
 ,''C'' AS DocStatus
 ,NULL AS FolioNum
 ,NULL AS FolioPref
 ,NULL AS IsICT
 ,NULL AS isIns
 ,T1."ObjType"
 ,NULL AS PeyMethod
 ,NULL AS Posted
 ,NULL AS Model
 ,T1."Serial"
 ,T1."SeriesStr"
 ,T1."SubStr"
 ,T1."SeqCode"
 /* OPAY / DOC6 */
 ,T1."DocDueDate" AS DueDate
 ,-1 AS InstlmntID
 ,0.0 AS InsTotal
 ,0.0 AS InsTotalFC
 ,0.0 AS InsTotalSy
 ,(T1."OpenBal") * -1 AS Paid
 ,(T1."OpenBalFc") * -1 AS PaidFC
 ,(T1."OpenBalSc") * -1 AS PaidSc
 ,''C'' AS InsStatus
 /* OPAY */
 ,T1."BoeSum"
 ,T1."BoeSumFc"
 ,T1."BoeSumSc"
 ,T1."CashSum"
 ,T1."CashSumFC"
 ,T1."CashSumSy"
 ,T1."CheckSum"
 ,T1."CheckSumFC"
 ,T1."CheckSumSy"
 ,T1."CreditSum"
 ,T1."CredSumFC"
 ,T1."CredSumSy"
 ,T1."TrsfrSum"
 ,T1."TrsfrSumFC"
 ,T1."TrsfrSumSy"
 FROM O' || :TableName || ' T1
 LEFT OUTER JOIN ' || :TableName || '2 T2 ON T1."DocEntry" = T2."DocNum"
 WHERE T1.'|| :canceledColumn ||' = ''N'' AND T1."DocType" <> ''A''
 AND T1.'|| :columnName || ' >= ''' ||:FromDate||'''
 AND T1.'|| :columnName || ' <= ''' ||:ToDate||'''
 AND IFNULL(T2."InvType",''-1'') NOT IN (''13'',''14'',''18'',''19'',''203'',''204'',''24'',''46'',''30'')
 ';
                /* DEBUG 
 PRINT @sqlSelect 
 */
            ELSE 
                IF  :ObjectID = '30' THEN 
                    sqlSelect := :sqlSelect || '
                    
 SELECT 
 /* Unique IDs*/
 ''ObjType''||CAST(T1."ObjType" AS NVARCHAR(100))||''DocEntry''||CAST(T0."TransId" AS NVARCHAR(100)) AS DocID
 ,''ObjType''||CAST(T1."ObjType" AS NVARCHAR(100))||''DocEntry''||CAST(T0."TransId" AS NVARCHAR(100))||''InstID''||CAST(T0."Line_ID" || 1 AS NVARCHAR(100)) AS DocInstID
 /* Object */
 ,''' || :ObjectID || ''' AS ObjectID
 ,''' || :TableName || ''' AS TableName
 ,''' || :ObjDesc || ''' AS ObjDesc
 ,' || 'CASE T2."CardType" WHEN ''C'' THEN ''1'' ELSE ''-1'' END' || ' AS Factor
 /* Company */
 ,''' || :Country || ''' AS Country
 /* ODOC */
 ,T1.'|| :columnName3 || ' AS FilterDate
 ,T1."RefDate"
 ,T1."DueDate"
 ,T1."TaxDate"
 ,T0."ShortName" AS CardCode
 ,T2."CardName" AS CardName
 ,T1."TransCurr" AS DocCur
 ,T1."TransId" AS DocEntry
 ,T1."Number" AS DocNum
 ,''O'' AS DocStatus
 ,T1."FolioNum"
 ,T1."FolioPref"
 ,''N'' AS IsICT
 ,''N'' AS isIns
 ,T1."ObjType"
 ,''N'' AS PeyMethod
 ,''Y'' AS Posted
 ,'''' AS Model
 ,T1."Serial"
 ,T1."SeriesStr"
 ,T1."SubStr"
 ,T1."SeqCode"
 /* JDT1 */
 ,T0."DueDate"
 ,T0."Line_ID" AS InstlmntID
 ,' || 'CASE T2."CardType" WHEN ''C'' THEN T0."Debit" - T0."Credit" ELSE T0."Credit"-T0."Debit" END' || ' AS InsTotal
 ,' || 'CASE T2."CardType" WHEN ''C'' THEN T0."FCDebit" - T0."FCCredit" ELSE T0."FCCredit"-T0."FCDebit" END' || ' AS InsTotalFC
 ,' || 'CASE T2."CardType" WHEN ''C'' THEN T0."SYSDeb" - T0."SYSCred" ELSE T0."SYSCred"-T0."SYSDeb" END' || ' AS InsTotalSy
 ,T0."Debit" - T0."Credit" - (T0."BalDueDeb" - T0."BalDueCred") AS Paid
 ,T0."FCDebit" - T0."FCCredit" - (T0."BalFcDeb" - T0."BalFcCred") AS PaidFC
 ,T0."SYSDeb" - T0."SYSCred" - (T0."BalScDeb" - T0."BalScCred") AS PaidSc
 ,''O'' AS InsStatus
 /* OPAY */
 ,0.0 AS BoeSum
 ,0.0 AS BoeSumFc
 ,0.0 AS BoeSumSc
 ,0.0 AS CashSum
 ,0.0 AS CashSumFC
 ,0.0 AS CashSumSy
 ,0.0 AS CheckSum
 ,0.0 AS CheckSumFC
 ,0.0 AS CheckSumSy
 ,0.0 AS CreditSum
 ,0.0 AS CredSumFC
 ,0.0 AS CredSumSy
 ,0.0 AS TrsfrSum
 ,0.0 AS TrsfrSumFC
 ,0.0 AS TrsfrSumSy
 FROM ' || :TableName || '1 T0
 INNER JOIN O' || :TableName || ' T1 ON T0."TransId" = T1."TransId"
 INNER JOIN OCRD T2 ON T0."ShortName" = T2."CardCode"
 WHERE T1."TransType" = ''30''
 AND T0.'|| :columnName3 || ' >= ''' ||:FromDate||'''
 AND T0.'|| :columnName3 || ' <= ''' ||:ToDate||'''
 ';
                END IF;
                /* PART 3: MANUAL JOURNAL POSTINGS FOR BUSINESS PARTNERS */
            END IF;
            /* PART 2: PAYMENTS */
 /* DEBUG 
 PRINT @sqlSelect 
 */        

         END IF;
    END WHILE;
    CLOSE doctables_Cursor;
    delete from "CRSP_PaymentsOnInvoice";
    /* DEBUG 
PRINT @sqlSelect 
*/
    execute immediate (:sqlSelect);
END;











