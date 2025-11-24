-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2
CREATE PROCEDURE CRSP_PayableReceivable_BR_enUS2
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
sqlSelect nvarchar(32000);
ObjectID nvarchar(20);
TableName nvarchar(3);
ObjDesc nvarchar(100);
Factor nvarchar(2);
temp_var_0 integer;
next INT;
columnName nvarchar(100);
CURSOR doctables_Cursor FOR SELECT "ObjectID", "TableName", "ObjDesc", 
    "Factor"
FROM "CRSP_PaymentsOnInvoice" 
WHERE "ObjectID" IN ('24','46');

BEGIN /*
***** General Information *****
Name: OPAY_PaymentsOnInvoices
Description:
Returns the Relevant information of incoming and outgoing payments linked invoices

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
    	(SELECT "LawsSet" FROM CINF) 
    INTO Country FROM DUMMY;
    sqlSelect := '';
    /* ***** CREATE A TEMP TABLE TO STORE THE RELEVANT INFORMATION FOR ALL SUPPORTED MARKETING DOCUMENTS ***** */
	
	insert into "CRSP_PaymentsOnInvoice" values( '13', 'INV', 'AR Invoice', '1');
    insert into "CRSP_PaymentsOnInvoice" values( '14', 'RIN', 'AR Credit Note', '-1');
    insert into "CRSP_PaymentsOnInvoice" values( '15', 'DLN', 'Delivery', '1');
    insert into "CRSP_PaymentsOnInvoice" values( '17', 'RDR', 'Sales Order', '1');
    insert into "CRSP_PaymentsOnInvoice" values( '18', 'PCH', 'AP Invoice', '-1');
    insert into "CRSP_PaymentsOnInvoice" values( '19', 'RPC', 'AP Credit Note', '1');
    insert into "CRSP_PaymentsOnInvoice" values( '20', 'PDN', 'Goods Receipt PO', '-1');
    insert into "CRSP_PaymentsOnInvoice" values( '203', 'DPI', 'AR Down Payment', '1');
    insert into "CRSP_PaymentsOnInvoice" values( '204', 'DPO', 'AP Down Payment', '-1'); 
    insert into "CRSP_PaymentsOnInvoice" values( '22', 'POR', 'Purchase Order', '-1'); 
    insert into "CRSP_PaymentsOnInvoice" values( '23', 'QUT', 'Sales Quotation', '1');
    insert into "CRSP_PaymentsOnInvoice" values( '30', 'JDT', 'Journal Entries', '1'); 
    insert into "CRSP_PaymentsOnInvoice" values( '24', 'RCT', 'Incoming Payment', '1'); 
    insert into "CRSP_PaymentsOnInvoice" values( '46', 'VPM', 'Outgoing Payment', '-1');
    
    /* ***** CREATE THE CURSOR FOR THE TABLES TO USE. 
 	Define here which tables are interesting for the report
	***** */
	
    --OPEN doctables_Cursor;
    OPEN doctables_Cursor;
    
	next := 1;
	WHILE :next > 0 DO
    	FETCH doctables_Cursor INTO ObjectID, TableName, ObjDesc, Factor;
    	if doctables_Cursor::NOTFOUND THEN
    		next := -1;
    		continue;
    	end if;
    	IF :sqlSelect <> '' THEN 
    		sqlSelect := :sqlSelect || 'UNION ALL';
    	END IF;
    	if (:DateType = 'DocDueDate') then
    		columnName := '"DocDueDate"';
    	end if;
    	if (:DateType = 'TaxDate') then
    		columnName := '"TaxDate"';
    	else
    		columnName := '"DocDate"';
    	end if;
        sqlSelect := :sqlSelect || '  
  SELECT
  /* Unique IDs*/
''ObjType''||CAST(IFNULL(T2."InvType",0) AS NVARCHAR(100))||''DocEntry''||CAST(IFNULL(T2."DocEntry",0) AS NVARCHAR(100)) AS DocID
 ,''ObjType''||CAST(IFNULL(T2."InvType",0) AS NVARCHAR(100))||''DocEntry''||CAST(IFNULL(T2."DocEntry",0) AS NVARCHAR(100))||''InstID''||CAST(IFNULL(T2."DocLine",-1) + 1 AS NVARCHAR(100)) AS DocInstID
 ,''ObjType''||CAST(T1."ObjType" AS NVARCHAR(100))||''DocEntry''||CAST(T1."DocEntry" AS NVARCHAR(100)) AS PayID
 ,''ObjType''||CAST(IFNULL(T1."ObjType",0) AS NVARCHAR(100))||''DocEntry''||CAST(IFNULL(T1."DocEntry",0) AS NVARCHAR(100))||''InvoiceId''||CAST(IFNULL(T2."InvoiceId",0) AS NVARCHAR(100)) AS PayInvID
 /* Object */
 ,''' || :ObjectID || ''' AS ObjectID
 ,''' || :TableName || ''' AS TableName
 ,''' || :ObjDesc || ''' AS ObjDesc
 ,' || :Factor || ' AS Factor
 /* Company */
 ,''' || :Country || ''' AS Country
 /* PAY2 */
 ,T2."InvType"
 ,T2."DocEntry" AS InvEntry
 ,T2."InvoiceId"
 ,T2."DocLine" AS InvLine
 ,T1.'|| :columnName || ' AS FilterDate
 /* OPAY */
 ,T1."DocDate"
 ,T1."DocDueDate"
 ,T1."TaxDate"
 ,T1."CardCode"
 ,T1."CardName"
 ,T1."DocCurr" AS DocCur
 ,T1."DocEntry"
 ,T1."DocNum"
 ,T1."Status" AS DocStatus
 ,T1."ObjType"
 ,T1."Serial"
 ,T1."SeriesStr"
 ,T1."SubStr"
 ,T1."SeqCode"
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
 /* PAY2 */
 ,T2."SumApplied"
 ,T2."AppliedFC"
 ,T2."AppliedSys"
 FROM O' || :TableName || ' T1
 LEFT OUTER JOIN ' || :TableName || '2 T2 ON T1."DocEntry" = T2."DocNum"
 WHERE T1."Canceled" = ''N''
 AND T1.'|| :columnName || ' >= ''' ||:FromDate||'''
 AND T1.'|| :columnName || ' <= ''' ||:ToDate||'''
 AND T2."InvType" IN (''13'',''14'',''18'',''19'',''203'',''204'',''24'',''46'',''30'')
 ';


/* DEBUG 
 PRINT @sqlSelect 
 */
    END WHILE;
    
	CLOSE doctables_Cursor;
    
	delete from "CRSP_PaymentsOnInvoice";
    /* DEBUG 
PRINT @sqlSelect 
*/
    execute immediate(:sqlSelect);
	
END;











