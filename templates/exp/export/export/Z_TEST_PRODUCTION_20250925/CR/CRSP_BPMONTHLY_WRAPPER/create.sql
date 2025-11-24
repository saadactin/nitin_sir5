-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

CREATE PROCEDURE CRSP_BPMONTHLY_WRAPPER
(
	FromDate1 Date,
	ToDate1 Date,
	DataType VARCHAR(1),
	execSQL NCLOB
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	FromDate VARCHAR(20);
	ToDate VARCHAR(20);
	FromDate2 timestamp;
	ToDate2 timestamp;
	IsExists INTEGER := 0;
	currentDate NVARCHAR(8);
BEGIN

	call _TmSp_ValidateSpParam(DataType);
	call _TmSp_ValidateSpParam(execSQL);
	
	DELETE FROM CRSP_TEMP_OCRD;
	exec ('INSERT INTO CRSP_TEMP_OCRD SELECT T0."CardCode", T0."CardName", T0."GroupCode", T0."Balance" FROM OCRD T0 WHERE ' || :execSQL );

	/*MonthBegin*/
	SELECT TO_CHAR(FromDate1, 'YYYYMMDD'), TO_CHAR(ToDate1, 'YYYYMMDD') INTO FromDate, ToDate FROM DUMMY;
	SELECT COUNT(*) INTO IsExists 
	FROM M_TEMPORARY_TABLES
	WHERE TABLE_NAME = '#MONTHBEGIN' AND SCHEMA_NAME = CURRENT_SCHEMA and connection_id = current_connection;
	
	IF :IsExists > 0 THEN
		DROP TABLE #MONTHBEGIN;
	END IF;
	CREATE LOCAL TEMPORARY ROW TABLE #MONTHBEGIN
	( "DATE" date);
	/*Work around for HANA rev74*/
	DELETE FROM #MONTHBEGIN WHERE 1=1;
	
	currentDate := :FromDate;
	WHILE LEFT(:currentDate, 6) < LEFT(:ToDate, 6) DO
		currentDate := TO_NVARCHAR(ADD_DAYS(LAST_DAY(TO_DATE(:currentDate)),1), 'YYYYMMDD');
		INSERT INTO #MONTHBEGIN VALUES(TO_DATE(:currentDate));
	END WHILE;
					
	select add_months(to_date(substr(:FromDate,1,6)||'01'),-1), 
	TO_DATE(last_day(add_months(to_date(substr(:FromDate,1,6)||'01'),-1))) 
	into FromDate2, ToDate2 from dummy;
	if :DataType = 'P'
	then
	/*Ordered*/
	TBL1 =
		select
			YEAR(T1."DocDate") as "YEARPARAM",
			MONTH(T1."DocDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			CAST(Sum(T1."DocTotal" - T1."VatSum" - T1."TotalExpns" + T1."WTSum" - T1."RoundDif") AS DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			CAST(Sum(T1."GrosProfit") AS DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
	from ORDR T1 join CRSP_TEMP_OCRD T2 on T1."CardCode" = T2."CardCode" 
	where T1."CANCELED" = 'N' 
	and ( (T1."DocDate" >= :FromDate and T1."DocDate" <= :ToDate)
		or (T1."DocDate" >= :FromDate2 
			and T1."DocDate" <= :ToDate2))
	group by T2."CardCode",extract(YEAR FROM (T1."DocDate")),extract(MONTH FROM (T1."DocDate")) 
	;
	/*Order's Gross Base Total*/
	TBL2 = 
		select
			YEAR(T1."DocDate") as "YEARPARAM",
			MONTH(T1."DocDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			CAST ((Case When T4."GrossBySal" = 'Y' then Sum(T3."LineTotal") - Sum(T1."DiscSum")
				 Else Sum(T3."GPTtlBasPr")
			End) AS DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From ORDR T1 
		Join CRSP_TEMP_OCRD T2
		 On T1."CardCode" = T2."CardCode"
		Join "RDR1" T3
		 on T1."DocEntry" = T3."DocEntry"
		Cross Join "OADM" T4
		Where T1."CANCELED" = 'N'
		and		((T1."DocDate" >= :FromDate and T1."DocDate" <= :ToDate)
			or	(T1."DocDate" >= :FromDate2
				and T1."DocDate" <= :ToDate2))
		Group By T2."CardCode",extract(YEAR FROM (T1."DocDate")),extract(MONTH FROM (T1."DocDate")), T4."GrossBySal"
	;
	/*Delivered*/
	TBL3 =
		select
			YEAR(T1."DocDate") as "YEARPARAM",
			MONTH(T1."DocDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			CAST(Sum(T1."DocTotal" - T1."VatSum" - T1."TotalExpns") AS DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From "ODLN" T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" 
		Where T1."DocDate" >= :FromDate 
		and T1."DocDate" <= :ToDate 
		Group By T2."CardCode",extract(YEAR FROM (T1."DocDate")),extract(MONTH FROM (T1."DocDate")) 
	;
	/*Returned*/
	TBL4 =
		select
			YEAR(T1."DocDate") as "YEARPARAM",
			MONTH(T1."DocDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			CAST(Sum(T1."DocTotal" - T1."VatSum" - T1."TotalExpns") AS DECIMAL(21,6))as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From "ORDN" T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" 
		Where T1."DocDate" >= :FromDate 
		and T1."DocDate" <= :ToDate 
		Group By T2."CardCode",extract(YEAR FROM (T1."DocDate")),extract(MONTH FROM (T1."DocDate")) 
	;
	/*Invoiced*/
	TBL51 =
		select
			YEAR(T1."DocDate") as "YEARPARAM",
			MONTH(T1."DocDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			CAST (Sum(T3."Debit" - T3."Credit") AS DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From "OINV" T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" Join "JDT1" T3 on T1."TransId" = T3."TransId" 
		and T3."ShortName" = T2."CardCode" 
		Where T1."DocDate" >= :FromDate 
		and T1."DocDate" <= :ToDate 
		Group By T2."CardCode",extract(YEAR	FROM (T1."DocDate")),extract(MONTH FROM (T1."DocDate")) 
	;
	TBL52 =
		select
			YEAR(T1."DocDate") as "YEARPARAM",
			MONTH(T1."DocDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			CAST(Sum(T3."Debit" - T3."Credit") AS DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From "OCSI" T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" Join "JDT1" T3 on T1."TransId" = T3."TransId" 
		and T3."ShortName" = T2."CardCode" 
		Where T1."DocDate" >= :FromDate 
		and T1."DocDate" <= :ToDate 
		Group By T2."CardCode",extract(YEAR FROM (T1."DocDate")),extract(MONTH FROM (T1."DocDate"))
	;
	TBL53 =
		select
			YEAR(T1."DocDate") as "YEARPARAM",
			MONTH(T1."DocDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			CAST (Sum(T3."Debit" - T3."Credit") AS DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From "OCSV" T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" Join "JDT1" T3 on T1."TransId" = T3."TransId" 
		and T3."ShortName" = T2."CardCode" 
		Where T1."DocDate" >= :FromDate 
		and T1."DocDate" <= :ToDate 
		Group By T2."CardCode",extract(YEAR FROM (T1."DocDate")),extract(MONTH FROM (T1."DocDate")) 
	;
	/*Credited*/
	TBL6 =
		select
			YEAR(T1."DocDate") as "YEARPARAM",
			MONTH(T1."DocDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			CAST (Sum(T3."Debit" - T3."Credit") AS DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE" 
		From "ORIN" T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" Join "JDT1" T3 on T1."TransId" = T3."TransId" 
		and T3."ShortName" = T2."CardCode" 
		Where T1."DocDate" >= :FromDate 
		and T1."DocDate" <= :ToDate 
		Group By T2."CardCode",extract(YEAR FROM (T1."DocDate")),extract(MONTH FROM (T1."DocDate")) 
	;
	/*Collected*/
	TBL7 =
		select
			YEAR(T1."RefDate") as "YEARPARAM",
			MONTH(T1."RefDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			CAST (Sum(T4."DocTotal") AS DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE" 
		From "JDT1" T1 Join CRSP_TEMP_OCRD T2 On T1."ShortName" = T2."CardCode" Join "OJDT" T3 On T1."TransId" = T3."TransId" 
		And T1."TransType" = 24 
		And T3."StornoToTr" IS NULL Join "ORCT" T4 On T3."CreatedBy" = T4."DocEntry" 
		Where T1."RefDate" >= :FromDate 
		and T1."RefDate" <= :ToDate 
		Group By T2."CardCode",extract(YEAR FROM (T1."RefDate")),extract(MONTH FROM (T1."RefDate")) 
	;
	/*OpenBalance*/
	TBL8 =
		Select
			YEAR(T0."Date") as "YEARPARAM",
			MONTH(T0."Date") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			CAST ((Max(T2."Balance") - (Sum(T1."Debit") - Sum(T1."Credit"))) AS DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE" 
		From "JDT1" T1 Join CRSP_TEMP_OCRD T2 on T1."ShortName" = T2."CardCode"
		Join 
		(
			select To_Date(:FromDate) as "Date" from DUMMY
			union
			select "DATE" from #MONTHBEGIN
		) T0
		On T1."RefDate" >= T0."Date" 
		Group By T2."CardCode",YEAR(T0."Date"),MONTH(T0."Date")
	;
	/*CloseBalance*/
	TBL9 =
		Select
			YEAR(T0."Date") as "YEARPARAM",
			MONTH(T0."Date") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			CAST ((Max(T2."Balance") - (Sum(T1."Debit") - Sum(T1."Credit"))) AS DECIMAL(21,6)) as "CLOSEBALANCE" 
		From "JDT1" T1 Join CRSP_TEMP_OCRD T2 on T1."ShortName" = T2."CardCode"
		Join 
		(
			select ADD_DAYS("DATE",-1) as "Date" from #MONTHBEGIN
			where  ADD_DAYS("DATE",-1) >= To_Date(:FromDate)
			union
			select To_Date(:ToDate) as "Date" from DUMMY
		) T0 
		On T1."RefDate" > T0."Date" 
		Group By T2."CardCode",YEAR(T0."Date"),MONTH(T0."Date")
	;

	TBL10 = SELECT * FROM :TBL1
			UNION ALL
			SELECT * FROM :TBL2
			UNION ALL
			SELECT * FROM :TBL3
			UNION ALL
			SELECT * FROM :TBL4
			UNION ALL
			SELECT * FROM :TBL51
			UNION ALL
			SELECT * FROM :TBL52
			UNION ALL
			SELECT * FROM :TBL53
			UNION ALL
			SELECT * FROM :TBL6
			UNION ALL			
			SELECT * FROM :TBL7
			UNION ALL
			SELECT * FROM :TBL8
			UNION ALL
			SELECT * FROM :TBL9
			;
	 
	/*TblResult*/
	TBL11 =
		select
			"YEARPARAM",
			"MONTHPARAM",
			"CARDCODE",
			Max("CARDNAME")as "CARDNAME",
			Max("GROUPCODE") as "GROUPCODE",
			Sum("RDRTOTAL") as "RDRTOTAL",
			Sum("GROSSBASE") as "GROSSBASE",
			Sum("GROSSPROFIT") as "GROSSPROFIT",
			Sum("DLNTOTAL") as "DLNTOTAL",
			Sum("RDNTOTAL") as "RDNTOTAL",
			Sum("INVTOTAL") as "INVTOTAL",
			Sum("RINTOTAL") as "RINTOTAL",
			Sum("RCTTOTAL") as "RCTTOTAL",
			Sum("OPENBALANCE") as "OPENBALANCE",
			Sum("CLOSEBALANCE") as "CLOSEBALANCE" 
		from :TBL10
		Group By "CARDCODE", "YEARPARAM", "MONTHPARAM" 
	;
	select
		T0."YEARPARAM",
		T0."MONTHPARAM",
		T0."CARDCODE",
		T0."CARDNAME",
		T0."GROUPCODE",
		T0."RDRTOTAL",
		T0."GROSSBASE",
		T0."GROSSPROFIT",
		T0."DLNTOTAL",
		T0."RDNTOTAL",
		T0."INVTOTAL",
		T0."RINTOTAL",
		T0."RCTTOTAL",
		Case When "OPENBALANCE" is null
			 And  T0."YEARPARAM" >= Year(To_Date(:FromDate))
			 And  T0."MONTHPARAM" >= Month(To_Date(:FromDate))
			 Then T1."Balance"
			 Else "OPENBALANCE"
		End as "OPENBALANCE",
		Case When "CLOSEBALANCE" is null 
			 And  T0."YEARPARAM" >= Year(To_Date(:FromDate))
			 And  T0."MONTHPARAM" >= Month(To_Date(:FromDate))
			 Then T1."Balance" 
			 Else "CLOSEBALANCE" 
		End as "CLOSEBALANCE"
	from :TBL11 T0 
	Join CRSP_TEMP_OCRD T1
	On T0."CARDCODE" = T1."CardCode"
	Order By T0."CARDCODE" Asc, T0."YEARPARAM" Desc, T0."MONTHPARAM" Desc 
	;
	else
	/*Ordered*/
	TBL1 =
		select
			YEAR(T1."TaxDate") as "YEARPARAM",
			MONTH(T1."TaxDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			CAST (Sum(T1."DocTotal" - T1."VatSum" - T1."TotalExpns" + T1."WTSum" - T1."RoundDif") AS DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			CAST (Sum(T1."GrosProfit") AS DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
	from ORDR T1 join CRSP_TEMP_OCRD T2 on T1."CardCode" = T2."CardCode" 
	where T1."CANCELED" = 'N' 
	and ( (T1."TaxDate" >= :FromDate and T1."TaxDate" <= :ToDate)
		or (T1."TaxDate" >= :FromDate2 
			and T1."TaxDate" <= :ToDate2))
	group by T2."CardCode",YEAR(T1."TaxDate"),MONTH(T1."TaxDate") 
	;
	/*Order's Gross Base Total*/
	TBL2 = 
		select
			YEAR(T1."TaxDate") as "YEARPARAM",
			MONTH(T1."TaxDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			CAST ((Case When T4."GrossBySal" = 'Y' then Sum(T3."LineTotal") - Sum(T1."DiscSum")
				 Else Sum(T3."GPTtlBasPr")
			End) AS DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From ORDR T1 
		Join CRSP_TEMP_OCRD T2
		 On T1."CardCode" = T2."CardCode"
		Join RDR1 T3
		 on T1."DocEntry" = T3."DocEntry"
		Cross Join OADM T4
		Where T1."CANCELED" = 'N'
		and		((T1."TaxDate" >= :FromDate and T1."TaxDate" <= :ToDate)
			or	(T1."TaxDate" >= :FromDate2
				and T1."TaxDate" <= :ToDate2))
		Group By T2."CardCode",YEAR(T1."TaxDate"),MONTH(T1."TaxDate"), T4."GrossBySal"
	;
	/*Delivered*/
	TBL3 =
		select
			YEAR(T1."TaxDate") as "YEARPARAM",
			MONTH(T1."TaxDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			CAST (Sum(T1."DocTotal" - T1."VatSum" - T1."TotalExpns") AS DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From ODLN T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" 
		Where T1."TaxDate" >= :FromDate 
		and T1."TaxDate" <= :ToDate 
		Group By T2."CardCode",YEAR(T1."TaxDate"),MONTH(T1."TaxDate") 
	;
	/*Returned*/
	TBL4 =
		select
			YEAR(T1."TaxDate") as "YEARPARAM",
			MONTH(T1."TaxDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			CAST (Sum(T1."DocTotal" - T1."VatSum" - T1."TotalExpns") AS DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From ORDN T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" 
		Where T1."TaxDate" >= :FromDate 
		and T1."TaxDate" <= :ToDate 
		Group By T2."CardCode",YEAR(T1."TaxDate"),MONTH(T1."TaxDate") 
	;
	/*Invoiced*/
	TBL51 =
		select
			YEAR(T1."TaxDate") as "YEARPARAM",
			MONTH(T1."TaxDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			CAST (Sum(T3."Debit" - T3."Credit") AS DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From OINV T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" Join "JDT1" T3 on T1."TransId" = T3."TransId" 
		and T3."ShortName" = T2."CardCode" 
		Where T1."TaxDate" >= :FromDate 
		and T1."TaxDate" <= :ToDate 
		Group By T2."CardCode",YEAR(T1."TaxDate"),MONTH(T1."TaxDate") 
	;
	TBL52 =
		select
			YEAR(T1."TaxDate") as "YEARPARAM",
			MONTH(T1."TaxDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			CAST (Sum(T3."Debit" - T3."Credit") AS DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From OCSI T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" Join "JDT1" T3 on T1."TransId" = T3."TransId" 
		and T3."ShortName" = T2."CardCode" 
		Where T1."TaxDate" >= :FromDate 
		and T1."TaxDate" <= :ToDate 
		Group By T2."CardCode",YEAR(T1."TaxDate"),MONTH(T1."TaxDate")
	;
	TBL53 =
		select
			YEAR(T1."TaxDate") as "YEARPARAM",
			MONTH(T1."TaxDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			CAST (Sum(T3."Debit" - T3."Credit") AS DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE"
		From OCSV T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" Join "JDT1" T3 on T1."TransId" = T3."TransId" 
		and T3."ShortName" = T2."CardCode" 
		Where T1."TaxDate" >= :FromDate 
		and T1."TaxDate" <= :ToDate 
		Group By T2."CardCode",YEAR(T1."TaxDate"),MONTH(T1."TaxDate") 
	;
	/*Credited*/
	TBL6 =
		select
			YEAR(T1."TaxDate") as "YEARPARAM",
			MONTH(T1."TaxDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			CAST (Sum(T3."Debit" - T3."Credit") AS DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE" 
		From ORIN T1 Join CRSP_TEMP_OCRD T2 On T1."CardCode" = T2."CardCode" Join "JDT1" T3 on T1."TransId" = T3."TransId" 
		and T3."ShortName" = T2."CardCode" 
		Where T1."TaxDate" >= :FromDate 
		and T1."TaxDate" <= :ToDate 
		Group By T2."CardCode",YEAR(T1."TaxDate"),MONTH(T1."TaxDate") 
	;
	/*Collected*/
	TBL7 =
		select
			YEAR(T1."TaxDate") as "YEARPARAM",
			MONTH(T1."TaxDate") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			CAST (Sum(T4."DocTotal") AS DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE" 
		From JDT1 T1 Join CRSP_TEMP_OCRD T2 On T1."ShortName" = T2."CardCode" Join "OJDT" T3 On T1."TransId" = T3."TransId" 
		And T1."TransType" = 24 
		And T3."StornoToTr" IS NULL Join "ORCT" T4 On T3."CreatedBy" = T4."DocEntry" 
		Where T1."TaxDate" >= :FromDate 
		and T1."TaxDate" <= :ToDate 
		Group By T2."CardCode",YEAR(T1."TaxDate"),MONTH(T1."TaxDate") 
	;
	/*OpenBalance*/
	TBL8 =
		Select
			YEAR(T0."Date") as "YEARPARAM",
			MONTH(T0."Date") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			CAST ((Max(T2."Balance") - (Sum(T1."Debit") - Sum(T1."Credit"))) AS DECIMAL(21,6)) as "OPENBALANCE",
			cast (null as DECIMAL(21,6)) as "CLOSEBALANCE" 
		From JDT1 T1 Join CRSP_TEMP_OCRD T2 on T1."ShortName" = T2."CardCode"
		Join 
		(
			select To_Date(:FromDate) as "Date" from DUMMY
			union
			select "DATE" from #MONTHBEGIN
		) T0
		On T1."TaxDate" >= T0."Date" 
		Group By T2."CardCode",YEAR(T0."Date"),MONTH(T0."Date")
	;
	/*CloseBalance*/
	TBL9 =
		Select
			YEAR(T0."Date") as "YEARPARAM",
			MONTH(T0."Date") as "MONTHPARAM",
			T2."CardCode" as "CARDCODE",
			Max(T2."CardName") as "CARDNAME",
			Max(T2."GroupCode") as "GROUPCODE",
			cast (null as DECIMAL(21,6)) as "RDRTOTAL",
			cast (null as DECIMAL(21,6)) as "GROSSBASE",
			cast (null as DECIMAL(21,6)) as "GROSSPROFIT",
			cast (null as DECIMAL(21,6)) as "DLNTOTAL",
			cast (null as DECIMAL(21,6)) as "RDNTOTAL",
			cast (null as DECIMAL(21,6)) as "INVTOTAL",
			cast (null as DECIMAL(21,6)) as "RINTOTAL",
			cast (null as DECIMAL(21,6)) as "RCTTOTAL",
			cast (null as DECIMAL(21,6)) as "OPENBALANCE",
			CAST((Max(T2."Balance") - (Sum(T1."Debit") - Sum(T1."Credit"))) AS DECIMAL(21,6)) as "CLOSEBALANCE" 
		From JDT1 T1 Join CRSP_TEMP_OCRD T2 on T1."ShortName" = T2."CardCode"
		Join 
		(
			select ADD_DAYS("DATE",-1) as "Date" from #MONTHBEGIN
			where  ADD_DAYS("DATE",-1) >= To_Date(:FromDate)
			union
			select To_Date(:ToDate) as "Date" from DUMMY
		) T0
		On T1."TaxDate" > T0."Date" 
		Group By T2."CardCode",YEAR(T0."Date"),MONTH(T0."Date")
	;

	TBL10 = SELECT * FROM :TBL1
			UNION ALL
			SELECT * FROM :TBL2
			UNION ALL
			SELECT * FROM :TBL3
			UNION ALL
			SELECT * FROM :TBL4
			UNION ALL
			SELECT * FROM :TBL51
			UNION ALL
			SELECT * FROM :TBL52
			UNION ALL
			SELECT * FROM :TBL53
			UNION ALL
			SELECT * FROM :TBL6
			UNION ALL			
			SELECT * FROM :TBL7
			UNION ALL
			SELECT * FROM :TBL8
			UNION ALL
			SELECT * FROM :TBL9
			;
			
	/*TblResult*/
	TBL11 =
		select
			"YEARPARAM",
			"MONTHPARAM",
			"CARDCODE",
			Max("CARDNAME")as "CARDNAME",
			Max("GROUPCODE") as "GROUPCODE",
			Sum("RDRTOTAL") as "RDRTOTAL",
			Sum("GROSSBASE") as "GROSSBASE",
			Sum("GROSSPROFIT") as "GROSSPROFIT",
			Sum("DLNTOTAL") as "DLNTOTAL",
			Sum("RDNTOTAL") as "RDNTOTAL",
			Sum("INVTOTAL") as "INVTOTAL",
			Sum("RINTOTAL") as "RINTOTAL",
			Sum("RCTTOTAL") as "RCTTOTAL",
			Sum("OPENBALANCE") as "OPENBALANCE",
			Sum("CLOSEBALANCE") as "CLOSEBALANCE" 
		from :TBL10
		Group By "CARDCODE", "YEARPARAM", "MONTHPARAM" 
	;
	select
		T0."YEARPARAM",
		T0."MONTHPARAM",
		T0."CARDCODE",
		T0."CARDNAME",
		T0."GROUPCODE",
		T0."RDRTOTAL",
		T0."GROSSBASE",
		T0."GROSSPROFIT",
		T0."DLNTOTAL",
		T0."RDNTOTAL",
		T0."INVTOTAL",
		T0."RINTOTAL",
		T0."RCTTOTAL",
		Case When "OPENBALANCE" is null
			 And  T0."YEARPARAM" >= Year(To_Date(:FromDate))
			 And  T0."MONTHPARAM" >= Month(To_Date(:FromDate))
			 Then T1."Balance"
			 Else "OPENBALANCE"
		End as "OPENBALANCE",
		Case When "CLOSEBALANCE" is null 
			 And  T0."YEARPARAM" >= Year(To_Date(:FromDate))
			 And  T0."MONTHPARAM" >= Month(To_Date(:FromDate))
			 Then T1."Balance" 
			 Else "CLOSEBALANCE" 
		End as "CLOSEBALANCE" 
	from :TBL11 T0 
	Join CRSP_TEMP_OCRD T1
	On T0."CARDCODE" = T1."CardCode" 
	Order By T0."CARDCODE" Asc, T0."YEARPARAM" Desc, T0."MONTHPARAM" Desc
	;
	end if;
END











