-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

CREATE PROCEDURE CRSP_Monthly_Customer_Status_Report
(
in DateTypeIn nvarchar(1),
in FromDateIn datetime,
in ToDateIn datetime,
in CustomerIdsIn nclob
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
execSQL 		nclob;
DateType 		nvarchar(1);
FromDate 		datetime;
ToDate 			datetime;
targetField		VarChar(50);	--- The total field in the global table (CRSP_Monthly_Customer_Status_Report_BPMonthly)
sourceField		VarChar(200);	--- The total field from the source tables (RDR, DLN, INV, ...)
dateField		varChar(12);	--- Date field from source tables
date_parts		varchar(200);
dateInSpan		varchar(200);
newLine			char(2);
tblName			Char(4);
_insert			varchar(200);
_select			varchar(200);
_from			Char(6);
_where			varchar(1000);
parameters		nvarchar(100);
_lastMonthBegin DateTime;
_lastMonthEnd	DateTime;
GrossBySal		nvarchar(1);
_DateVar		Date;

BEGIN
--For HANA security issue call procedure "_TmSp_ValidateSpParam"
    call _TmSp_ValidateSpParam(:DateTypeIn);    
	call _TmSp_ValidateSpParam(:CustomerIdsIn);  
DateType := :DateTypeIn;
FromDate := :FromDateIn;
ToDate := :ToDateIn;

delete from "CRSP_Monthly_Customer_Status_Report_Customers";
delete from "CRSP_Monthly_Customer_Status_Report_BPMonthly";
delete from "CRSP_Monthly_Customer_Status_Report_MonthBegin";
delete from "CRSP_Monthly_Customer_Status_Report_TblResult";

execSQL:= '
Insert Into "CRSP_Monthly_Customer_Status_Report_Customers"
	Select "CardCode","CardName","GroupCode","Balance" From OCRD where "CardCode" in ' || :CustomerIdsIn;
	
exec (:execSQL);

IF :DateType = N'P' THEN
	dateField := 'T1."DocDate"';
ELSE
	dateField := 'T1."TaxDate"';
END IF;

 -- Get the month before the from date for sales order growth calculation
_lastMonthBegin := ADD_MONTHS(ADD_DAYS(:FromDate, 1 - dayofmonth(:FromDate)), -1); 
_lastMonthEnd := ADD_DAYS(:FromDate, 1 - dayofmonth(:FromDate) - 1); 
	
date_parts := 'Year(' || :dateField || ')' || ',Month(' || :dateField || ')';
dateInSpan := ' ''' || :FromDate || ''' <= ' || :dateField || ' and ' || :dateField || ' <= ''' || ToDate || ''' ';
newLine := char(9) || char(10);

_insert := 'INSERT INTO "CRSP_Monthly_Customer_Status_Report_BPMonthly" (YearParam, MonthParam, CardCode, CardName, GroupCode, ';
_select := ') Select ' || :date_parts || ', T2.CardCode, Max(T2.CardName), Max(T2.GroupCode), ';
_from := ' From ';

-- Ordered
_where := ' T1 Join "CRSP_Monthly_Customer_Status_Report_Customers" T2 On T1."CardCode" = T2.CardCode
Where T1."CANCELED" = ''N'' and ('|| :dateInSpan || ' Or ''' || :_lastMonthBegin || ''' <= ' || :dateField || ' and ' || :dateField || ' <= ''' || :_lastMonthEnd || ''')
Group By T2.CardCode,' || :date_parts;

tblName := 'ORDR';
targetField := 'RDRTotal, GrossProfit';
sourceField := 'Sum(T1."DocTotal" - T1."VatSum" - T1."TotalExpns" + T1."WTSum" - T1."RoundDif"), Sum(T1."GrosProfit")';
execSQL := :_insert || :targetField || :_select || :sourceField || :newLine || :_from || :tblName || :_where;

exec(:execSQL);

 -- Order's Gross Base Total 
targetField := 'GrossBase';
select "GrossBySal" into GrossBySal from OADM;
IF :GrossBySal = 'Y' THEN
	sourceField := 'Sum(T3."LineTotal") - Sum(T1."DiscSum")';
ELSE
	sourceField := 'Sum(T3."GPTtlBasPr")';
END IF;

_where := ' T1 Join "CRSP_Monthly_Customer_Status_Report_Customers" T2 On T1."CardCode" = T2.CardCode
Join RDR1 T3 on T1."DocEntry" = T3."DocEntry"
Where T1."CANCELED" = ''N'' and ('|| :dateInSpan || ' Or ''' || :_lastMonthBegin || ''' <= ' || :dateField || ' and ' || :dateField || ' <= ''' || :_lastMonthEnd || ''')
Group By T2.CardCode,' || :date_parts;
execSQL := :_insert || :targetField || :_select || :sourceField || :newLine || :_from || :tblName || :_where;

exec(:execSQL);


----------- Set the SQL components for Documents
_where := ' T1 Join "CRSP_Monthly_Customer_Status_Report_Customers" T2 On T1."CardCode" = T2.CardCode
Where ' || :dateInSpan || ' Group By T2.CardCode,' || :date_parts;

-- Delivered
tblName := 'ODLN';
targetField := 'DLNTotal';
sourceField := 'Sum(T1."DocTotal" - T1."VatSum" - T1."TotalExpns")';
execSQL := :_insert || :targetField || :_select || :sourceField || :newLine || :_from || :tblName || :_where;

exec(:execSQL);


-- Returned
tblName := 'ORDN';
targetField := 'RDNTotal';
execSQL := :_insert || :targetField || :_select || :sourceField || :newLine || :_from || :tblName || :_where;

exec(:execSQL);


-- Invoiced
_where := ' T1 Join "CRSP_Monthly_Customer_Status_Report_Customers" T2 On T1."CardCode" = T2.CardCode
Join JDT1 T3 on T1."TransId" = T3."TransId" and T3."ShortName" = T2.CardCode
Where ' || :dateInSpan || ' Group By T2.CardCode,' || :date_parts;

tblName := 'OINV';
targetField := 'INVTotal';
sourceField := 'Sum(T3."Debit" - T3."Credit")';
execSQL := :_insert || :targetField || :_select || :sourceField || :newLine || :_from || :tblName || :_where;

exec(:execSQL);


tblName := 'OCSI';
execSQL := :_insert || :targetField || :_select || :sourceField || :newLine || :_from || :tblName || :_where;

exec(:execSQL);

tblName := 'OCSV';
execSQL := :_insert || :targetField || :_select || :sourceField || :newLine || :_from || :tblName || :_where;

exec(:execSQL);


-- Credited
tblName := 'ORIN';
targetField := 'RINTotal';
execSQL := :_insert || :targetField || :_select || :sourceField || :newLine || :_from || :tblName || :_where;

exec(:execSQL);


-- Collected
tblName := 'JDT1';

IF :DateType = N'P' THEN
	dateField := 'T1."RefDate"';
ELSE
	dateField := 'T1."TaxDate"';
END IF;
 
date_parts := 'Year(' || :dateField || ')' || ',Month(' || :dateField || ')';
dateInSpan := '''' || :FromDate || ''' <= ' || :dateField || ' and ' || :dateField || ' <= ''' || :ToDate || '''';
_select := ')
Select ' || :date_parts || ', T2.CardCode, Max(T2.CardName), Max(T2.GroupCode), ';

targetField := 'RCTTotal';
SourceField := 'Sum(T4."DocTotal")';
_where := ' T1 Join "CRSP_Monthly_Customer_Status_Report_Customers" T2 On T1."ShortName" = T2.CardCode
Join OJDT T3 On T1."TransId" = T3."TransId" And T1."TransType" = 24 And T3."StornoToTr" IS NULL
Join ORCT T4 On T3."CreatedBy" = T4."DocEntry"
Where ' || :dateInSpan || ' Group By T2.CardCode,' || :date_parts;
execSQL := :_insert || :targetField || :_select || :sourceField || :newLine || :_from || :tblName || :_where;

exec(:execSQL);


---------------------------------------------------------
-- Create the table of Month Beginings between the FromDate and ToDate
_DateVar := ADD_MONTHS(ADD_DAYS(:FromDate, 1 - dayofmonth(:FromDate)), 1); --DATEADD(M, DateDiff(M, 0, @_FromDate) + 1, 0)

WHILE :_DateVar <= :ToDate DO
	INSERT INTO "CRSP_Monthly_Customer_Status_Report_MonthBegin" Select :_DateVar from DUMMY;
	_DateVar := ADD_MONTHS(:_Datevar, 1); 
END WHILE;


IF :DateType = 'P' THEN
	INSERT INTO "CRSP_Monthly_Customer_Status_Report_BPMonthly" (YearParam, MonthParam, CardCode, CardName, GroupCode, OpenBalance)
	Select Year(T0.Date), Month(T0.Date), T2.CardCode, Max(T2.CardName), Max(T2.GroupCode), Max(T2.Balance) - Sum(T1."Debit"- T1."Credit")
	From JDT1 T1 Join "CRSP_Monthly_Customer_Status_Report_Customers" T2 on T1."ShortName" = T2.CardCode --COLLATE database_default
	Join ( Select :FromDate as Date from DUMMY
	Union
	Select MonthBegin from "CRSP_Monthly_Customer_Status_Report_MonthBegin"
	) as T0 
	on T1."RefDate" >= T0.Date 
	Group By T2.CardCode, Year(T0.Date), Month(T0.Date);
ELSE
	INSERT INTO "CRSP_Monthly_Customer_Status_Report_BPMonthly" (YearParam, MonthParam, CardCode, CardName, GroupCode, OpenBalance)
	Select Year(T0.Date), Month(T0.Date), T2.CardCode, Max(T2.CardName), Max(T2.GroupCode), Max(T2.Balance) - Sum(T1."Debit"- T1."Credit") 
	From JDT1 T1 Join "CRSP_Monthly_Customer_Status_Report_Customers" T2 on T1."ShortName" = T2.CardCode --COLLATE database_default
	Join ( Select :FromDate as Date from DUMMY
	 Union
	Select MonthBegin from "CRSP_Monthly_Customer_Status_Report_MonthBegin" 
	) as T0 
	on T1."TaxDate" >= T0.Date 
	Group By T2.CardCode, Year(T0.Date), Month(T0.Date);
END IF;

-- Close Balance 
If :DateType = 'P' THEN
	INSERT INTO "CRSP_Monthly_Customer_Status_Report_BPMonthly" (YearParam, MonthParam, CardCode, CardName, GroupCode, CloseBalance)
	Select Year(T0.Date), Month(T0.Date), T2.CardCode, Max(T2.CardName), Max(T2.GroupCode), Max(T2.Balance) - Sum(T1."Debit"- T1."Credit") 
	From JDT1 T1 Join "CRSP_Monthly_Customer_Status_Report_Customers" T2 on T1."ShortName" = T2.CardCode --COLLATE database_default
	Join ( Select ADD_DAYS(MonthBegin, -1) as Date from "CRSP_Monthly_Customer_Status_Report_MonthBegin"
	Where ADD_DAYS(MonthBegin, -1) >= :FromDate
	Union 
	Select :ToDate from DUMMY) as T0 
	on T1."RefDate" > T0.Date 
	Group By T2.CardCode, Year(T0.Date), Month(T0.Date);
ELSE
	INSERT INTO "CRSP_Monthly_Customer_Status_Report_BPMonthly" (YearParam, MonthParam, CardCode, CardName, GroupCode, CloseBalance)
	Select Year(T0.Date), Month(T0.Date), T2.CardCode, Max(T2.CardName), Max(T2.GroupCode), Max(T2.Balance) - Sum(T1."Debit" - T1."Credit") 
	From JDT1 T1 Join "CRSP_Monthly_Customer_Status_Report_Customers" T2 on T1."ShortName" = T2.CardCode --COLLATE database_default
	Join ( Select ADD_DAYS(MonthBegin, -1) as Date from "CRSP_Monthly_Customer_Status_Report_MonthBegin" 
	Where ADD_DAYS(MonthBegin, -1) >= :FromDate
	Union 
	Select :ToDate from DUMMY) as T0 
	on T1."TaxDate" > T0.Date 
	Group By T2.CardCode, Year(T0.Date), Month(T0.Date);
END IF;


-----------------------------------------------------------------------------------------------------------------

Insert Into "CRSP_Monthly_Customer_Status_Report_TblResult"
select YearParam, MonthParam, CardCode, Max(CardName)as CardName, Max(GroupCode) as GroupCode, 
	Sum(RDRTotal) as RDRTotal, Sum(GrossBase) as GrossBase, Sum(GrossProfit) as GrossProfit, 
	Sum(DLNTotal) as DLNTotal, Sum(RDNTotal) as RDNTotal, 
	Sum(INVTotal) as INVTotal, Sum(RINTotal) as RINTotal, Sum(RCTTotal) as RCTTotal,
	Sum(OpenBalance) as OpenBalance, Sum(CloseBalance) as CloseBalance
from "CRSP_Monthly_Customer_Status_Report_BPMonthly" 
Group By CardCode, YearParam, MonthParam 
Order By CardCode Asc, YearParam Desc, MonthParam Desc;

-- Udpate the OpenBalance and CloseBalance if they are NULL. 
-- NOTE: The month before the from date is not necessary to update.
Update "CRSP_Monthly_Customer_Status_Report_TblResult" T0
Set T0.OpenBalance = (Select  T1.Balance From "CRSP_Monthly_Customer_Status_Report_Customers" T1 
Where T1.CardCode = T0.CardCode )
WHERE T0.OpenBalance Is NULL And T0.YearParam >= YEAR(:FromDate) And T0.MonthParam >= Month(:FromDate);



Update "CRSP_Monthly_Customer_Status_Report_TblResult" T0
Set CloseBalance = (Select T1.Balance From "CRSP_Monthly_Customer_Status_Report_Customers" T1
Where T0.CardCode = T1.CardCode )
WHERE T0.CloseBalance Is NULL And YearParam >= YEAR(:FromDate) And MonthParam >= Month(:FromDate);

Select * From "CRSP_Monthly_Customer_Status_Report_TblResult";

delete from "CRSP_Monthly_Customer_Status_Report_Customers";
delete from "CRSP_Monthly_Customer_Status_Report_BPMonthly";
delete from "CRSP_Monthly_Customer_Status_Report_MonthBegin";
delete from "CRSP_Monthly_Customer_Status_Report_TblResult";
END;











