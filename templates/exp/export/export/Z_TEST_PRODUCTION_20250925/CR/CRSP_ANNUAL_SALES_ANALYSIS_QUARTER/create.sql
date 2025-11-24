-- B1 DEPENDS: AFTER:SP:CRSP_Annual_Sales_Analysis_Quarter_Insert AFTER:PT:PROCESS_END

create procedure CRSP_Annual_Sales_Analysis_Quarter (
	in DocDateFrom_in nvarchar(4),
	in DocumentType_in nvarchar(3)
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
as
	ObjectType 		nvarchar(3);
	ShowType 		nvarchar(3);
	DocDateFrom 		datetime;
	DocDateTo		datetime;
	DocDueDateFrom 	datetime;
	DocDueDateTo		datetime;
	CardCodeFrom		nvarchar(30);
	CardCodeTo		nvarchar(30);
	CrdGroupCode  		nvarchar(11);
	ItemCodeFrom		nvarchar(50);
	ItemCodeTo		nvarchar(50);
	ItmGroupCode  		nvarchar(11);
	ItmProps	   	nvarchar(70);
	CrdProps   		nvarchar(70);
	SlpCodeFrom		nvarchar(32);
	SlpCodeTo		nvarchar(32);
	TaxDateFrom 		datetime;
	TaxDateTo		datetime;
	Brand			nvarchar(11);
	PrjCode		nvarchar(8);
	RptType		int; 	    
	cutbyObject		nchar(3);
	DspFrznBP		nvarchar(1);
	DspFrznITM		nvarchar(1);
	Header 	nvarchar(4);
	Lines  	nvarchar(4);
	InstLines  	nvarchar(4);
	opObject 	nvarchar(4);
	tmpStr 	nvarchar(4000);
	PropsRel 	nvarchar(10);
	ii 		int;
	jj 		int;
	GrossBySal 	nChar(1);
	WhereStr 	nvarchar(4000);
	VatSysColName 	nvarchar(20);
	Mult 		nvarchar(5);
	WTLines	nvarchar(4);
	prevDocDateFrom datetime;
	prevDocDateTo datetime;
	prevDueDateFrom datetime;
	prevDueDateTo datetime;
	prevTaxDateFrom datetime;
	prevTaxDateTo datetime;
	FormatStr nvarchar(200);
	specialIndicator int;
begin
	delete from "CRSPAnnualSalesAnalysisQuarter_TmpPsar";

	ObjectType := :DocumentType_in;
	ShowType := 'SNG';
	RptType := 0;
	
	if (:ObjectType = 'INV')then
		opObject := 'RIN';
	end if;

	if (:ObjectType = 'RDR')then
		opObject := 'NON';
	end if;
	
	if (:ObjectType = 'DLN')then
		opObject := 'RDN';
	end if;

	cutbyObject := 'NON';
	DspFrznBP := 'Y';
	DspFrznITM := 'Y';

	DocDueDateFrom := '19700101';
	DocDueDateTo := '21000101';
	TaxDateFrom := '19700101';
	TaxDateTo := '21000101';
	prevDueDateFrom := '19700101';
	prevDueDateTo := '21000101';
	prevTaxDateFrom := '19700101';
	prevTaxDateTo := '21000101';
	
	if LENGTH(:DocDateFrom_in) <> 4 then
		DocDateFrom := '19700101';
	else
		DocDateFrom := :DocDateFrom_in || '0101';
	end if;
		
	DocDateTo := ADD_DAYS(ADD_YEARS(:DocDateFrom, 1), -1);-- ADD_DAYS(dd, -1, ADD_YEARS(year,1,:DocDateFrom));
	prevDocDateFrom := ADD_YEARS(:DocDateFrom,-1);--ADD_YEARS(year,-1,:DocDateFrom);
	prevDocDateTo := ADD_YEARS(:DocDateTo,-1);--ADD_YEARS(year,-1,:DocDateTo);

	VatSysColName :='tmp';

	if(:ObjectType <> 'NON') then
		call CRSP_Annual_Sales_Analysis_Quarter_Insert(:ObjectType, :ShowType, :VatSysColName, :DspFrznBP, :DocDateFrom, :DocDateTo,
		 					 :DocDueDateFrom, :DocDueDateTo, :TaxDateFrom, :TaxDateTo, 1); 	  													
 	  	call CRSP_Annual_Sales_Analysis_Quarter_Insert(:ObjectType, :ShowType, :VatSysColName, :DspFrznBP, :prevDocDateFrom, 
 	  				:prevDocDateTo, :prevDueDateFrom, :prevDueDateTo, :prevTaxDateFrom, :prevTaxDateTo, 0);
 	  				
		if (:ShowType = 'SNG') then
  			ObjectType := :opObject;
  			ShowType := 'NST';
  			
  			if (:ObjectType <> 'NON') then
  				call CRSP_Annual_Sales_Analysis_Quarter_Insert(:ObjectType, :ShowType, :VatSysColName, :DspFrznBP, :DocDateFrom, :DocDateTo,
		 					 :DocDueDateFrom, :DocDueDateTo, :TaxDateFrom, :TaxDateTo, 1); 	  													
 	  			call CRSP_Annual_Sales_Analysis_Quarter_Insert(:ObjectType, :ShowType, :VatSysColName, :DspFrznBP, :prevDocDateFrom, 
 	  				:prevDocDateTo, :prevDueDateFrom, :prevDueDateTo, :prevTaxDateFrom, :prevTaxDateTo, 0);
  			end if;
 		end if;
	end if;
	
	--Card Code range

	/*Loop_End:*/
	tmpStr := ' Insert Into "CRSPAnnualSalesAnalysisQuarter_TmpPsar" Select "CardCode", "CardName" , "Quarter", "Year", Sum("Sales"), Sum("GrossPrft") , 0 , 0 , Sum("SalesForPrcnt") , -1 From "CRSPAnnualSalesAnalysisQuarter_TmpPsar" ' || 
				' Group By "CardCode", "CardName", "Quarter", "Year" ';
	exec (:tmpStr);
	
	Select "T0"."CardCode", "T0"."CardName", "T0"."Quarter", "T0"."Year", "T0"."Sales", "T0"."GrossPrft", "T0"."SalesForPrcnt",
		Sum(Case "T1"."Quarter" When 1 Then "T1"."Sales" ELse 0 End), Sum(Case "T1"."Quarter" When 1 Then "T1"."GrossPrft" ELse 0 End), Sum(Case "T1"."Quarter" When 1 Then "T1"."SalesForPrcnt" ELse 0 End),
		Sum(Case "T1"."Quarter" When 2 Then "T1"."Sales" ELse 0 End), Sum(Case "T1"."Quarter" When 2 Then "T1"."GrossPrft" ELse 0 End), Sum(Case "T1"."Quarter" When 2 Then "T1"."SalesForPrcnt" ELse 0 End),
		Sum(Case "T1"."Quarter" When 3 Then "T1"."Sales" ELse 0 End), Sum(Case "T1"."Quarter" When 3 Then "T1"."GrossPrft" ELse 0 End), Sum(Case "T1"."Quarter" When 3 Then "T1"."SalesForPrcnt" ELse 0 End),
		Sum(Case "T1"."Quarter" When 4 Then "T1"."Sales" ELse 0 End), Sum(Case "T1"."Quarter" When 4 Then "T1"."GrossPrft" ELse 0 End), Sum(Case "T1"."Quarter" When 4 Then "T1"."SalesForPrcnt" ELse 0 End)
		From "CRSPAnnualSalesAnalysisQuarter_TmpPsar" T0 Inner Join "CRSPAnnualSalesAnalysisQuarter_TmpPsar" T1 On "T0"."CardCode" = "T1"."CardCode" and "T0"."Year" = "T1"."Year" and "T1"."SalesForPrcntSys" = "T0"."SalesForPrcntSys"
		Where "T0"."SalesForPrcntSys" = -1
		Group By "T0"."CardCode", "T0"."CardName", "T0"."Quarter", "T0"."Year", "T0"."Sales", "T0"."GrossPrft", "T0"."SalesForPrcnt"
		Order By "T0"."CardCode", "T0"."Year", "T0"."Quarter";
	
	delete from "CRSPAnnualSalesAnalysisQuarter_TmpPsar";		
end;











