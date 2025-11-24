-- B1 DEPENDS: AFTER:PT:PROCESS_END

create procedure CRSP_Annual_Sales_Analysis_Quarter_Insert
(
	in ObjectType 		nChar(3),
	in ShowType 		nChar(3),
	in VatSysColName 	nvarchar(20),
	in DspFrznBP		nvarchar(1),
	in DocDateFrom 		datetime,
	in DocDateTo 		datetime,
	in DocDueDateFrom 	datetime,
	in DocDueDateTo 	datetime,
	in TaxDateFrom 		datetime,
	in TaxDateTo 		datetime,
	in yearv 			int	
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
as
	tmpStr0 	nvarchar(4000);
	tmpStr 		nvarchar(4000);
	WhereStr 	nvarchar(4000);
	Mult 		nvarchar(5);
	Header 		nvarchar(4);
	Lines  		nvarchar(4);
	InstLines  	nvarchar(4);
	opObject 	nvarchar(4);
	WTLines		nvarchar(4);
	FormatStr 	nvarchar(200);
begin

delete from "CRSPAnnualSalesAnalysisQuarterInsert_TMPMonth";

if(:ObjectType <> 'NON') then
	if (:ShowType = 'SNG' or :ShowType = 'SN1') then
		Mult := ' 1 ';
	else
		Mult :=  ' -1 ';
	end if;
	if (:ObjectType = 'INV')then
		Header := 'OINV';
		Lines :=  'INV1';
		InstLines := 'INV6';
		opObject := 'RIN';
		WTLines := 'INV5';
	end if;
	if (:ObjectType = 'CSI')then
		Header := 'OCSI';
		Lines :=  'CSI1';
		InstLines := 'CSI6';
  		WTLines := 'CSI5';
	end if;
	if (:ObjectType = 'CSV')then
		Header := 'OCSV';
		Lines :=  'CSV1';
		InstLines := 'CSV6';
  		WTLines := 'CSV5';
	end if;
	if (:ObjectType = 'RDR')then
		Header := 'ORDR';
		Lines :=  'RDR1';
		InstLines := 'RDR6';
		opObject := 'NON';
		WTLines := 'RDR5';
	end if;
	if (:ObjectType = 'DLN')then
		Header := 'ODLN';
		Lines :=  'DLN1';
		InstLines := 'DLN6';
		opObject := 'RDN';
		WTLines := 'DLN5';
	end if;
	if (:ObjectType = 'RIN')then
		Header := 'ORIN';
		Lines :=  'RIN1';
		InstLines := 'RIN6';
		WTLines := 'RIN5';
	end if;
	if (:ObjectType = 'RDN')then
		Header := 'ORDN';
		Lines :=  'RDN1';
		InstLines := 'RDN6';
		WTLines := 'RDN5';
	end if;
	
	SELECT TOP 1 COLUMN_NAME into VatSysColName FROM "PUBLIC"."TABLE_COLUMNS" as A WHERE TABLE_NAME = :Header and POSITION = 57;
	
		
	WhereStr := 'Main."DocDate" >= '''|| :DocDateFrom || '''  And  Main."DocDate" <='''|| :DocDateTo || 
		''' And Inst."DueDate" >= '''|| :DocDueDateFrom ||
		''' And Inst."DueDate" <= ''' || :DocDueDateTo || ''' And Main."TaxDate" >= '''|| :TaxDateFrom ||
		''' And Main."TaxDate" <= '''|| :TaxDateTo || ''' And Main."Instance" = 0 ';
		
	if (:ObjectType <> 'CSI') then
		WhereStr := :WhereStr || 'And	   Main."CANCELED" = ''N'' ';
	end if;
	if (:ObjectType = 'RIN') then
		WhereStr := :WhereStr || '  AND NOT  EXISTS (SELECT 1 FROM '|| :ObjectType || '1 T1 WHERE T1."DocEntry" = Main."DocEntry"  AND T1."BaseType" = 203) '; 
	end if;
	
	tmpStr0 := 'insert into "CRSPAnnualSalesAnalysisQuarterInsert_TMPMonth" select Main."DocEntry", floor((MONTH(Main."DocDate")+2)/3) from ' || :Header ||
			' Main ';--inner join '|| :InstLines ||' Inst On inst."DocEntry" = Main."DocEntry"  where ' || :WhereStr;
			
	exec(:tmpStr0);
		
	tmpStr0 := 'insert into  "CRSPAnnualSalesAnalysisQuarter_TmpPsar" Select Crdg."GroupCode", Crdg."GroupName", TX."Quarter" As Quarter, ';
	tmpStr := ' As Year, (Sum(Inst."InsTotal") - Sum(Inst."VatSum") - Sum(Inst."TotalExpns") 
				+ (CASE   (SELECT MIN(Wt."Category")   FROM ' || :WTLines || ' Wt where Wt."AbsEntry" = Main."DocEntry")
	         	WHEN ''I'' THEN Sum(Inst."WTSum") ELSE 0 END)
				 + Sum(Main."DpmAmnt" * floor(Inst."InstPrcnt" / 100)) - Sum(Main."RoundDif" * floor(Inst."InstPrcnt" / 100)) )  * ' || :Mult || ' As Sales, Sum(Main."GrosProfit" * Inst."InstPrcnt" / 100)  * ' 
				|| :Mult || ' As GrossProfit, (Sum(Inst."InsTotalSy") - Sum(Inst."' || :VatSysColName || '") - Sum(Inst."TotalExpSC") +
				(CASE (SELECT MIN(Wt."Category")   FROM ' || :WTLines || ' Wt where Wt."AbsEntry" = Main."DocEntry")
	         	WHEN ''I''  THEN Sum(Inst."WTSumSC") ELSE 0 END)
	         	   + Sum(Main."DpmAmntSC" * Inst."InstPrcnt" / 100) -  Sum(Main."RoundDifSy" * Inst."InstPrcnt" / 100) )  * ' || :Mult || '  As SalesSys, (Sum(Main."GrosProfSy" * Inst."InstPrcnt" / 100))  * ' 
	  			|| :Mult || '  As GrossProfitSys,';
	
	tmpStr := :tmpStr || ' (Select  (Sum(aa."DocTotal") - Sum(aa."VatSum") - Sum(aa."TotalExpns") + 
			(CASE 
				(SELECT MIN(Wt."Category") FROM ' || :WTLines || ' Wt where Wt."AbsEntry" = Main."DocEntry")
			         	WHEN ''I'' THEN 
			         		Sum(Inst."WTSum") 
			         	ELSE 
							0
	  		END) + Sum(aa."DpmAmnt") - Sum(aa."RoundDif")
			) From ' || :Header || ' aa   Where  aa."DocEntry" = Main."DocEntry" )  * ' || :Mult || '  As  SalesForPrcnt  , 0 As  SalesForPrcntSys ';

	tmpStr := :tmpStr || ' From  ' || :Header || ' Main 
	Inner  Join OCRD Cards On Cards."CardCode" = Main."CardCode"
	Inner  Join OCRG Crdg On Crdg."GroupCode" = Cards."GroupCode"
	Inner join "CRSPAnnualSalesAnalysisQuarterInsert_TMPMonth" TX on TX."DocEntry" = Main."DocEntry"
	Inner  Join ' || :InstLines || ' Inst On inst."DocEntry" = Main."DocEntry" ';
	
	if (:DspFrznBP = 'N') then
		WhereStr := '(' || :WhereStr || ') And ' || 
					'(Cards."validFor" = ''Y'' or (Cards."frozenFor" = ''Y'' and (Cards."frozenFrom" is not null or Cards."frozenTo" is not null)) or (Cards."validFor" = ''N'' and Cards."frozenFor" = ''N''))';
	end if;
	
	WhereStr := ' Where ' || :WhereStr || '  Group By  Crdg."GroupCode", Crdg."GroupName",  TX."Quarter",   Year(Main."DocDate"), Main."DocEntry"
  				Order By Crdg."GroupCode" , Year(Main."DocDate")';
  				
  	tmpStr := :tmpStr || :WhereStr;
  	
	exec(:tmpStr0 || :yearv || :tmpStr);
  	delete from "CRSPAnnualSalesAnalysisQuarterInsert_TMPMonth";
  	
	/* End Here */
end if;
end;











