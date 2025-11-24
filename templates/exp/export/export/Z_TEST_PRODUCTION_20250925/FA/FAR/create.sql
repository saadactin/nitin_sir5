CREATE Procedure FAR 
(IN Fromdate timestamp
,IN ToDate timestamp)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
 Begin
with AP_INV as (
select PH1."ItemCode",Min(PH1."DocEntry") "DocEntry",Ph1."OcrCode2"
from OPCH PCH
INNER JOIN PCH1 PH1 on PCH."DocEntry"=PH1."DocEntry"
INNER JOIN OITM ITM ON PH1."ItemCode"=ITM."ItemCode"
WHERE ITM."ItemType" ='F' and PCH."CANCELED"='N'
group by PH1."ItemCode",Ph1."OcrCode2"
),
Capitl as (
select AQ1."ItemCode",jt1."ContraAct",MIN(ACQ."DocEntry") "DocEntry",ACQ."DocNum",ACQ."DocDate"
from JDT1 JT1
inner join OACQ ACQ on Jt1."TransId"=ACQ."TransId"
Inner JOIN ACQ1 AQ1 on ACQ."DocEntry"=AQ1."DocEntry"
group by AQ1."ItemCode",jt1."ContraAct",ACQ."DocNum",ACQ."DocDate"
)


select distinct ITM."ItemCode" "Asset Code",LEFT(ITM."ItemCode",2) "Asset Category"
,SUBSTRING(ITM."ItemCode",3,2) "Asset Sub-Category"
,act."AcctName" "Description"
,ACT."AcctCode" "GL no"
,PCH."NumAtCard" "A/P Invoice Transaction Ref"
,cap."DocNum" "Capitalization Ref Transaction Ref"
,PCH."DocDate" "Transaction Date"
,PCH."DocNum" "Invoice number"
,PCH."TaxDate" "Invoice Date"
,PCH."CardName" "Vendor Name"
,LCT."Location" "Location"
,AP."OcrCode2" "BU"
,PH1."OcrCode3" "Sub-BU"
,Ph1."Quantity"
,Ph1."Price" "Rate"
,(case when IFNULL(cast(ITM."U_Actual_Purchase_Amount" as decimal),0)=0 
	then APC."APC" 
	else cast(ITM."U_Actual_Purchase_Amount" as decimal) end)  "Gross Value- at the beginning of the year "
,PH1."LineTotal" "Additions during the year"
,(Case When RI."cnt">0 then 
					(Case when IFNULL(cast(ITM."U_Actual_Purchase_Amount" as decimal),0)=0
					then APC."APC" 
					else cast(ITM."U_Actual_Purchase_Amount" as decimal)
					end)
	end) "Retirement"
,(case when IFNULL(cast(ITM."U_Actual_Purchase_Amount" as decimal),0)=0 
	then APC."APC" 
	else cast(ITM."U_Actual_Purchase_Amount" as decimal) end)
+PH1."LineTotal"
-(Case When RI."cnt">0 then 
					(Case when IFNULL(cast(ITM."U_Actual_Purchase_Amount" as decimal),0)=0
					then APC."APC" 
					else cast(ITM."U_Actual_Purchase_Amount" as decimal)
					end)
	end)  "Total Assets"
,(select Distinct "SalvageVal" from FIX1 where "ItemCode"=ITM."ItemCode" and "TransType"=540) "Salvage Value"
,cap."DocDate" "Capitalisation date"
,IM7."UsefulLife" "Useful Life in months"
,IFNULL(IM7."UsefulLife",0)-IFNULL(IM7."RemainLife",0) "Consumed useful life"
,IM7."RemainLife" "Balance useful life"
,IFNULL(ITM."U_Asset_Dep_Rate",0) "Asset_Dep_Rate"
,T1."Descr" "DprType"
,(select Sum("OrdDprPlan") from ODPV where "ItemCode"=ITM."ItemCode") "Opening Accumulated Depreciation"
,DPR."Depreciation" "Depreciation For current period"
,(select Sum("OrdDprPlan") from ODPV where "ItemCode"=ITM."ItemCode") "depreciation for retired assets"
,IFNULL((select Sum("OrdDprPlan") from ODPV where "ItemCode"=ITM."ItemCode"),0)+IFNULL(DPR."Depreciation",0)
-IFNULL((select Sum("OrdDprPlan") from ODPV where "ItemCode"=ITM."ItemCode"),0) "Total Accumulated Depreciation"
,(IFNULL((case when IFNULL(cast(ITM."U_Actual_Purchase_Amount" as decimal),0)=0 
	then APC."APC" 
	else cast(ITM."U_Actual_Purchase_Amount" as decimal) end),0)
+IFNULL(PH1."LineTotal",0)
-IFNULL((Case When RI."cnt">0 then 
					(Case when IFNULL(cast(ITM."U_Actual_Purchase_Amount" as decimal),0)=0
					then APC."APC" 
					else cast(ITM."U_Actual_Purchase_Amount" as decimal)
					end)
	end),0))
	-
	IFNULL((select Sum("OrdDprPlan") from ODPV where "ItemCode"=ITM."ItemCode"),0) "Closing WDV"
FROM OITM ITM
inner join OACT ACT on Itm."AssetClass"=ACT."AcctName"
inner join jdt1 jt1 on ACT."AcctCode"=jt1."ContraAct"
LEFT OUTER JOIN OLCT LCT on ITM."Location"=LCT."Code"
LEFT outer join AP_INV AP on AP."ItemCode"=ITM."ItemCode"
left outer join OPCH PCH on AP."DocEntry"=PCH."DocEntry"
left outer join (Select "DocEntry","ItemCode","OcrCode2","OcrCode3","Quantity","Price","LineTotal" from PCH1) PH1 
				on PCH."DocEntry"=PH1."DocEntry" and PH1."ItemCode"=ITM."ItemCode"
left outer join Capitl cap on cap."ItemCode"=Itm."ItemCode" --and ACT."AcctName"=cap."ContraAct"
left outer join ITM7 IM7 on IM7."ItemCode"=ITM."ItemCode"
left outer join ODTP T1 ON IM7."DprType" = T1."Code"
Left outer join (select COunt(*) "cnt",RI1."ItemCode" from ORTI RTI
				INNER JOIN RTI1 RI1 on RTI."DocEntry"=RI1."DocEntry"
			   where RTI."DocStatus"<>'C' group by RI1."ItemCode") RI on RI."ItemCode"=ITM."ItemCode"
LEFt outer join (select "APC","ItemCode" from FIX1 where "TransType"=110) APC on APC."ItemCode"=ITM."ItemCode"
Left outer join (select Sum("OrdDprPlan")"Depreciation","ItemCode" from ODPV 
				where "FromDate">=:FromDate and "ToDate"<=:ToDate
				group by "ItemCode") Dpr on Dpr."ItemCode"=Itm."ItemCode"
WHERE ITM."ItemType" ='F'; --and ITM."ItemCode"='COLT.0101'
end



