CREATE PROCEDURE FetchDataForwarranty
(
	--In FromDate Date,
	--IN ToDate Date
	
)
LANGUAGE SQLSCRIPT
AS

begin

select 
IFNULL((CASE T0."GSTTranTyp" WHEN 'GA' THEN 'Gst Tax Invoice' WHEN 'GD' THEN 'Gst Debit Memo' WHEN '' THEN 'Bill Of Supply' END),'') AS "GST Transaction Type", 
'AR Invoice' as "Document Type",
t0."DocNum" as "Document Number",
IFNULL(TO_VARCHAR(t0."DocDate",'DD/MM/YYYY'),'') as "Document Date" ,
--t0."Series",
IFNULL(N1."SeriesName",'')   as "Series" ,
N1."BeginStr" AS "DocSeriesPrefix",
N1."EndStr" AS "DocSeriesSuffix",
--IFNULL(t1."LineNum",0) as "InvoiceLineNumber",
IFNULL(t0."CardCode",'') as "CustomerCode",
IFNULL(t0."CardName",'') as "CustomerName",
IFNULL(t3."U_Device_Id",'') as "U_Device_Id",
IFNULL(t3."U_Sim_No",'') as "U_Sim_No", 
--(select IFNULL(t2."Phone1",'') from OCRD t2 where t2."CardCode"=t0."CardCode") as "PhoneNo",
IFNULL(t1."ItemCode",'') as "ItemCode",
IFNULL(t1."Dscription",'') as "Item Description",
IFNULL(t1."Price",0) as "UnitRate" ,
0 as "Original Invoice No",
NULL as "Original Invoice Date",
IFNULL(t1."U_SIM_PACKAGES" ,'') as "SIM_PKG",
IFNULL(t1."U_NO_OF_WARRENTY",'')as "NoOfMonthsWarranty",
--IFNULL(t3."U_Part_No",'') as "U_Part_No",
--IFNULL(t3."U_PCB_No", '') as "U_PCB_No",
--IFNULL(t3."U_HW_ID" , '') as "U_HW_ID",
IFNULL(t0."DocStatus",'') as "Document Status"
from OINV t0 
inner join INV1 t1 on t0."DocEntry"=t1."DocEntry"
Left outer join NNM1 N1 on N1."Series"= t0."Series"
inner join SRI1 t4 on  t4."BaseEntry"=t1."BaseEntry" and  t1."ItemCode"=t4."ItemCode" 
INNER JOIN OSRI t3 on t4."SysSerial"= t3."SysSerial" and t3."ItemCode"=t4."ItemCode" 
where t4."BaseType"=15 and t0."Series" in (93) and T0."CreateDate" = ADD_DAYS(current_date,-1) -- and T0."CreateDate" >= :FromDate and T0."CreateDate" <= :ToDate-- and t0."DocEntry"=4038

union all


select 
IFNULL((CASE T0."GSTTranTyp" WHEN 'GA' THEN 'Gst Tax Invoice' WHEN 'GD' THEN 'Gst Debit Memo' WHEN '' THEN 'Bill Of Supply' END),'') AS "GST Transaction Type", 
'Delivery Challan' as "Document Type",
t0."DocNum" as "Document Number",
IFNULL(TO_VARCHAR(t0."DocDate",'DD/MM/YYYY'),'') as "Document Date" ,
--t0."Series",
IFNULL(N1."SeriesName",'')   as "Series" ,
N1."BeginStr" AS "DocSeriesPrefix",
N1."EndStr" AS "DocSeriesSuffix",
--IFNULL(t1."LineNum",0) as "InvoiceLineNumber",
IFNULL(t0."CardCode",'') as "CustomerCode",
IFNULL(t0."CardName",'') as "CustomerName",
IFNULL(t3."U_Device_Id",'') as "U_Device_Id",
IFNULL(t3."U_Sim_No",'') as "U_Sim_No", 
--(select IFNULL(t2."Phone1",'') from OCRD t2 where t2."CardCode"=t0."CardCode") as "PhoneNo",
IFNULL(t1."ItemCode",'') as "ItemCode",
IFNULL(t1."Dscription",'') as "Item Description",
IFNULL(t1."Price",0) as "UnitRate" ,
0 as "Original Invoice No",
NULL  as "Original Invoice Date",
IFNULL(t1."U_SIM_PACKAGES" ,'') as "SIM_PKG",
IFNULL(t1."U_NO_OF_WARRENTY",'')as "NoOfMonthsWarranty",
--IFNULL(t3."U_Part_No",'') as "U_Part_No",
--IFNULL(t3."U_PCB_No", '') as "U_PCB_No",
--IFNULL(t3."U_HW_ID" , '') as "U_HW_ID",
IFNULL(t0."DocStatus",'') as "Document Status"
from ODLN t0 
inner join DLN1 t1 on t0."DocEntry"=t1."DocEntry"
Left outer join NNM1 N1 on N1."Series"= t0."Series"
inner join SRI1 t4 on  t4."BaseEntry"=t1."BaseEntry" and  t1."ItemCode"=t4."ItemCode" 
INNER JOIN OSRI t3 on t4."SysSerial"= t3."SysSerial" and t3."ItemCode"=t4."ItemCode" 
where t4."BaseType"=15 and t0."Series" in (102) and T0."CreateDate" = ADD_DAYS(current_date,-1) --T0."CreateDate" >= :FromDate and T0."CreateDate" <= :ToDate   -- and t0."DocEntry"=4038


union all

select 
IFNULL((CASE T0."GSTTranTyp" WHEN 'GA' THEN 'Gst Tax Invoice' WHEN 'GD' THEN 'Gst Debit Memo' WHEN '' THEN 'Bill Of Supply' END),'') AS "GST Transaction Type", 
'Credit Note' as "Document Type",
t0."DocNum" as "Document Number",
IFNULL(TO_VARCHAR(t0."DocDate",'DD/MM/YYYY'),'') as "Document Date" ,
--t0."Series",
IFNULL(N1."SeriesName",'')   as "Series" ,
N1."BeginStr" AS "DocSeriesPrefix",
N1."EndStr" AS "DocSeriesSuffix",
--IFNULL(t1."LineNum",0) as "InvoiceLineNumber",
IFNULL(t0."CardCode",'') as "CustomerCode",
IFNULL(t0."CardName",'') as "CustomerName",
IFNULL(t3."U_Device_Id",'') as "U_Device_Id",
IFNULL(t3."U_Sim_No",'') as "U_Sim_No", 
--(select IFNULL(t2."Phone1",'') from OCRD t2 where t2."CardCode"=t0."CardCode") as "PhoneNo",
IFNULL(t1."ItemCode",'') as "ItemCode",
IFNULL(t1."Dscription",'') as "Item Description",
IFNULL(t1."Price",0) as "UnitRate" ,
(select Distinct "DocNum" from OINV where "DocEntry"=t1."BaseEntry") as "Original Invoice No",
(select Distinct "DocDate" from OINV where "DocEntry"=t1."BaseEntry") as "Original Invoice Date",
IFNULL(t1."U_SIM_PACKAGES" ,'') as "SIM_PKG",
IFNULL(t1."U_NO_OF_WARRENTY",'')as "NoOfMonthsWarranty",
--IFNULL(t3."U_Part_No",'') as "U_Part_No",
--IFNULL(t3."U_PCB_No", '') as "U_PCB_No",
--IFNULL(t3."U_HW_ID" , '') as "U_HW_ID",
IFNULL(t0."DocStatus",'') as "Document Status"
from ORIN t0 
inner join RIN1 t1 on t0."DocEntry"=t1."DocEntry"
Left outer join NNM1 N1 on N1."Series"= t0."Series"
inner join SRI1 t4 on  t4."BaseEntry"=(select Distinct "BaseEntry" from Inv1 where "DocEntry"=t1."BaseEntry") and  t1."ItemCode"=t4."ItemCode" 
INNER JOIN OSRI t3 on t4."SysSerial"= t3."SysSerial" and t3."ItemCode"=t4."ItemCode" 
where t4."BaseType"=15  and T0."CreateDate" = ADD_DAYS(current_date,-1); --T0."CreateDate" >= :FromDate and T0."CreateDate" <= :ToDate ; --and t0."Series" in (102) 

END;



