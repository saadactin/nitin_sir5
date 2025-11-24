CREATE Procedure Instock_item_StockByDate_020623
(IN ToDate datetime)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
 Begin 

 select t0."ItemCode",t0."ItemName",t1."WhsCode",t2."WhsName",0 "Qty In Whse",
 
 (select IFNULL(sum(("InQty")-("OutQty")),0) from OINM nm  where nm."ItemCode"= t0."ItemCode" and 
--nm."DocDate" <= TO_DATE('31/03/2023','DD/MM/YYYY') 
nm."DocDate" <= :ToDate
 ) as"Cumulative Qty",
 
 (select IFNULL(sum(nm."TransValue"),0) from OINM nm  where nm."ItemCode"= t0."ItemCode" and 
--nm."DocDate" <= TO_DATE('31/03/2023','DD/MM/YYYY') 
nm."DocDate" <= :ToDate
 ) as"SumOFTransValue",
 
 (T0."AvgPrice"*T0."OnHand") As "Total Cost",
 t0."AvgPrice" as "Todays Item Cost",
 (t0."AvgPrice" * 0) as "whs wise Trans value",


(select tb."ItmsGrpNam" from OITB tb where tb."ItmsGrpCod"=T0."ItmsGrpCod") as "Item Group Name"



 from OITM t0 inner join OITW t1 on t0."ItemCode"=t1."ItemCode"
 inner join OWHS t2 on t1."WhsCode"=t2."WhsCode"
where t1."OnHand"=0 --and t0."ItemCode"= '1F41.KTL.001.004.01.00' 

union all

select 
T0."ItemCode",
t2."ItemName",
T0."Warehouse",
T1."WhsName",

--sum("InQty"),sum("OutQty") ,
(sum("InQty")- sum("OutQty")) "Qty In Whse" ,
(select IFNULL(sum(("InQty")-("OutQty")),0) 
 
 from OINM nm
 
 where "ItemCode"= T2."ItemCode"  and 
--nm."DocDate" <= TO_DATE('31/03/2023','DD/MM/YYYY') ) as "InStock" ,
nm."DocDate" <= :ToDate) as "Cumulative Qty" ,

(select IFNULL(sum(nm."TransValue"),0) from OINM nm  where nm."ItemCode"= t0."ItemCode" and 
--nm."DocDate" <= TO_DATE('31/03/2023','DD/MM/YYYY') 
nm."DocDate" <= :ToDate
 ) as"SumOFTransValue",
 
 (t2."AvgPrice"*t2."OnHand") As "Total Cost",
 
 t2."AvgPrice" as "Todays Item Cost",
  (t2."AvgPrice" * (sum("InQty")- sum("OutQty"))) as "whs wise Trans value", 

 (select tb."ItmsGrpNam" from OITB tb where tb."ItmsGrpCod"=T2."ItmsGrpCod") as "Item Group Name"
 

 

 
 
 from OINM  
 t0 inner join OWHS t1 on t0."Warehouse" = t1."WhsCode"  
 inner join OITM t2 on T0."ItemCode"=T2."ItemCode"
 where --T0."ItemCode"= '1F41.KTL.001.004.01.00'  and 
--t0."DocDate" <= TO_DATE('31/03/2023','DD/MM/YYYY')
t0."DocDate" <= :ToDate
  group by T0."ItemCode",T0."Warehouse",t1."WhsName",t2."AvgPrice",t2."ItemName",T2."ItemCode",T2."OnHand",T2."ItmsGrpCod"
  order by T0."ItemCode";
  
End

