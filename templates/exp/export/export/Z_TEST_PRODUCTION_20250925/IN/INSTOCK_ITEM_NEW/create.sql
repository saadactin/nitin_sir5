CREATE Procedure Instock_item_new 
(IN ToDate datetime)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
 Begin 

select t0."ItemCode",t0."ItemName",t1."WhsCode",t2."WhsName",0 "Qty In Whse",
 (select sum(("InQty")-("OutQty")) from OINM  where "ItemCode"= t0."ItemCode" and 
 "DocDate" <= TO_DATE('31/05/2021','DD/MM/YYYY') ) as"InStock",
 t0."AvgPrice"

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
(select sum(("InQty")-("OutQty")) 
 
 from OINM 
 
 where "ItemCode"= T2."ItemCode"  and 
 --"DocDate" <= TO_DATE('31/05/2021','DD/MM/YYYY') ) as "InStock" ,
"DocDate" <= :ToDate ) as "InStock" ,
 t2."AvgPrice"
 
 
 from OINM 
 
 t0 inner join OWHS t1 on t0."Warehouse" = t1."WhsCode"  
 inner join OITM t2 on T0."ItemCode"=T2."ItemCode"
 where --T0."ItemCode"= '1F41.KTL.001.004.01.00'  and 
--"DocDate" <= TO_DATE('31/05/2021','DD/MM/YYYY')
"DocDate" <= :ToDate
  group by t0."ItemCode",T0."Warehouse",t1."WhsName",t2."AvgPrice",t2."ItemName",T2."ItemCode"
  order by T0."ItemCode";
  
  End


