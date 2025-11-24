CREATE Procedure Instock_item 
(IN ToDate datetime)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
 Begin 

SELECT T1."ItemCode",T2."ItemName", T0."WhsCode", T0."WhsName", 
 T1."OnHand" as "Qty WH wise" ,
T2."OnHand",
T2."AvgPrice" as "Cost PU",
(T2."AvgPrice"*T2."OnHand") As "Total Cost"

FROM OWHS T0  
INNER JOIN OITW T1 ON T0."WhsCode" = T1."WhsCode" 
INNER JOIN OITM T2 ON T1."ItemCode" = T2."ItemCode" 
INNER JOIN OINM T3 ON T3."ItemCode" = T2."ItemCode" 

WHERE 
  T3."DocDate"<=:ToDate and T1."OnHand" >0
group By T1."ItemCode",T2."ItemName",T0."WhsCode", T0."WhsName", 
T1."OnHand",T2."ItemCode",T2."AvgPrice",T2."OnHand"
order by T1."ItemCode";


end

/*---Thius is old query warehouse wise report----
select itm."ItemCode",itm."ItemName",sum(inm."InQty")-sum(inm."OutQty") "Quantity",itm."AvgPrice",inm."Warehouse"
,(sum(inm."InQty")-sum(inm."OutQty"))*itm."AvgPrice" "Total",ITB."ItmsGrpNam"
 from oinm inm
left outer join oitm itm on inm."ItemCode"=itm."ItemCode"
LEFT OUTER JOIN OITB ITB on ITB."ItmsGrpCod"=Itm."ItmsGrpCod"
where  inm."DocDate"<=:ToDate
group by itm."ItemCode",itm."ItemName",itm."AvgPrice",inm."Warehouse",ITB."ItmsGrpNam";*/


