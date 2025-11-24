CREATE Procedure Instock_item_warehouse
(IN ToDate datetime)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
 Begin 

select itm."ItemCode",itm."ItemName",sum(inm."InQty")-sum(inm."OutQty") "Quantity",itm."AvgPrice",inm."Warehouse"
,(sum(inm."InQty")-sum(inm."OutQty"))*itm."AvgPrice" "Total",ITB."ItmsGrpNam"
 from oinm inm
left outer join oitm itm on inm."ItemCode"=itm."ItemCode"
LEFT OUTER JOIN OITB ITB on ITB."ItmsGrpCod"=Itm."ItmsGrpCod"
where  inm."DocDate"<=:ToDate --and itm."ItemCode" = '1R13.KTL.198.15.CC'
group by itm."ItemCode",itm."ItemName",itm."AvgPrice",inm."Warehouse",ITB."ItmsGrpNam";


end

/*---Thius is old query warehouse wise report----
select itm."ItemCode",itm."ItemName",sum(inm."InQty")-sum(inm."OutQty") "Quantity",itm."AvgPrice",inm."Warehouse"
,(sum(inm."InQty")-sum(inm."OutQty"))*itm."AvgPrice" "Total",ITB."ItmsGrpNam"
 from oinm inm
left outer join oitm itm on inm."ItemCode"=itm."ItemCode"
LEFT OUTER JOIN OITB ITB on ITB."ItmsGrpCod"=Itm."ItmsGrpCod"
where  inm."DocDate"<=:ToDate
group by itm."ItemCode",itm."ItemName",itm."AvgPrice",inm."Warehouse",ITB."ItmsGrpNam";*/

