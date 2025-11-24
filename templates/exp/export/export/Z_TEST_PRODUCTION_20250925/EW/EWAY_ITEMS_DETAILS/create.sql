CREATE PROCEDURE "EWAY_ITEMS_DETAILS"
  
  (
 
  In DocEntry	integer , 
    
      In FrmTyp	 integer 
)   

  LANGUAGE SQLSCRIPT  
  SQL SECURITY INVOKER
  AS  
BEGIN



if :FrmTyp=133  then

select *,cast((A."Assval"+A."IGST_Sum"+A."CGST_Sum"+A."SGST_Sum"+A."Cess_Amt"+A."OthChg")as numeric(16,2))as "TotItemVal"

from(select O1."VisOrder" +1  "ItemCode",O1."LineNum",case when  O3."DocType"='I' then O1."Dscription" else substring(O1."Dscription",0,99) end as "ItemName",
case when (O2."ItemClass"=1 or O3."DocType"='S') then 'Y' else 'N' end as "InService",

case when O1."HsnEntry" is null   
then (select replace(ltrim(replace("ServCode",'0',' ')),' ','0') from OSAC where "AbsEntry" in(O1."SacEntry")) 
when   O1."HsnEntry" is not null
then (select substring(replace(replace(iFnull("ChapterID", ''),'.',' '),' ',''),1,8) from OCHP where "AbsEntry" in(O1."HsnEntry")) 
end as HSN,

IFNULL(O2."CodeBars",'') "Barcode",cast((O1."Quantity") as numeric(16,2))as "Qty",
IFNULL(case when O2."InvntryUom" is null then 'NOS' else (select "U_MAPUOM" from "@NOUOM" where "Code"=O2."InvntryUom") end,'NOS') as "UOM",
(cast((Case when o3."DocCur"!='INR' and O1."Currency"='INR' then  O1."PriceBefDi"  

   when  o3."DocCur" ='INR' and O1."Currency"='INR' then O1."PriceBefDi"

   when  o3."DocCur"!='INR'  and O1."Currency"!='INR' then O1."PriceBefDi"*o3."DocRate" 
end)  as numeric(16,2)))as "Unitprice",
(cast((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end)  as numeric (16,2))) as "ToAmt",



(cast(((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi" *o1."Quantity"*o3."DocRate"  
end)  *o1."DiscPrcnt"/100 )as numeric(16,2))) 

+

(case when o3."DiscPrcnt">0 then



((cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi" *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"   *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2)))) *(o3."DiscPrcnt"/100)







else 0 end)

 as "DiscPrnct",


(cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi" *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"   *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2))) *((case when O3."DiscPrcnt">0 then (100-O3."DiscPrcnt")/100  else 1 end)) "Assval",

IFNULL((Select "TaxRate" FROM INV4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and  "DocEntry" = O1."DocEntry" and "staType" in(select * from "CESS_STATYPE") and "RelateType"<>3),0) as "Cess_Rate" ,
IFNULL((Select "TaxRate" FROM INV4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType"=-120 and "RelateType"<>3),0) as "IGST_Rate" , 
IFNULL((Select "TaxRate" FROM INV4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType"=-100 and "RelateType"<>3),0) as "CGST_Rate" ,
IFNULL((Select "TaxRate" FROM INV4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType" in(-110,-150) and "RelateType"<>3),0) as "SGST_Rate" ,


case when IFNULL(O12."ImpORExp",'N') = 'N' then 
 IFNULL((SELECT "TaxSum" FROM INV4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" IN (SELECT * FROM "CESS_STATYPE") AND "RelateType" <> 3), 0) 
 else 
 IFNULL((SELECT "TaxSumFrgn" FROM INV4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" IN (SELECT * FROM "CESS_STATYPE") AND "RelateType" <> 3), 0) 
 end
 AS "Cess_Amt", 
   
 case when IFNULL(O12."ImpORExp",'N') = 'N' then 
   (case when o3."DiscPrcnt"=0 then

 IFNULL((SELECT "TaxSum" FROM INV4 WHERE "LineNum" = 
 O1."LineNum" AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" = -120 AND "RelateType" <> 3), 0) 

 
 else

 (
(cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi" *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"   *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2))) *

IFNULL((Select "TaxRate" FROM INV4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType" =-120 and "RelateType"<>3),0))/100

end ) --end case for discount amount


 else
 IFNULL((SELECT "TaxSum" FROM INV4 WHERE "LineNum" = 
 O1."LineNum" AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" = -120 AND "RelateType" <> 3), 0)
 
 end AS "IGST_Sum",
 
 case when IFNULL(O12."ImpORExp",'N') = 'N' then
  (case when o3."DiscPrcnt"=0 then
 IFNULL((SELECT "TaxSum" FROM INV4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" = -100 AND "RelateType" <> 3), 0) 

else
(
(cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi" *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"   *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2))) *

IFNULL((Select "TaxRate" FROM INV4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType"=-100 and "RelateType"<>3),0))/100

end ) --end case for discount amount
 else
 IFNULL((SELECT "TaxSumFrgn" FROM INV4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" = -100 AND "RelateType" <> 3), 0)


 end AS "CGST_Sum", 





 
 case when IFNULL(O12."ImpORExp",'N') = 'N' then 
 (case when o3."DiscPrcnt"=0 then 
 
 IFNULL((SELECT "TaxSum" FROM INV4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" IN (-110,-150) AND "RelateType" <> 3), 0)
 else
(
(cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi" *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"   *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2))) *

IFNULL((Select "TaxRate" FROM INV4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType" in (-110,-150) and "RelateType"<>3),0))/100

 end )
 else
 IFNULL((SELECT "TaxSumFrgn" FROM INV4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" IN (-110,-150) AND "RelateType" <> 3), 0) 
 end AS "SGST_Sum",
 
0
 AS "OthChg",


cast(O1."StockPrice" as numeric(16,2)) as "Price",
max(IFNULL(case when O2."ManBtchNum" = 'Y'then case when LENGTH(substring(T5."BatchNum",1,20))<3 then '00'||substring(T5."BatchNum",1,20) else substring(T5."BatchNum",1,20) end
when O2."ManSerNum" = 'Y' then case when LENGTH(substring(T7."DistNumber",1,20))<3 then '00'||substring(T7."DistNumber",1,20)  else substring(T7."DistNumber",1,20)  end  end,'')) as "BatchNum",

max(ifnull(case when O2."ManBtchNum" = 'Y'  then (T5."DocDate")
when O2."ManSerNum" = 'Y' then (T7."CreateDate")  end,'')) as "Expdt",
(ifnull(case when O2."ManBtchNum" = 'Y'  then (T5."CreateDate")
when O2."ManSerNum" = 'Y' then (T7."InDate")  end,'')) as "Wardate"



from INV1 O1  
inner join OINV O3 on O1."DocEntry"=O3."DocEntry"
left join OITM O2 on O1."ItemCode"=O2."ItemCode"
left join OITB O4 on O2."ItmsGrpCod"=o4."ItmsGrpCod"
left join INV12 O12 on O1."DocEntry"=O12."DocEntry"

left join IBT1 T5 on O1."ItemCode"=T5."ItemCode" and O1."LineNum"=T5."BaseLinNum" and O1."DocEntry"=T5."BaseEntry" and T5."BaseType"='13'
left join SRI1 T6 on O1."ItemCode"=T6."ItemCode" and O1."LineNum"=T6."BaseLinNum" and O1."DocEntry"=T6."BaseEntry" and T6."BaseType"='13'
left join OSRN T7 on T6."SysSerial"=T7."SysNumber" and T6."ItemCode"=T7."ItemCode"
--left join OIBT T7 on T5.BatchNum=T7.BatchNum
where O3."DocEntry"=:DocEntry group by O1."DocEntry",O1."VisOrder",
O1."ItemCode",O2."ItemName",O1."Dscription",O2."ItemClass",O3."DocType",T5."CreateDate",T7."InDate",
O1."HsnEntry",O1."SacEntry",O2."CodeBars",O2."ManBtchNum",
O2."ManSerNum",O2."InvntryUom",O1."LineNum",O1."TaxCode",O1."Quantity",O1."PriceBefDi",O3."DiscPrcnt",O1."StockPrice",
O1."LineTotal",O12."ImpORExp",o1."Currency",o3."DocCur",o3."DocRate",o1."DiscPrcnt" 




union all 
select distinct (select count("LineNum")+1 from INV1 where "DocEntry"=:DocEntry)"ItemCode",
(select count("LineNum")+1 from INV1 where "DocEntry"=:DocEntry)"LineNum",'Freight' "ItemName",
 'Y' "InService",
 --(select Top 1 ExpnsCode from INV3 where docentry=1146) 'Freightcode',

 --(select distinct "SacCode" from OEXD where "ExpnsCode"=O1."ExpnsCode")as
(select distinct case when  (select Left( "SacCode",2) from dummy) ='00' then 

(select substring("SacCode" ,3) from dummy)
else
"SacCode"
end 
  from OEXD  where "ExpnsCode"=O1."ExpnsCode") "HSN",
 null "Barcode",
1 as "Qty",
'NOS' "UOM",
 case when O1."LineTotal" <0 then 0 else  O1."LineTotal" end  "Unitprice",
case when O1."LineTotal" <0 then 0 else  O1."LineTotal" end  as "ToAmt",
0  as "DiscPrnct",
case when O1."LineTotal" <0 then 0 else  O1."LineTotal" end  as "Assval",
IFNULL((select case when "TaxSum" <0 then 0 else  "TaxRate" end from inv4 where "DocEntry"=O3."DocEntry" and "ExpnsCode"=O1."ExpnsCode" and "staType" in(select * from "CESS_STATYPE")  ) ,0) as "Cess_Rate" ,

IFNULL((select case when "TaxSum" <0 then 0 else  "TaxRate" end from inv4 where "DocEntry"=O3."DocEntry" and "ExpnsCode"=O1."ExpnsCode" and "staType" =-120  ) ,0) as  "IGST_Rate" ,

IFNULL((select case when "TaxSum" <0 then 0 else  "TaxRate" end from inv4 where "DocEntry"=O3."DocEntry" and "ExpnsCode"=O1."ExpnsCode" and "staType"=-100   ) ,0) as "CGST_Rate" ,

IFNULL((select case when "TaxSum" <0 then 0 else  "TaxRate" end from inv4 where "DocEntry"=O3."DocEntry" and "ExpnsCode"=O1."ExpnsCode" and "staType" in(-110,-150)  ) ,0) as "SGST_Rate" ,


IFNULL((select  case when "TaxSum" <0 then 0 else  "TaxSum" end from inv4 where "DocEntry"=O3."DocEntry" and "ExpnsCode"=O1."ExpnsCode" and "staType" in(select * from "CESS_STATYPE")  ) ,0) as "Cess_Amt" ,

IFNULL((select  case when "TaxSum" <0 then 0 else  "TaxSum" end from inv4 where "DocEntry"=O3."DocEntry" and "ExpnsCode"=O1."ExpnsCode" and "staType" =-120  ) ,0) as  "IGST_Sum" ,

IFNULL((select  case when "TaxSum" <0 then 0 else  "TaxSum" end from inv4 where "DocEntry"=O3."DocEntry" and "ExpnsCode"=O1."ExpnsCode" and "staType"=-100   ) ,0) as "CGST_Sum" ,

IFNULL((select  case when "TaxSum" <0 then 0 else  "TaxSum" end from inv4 where "DocEntry"=O3."DocEntry" and "ExpnsCode"=O1."ExpnsCode" and "staType" in(-110,-150)  ) ,0) as "SGST_Sum" ,



0  AS "OthChg",





  case when O1."LineTotal"<0 then 0 else  O1."LineTotal" end  as "Price",
''  as "BatchNum",
''  as "Expdt",
'' as "Wardate"



from INV3 O1  
inner join OINV O3 on O1."DocEntry"=O3."DocEntry"

where O3."DocEntry"=:DocEntry

)A
order by A."LineNum";
end if;



if :FrmTyp=179 then 

select *,cast((A."Assval"+A."IGST_Sum"+A."CGST_Sum"+A."SGST_Sum"+A."Cess_Amt"+A."OthChg")as numeric(16,2))as "TotItemVal"

from(select O1."VisOrder" +1 "ItemCode",O1."LineNum",case when  O3."DocType"='I' then O1."Dscription" else substring(O1."Dscription",0,99) end as "ItemName",
case when (O2."ItemClass"=1 or O3."DocType"='S') then 'Y' else 'N' end as "InService",

case when O1."HsnEntry" is null
then (select substring(IFNULL("ServCode", ''),1,4) from OSAC where "AbsEntry" in(O1."SacEntry")) 
when   O1."HsnEntry" is not null
then (select substring(replace(replace(IFNULL("ChapterID", ''),'.',' '),' ',''),1,8) from OCHP where "AbsEntry" in(O1."HsnEntry")) 
end as "HSN",

IFNULL(O2."CodeBars",'') "Barcode",cast((O1."Quantity") as numeric(16,2))as "Qty",

IFNULL(case when O2."InvntryUom" is null then 'NOS'
 else (select "U_MAPUOM" from "@NOUOM" where "Code"=O2."InvntryUom") end,'NOS') as "UOM",
(Cast((Case when o3."DocCur"!='INR' and O1."Currency"='INR' then  O1."PriceBefDi"  

   when  o3."DocCur" ='INR' and O1."Currency"='INR' then O1."PriceBefDi"

   when  o3."DocCur"!='INR'  and O1."Currency"!='INR' then O1."PriceBefDi"*o3."DocRate" 
end) as numeric(16,2))) 

as "Unitprice",
(cast((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end)  as numeric (16,2))) as "ToAmt",

--cast(sum(O1.DiscPrcnt) as numeric(16,2)) as DiscPrnct,
 
 --case when O3.DocType='I' then IFNULL(cast((sum(O1.Quantity*(O1.PriceBefDi*O3.DocRate))) as numeric(16,2))-cast(O1.LineTotal-((O1.LineTotal*O3.DiscPrcnt)/100) as Numeric(16,2)),0)
 --else IFNULL(cast((sum(O1.Quantity*O1.PriceBefDi)) as numeric(16,2))-cast((sum(O1.Quantity*(O1.PriceBefDi*O3.DocRate))-((O1.LineTotal*O3.DiscPrcnt)/100))as Numeric(16,2)),0) end
 --as DiscPrnct,

(cast(((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi" *o1."Quantity"*o3."DocRate"  
end)  *o1."DiscPrcnt"/100 )as numeric(16,2))) 

||


(case when o3."DiscPrcnt">0 then



((cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi" *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"   *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2)))) *(o3."DiscPrcnt"/100)

else 0 end)

 as "DiscPrnct",

(cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi" *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"   *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2))) * ((case when O3."DiscPrcnt">0 then (100-O3."DiscPrcnt")/100  else 1 end))
  as "Assval",

IFNULL((Select "TaxRate" FROM RIN4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and  "DocEntry" = O1."DocEntry" and "staType" in(select * from "CESS_STATYPE") and "RelateType"<>3),0) as "Cess_Rate" ,
IFNULL((Select "TaxRate" FROM RIN4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType"=-120 and "RelateType"<>3),0) as "IGST_Rate" , 
IFNULL((Select "TaxRate" FROM RIN4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType"=-100 and "RelateType"<>3),0) as "CGST_Rate" ,
IFNULL((Select "TaxRate" FROM RIN4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType" in(-110,-150) and "RelateType"<>3),0) as "SGST_Rate" ,


case when IFNULL(O12."ImpORExp",'N') = 'N' then 
 IFNULL((SELECT "TaxSum" FROM RIN4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" IN (SELECT * FROM "CESS_STATYPE") AND "RelateType" <> 3), 0) 
 else 
 IFNULL((SELECT "TaxSumFrgn" FROM RIN4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" IN (SELECT * FROM "CESS_STATYPE") AND "RelateType" <> 3), 0) 
 end
 AS "Cess_Amt", 
   
 case when IFNULL(O12."ImpORExp",'N') = 'N' then 

 (case when o3."DiscPrcnt"=0 then
 IFNULL((SELECT "TaxSum" FROM RIN4 WHERE "LineNum" = 
 O1."LineNum" AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" = -120 AND "RelateType" <> 3), 0) 




 else

((cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi"  * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi" *o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi"  *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi"  *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi" *o3."DocRate"   *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2)))
)*
  (IFNULL((Select "TaxRate" FROM RIN4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType"=-120 and "RelateType"<>3),0))/100

 end) --discount tax rate
 else
 IFNULL((SELECT "TaxSum" FROM RIN4 WHERE "LineNum" = 
 O1."LineNum" AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" = -120 AND "RelateType" <> 3), 0)
 
 end AS "IGST_Sum",
 
 case when IFNULL(O12."ImpORExp",'N') = 'N' then 
 (case when o3."DiscPrcnt"=0 then
 IFNULL((SELECT "TaxSum" FROM RIN4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" = -100 AND "RelateType" <> 3), 0) 

 else
 (cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi" *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2)))

*
  (IFNULL((Select "TaxRate" FROM RIN4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType"=-100 and "RelateType"<>3),0))/100


 end) --discount tax rate
 else
 IFNULL((SELECT "TaxSumFrgn" FROM RIN4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" = -100 AND "RelateType" <> 3), 0)
 end AS "CGST_Sum", 
 
 case when IFNULL(O12."ImpORExp",'N') = 'N' then 
 (case when o3."DiscPrcnt"=0 then
 IFNULL((SELECT "TaxSum" FROM RIN4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" IN (-110,-150) AND "RelateType" <> 3), 0)

 else
 (cast(((case when o3."DocCur"!='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then o1."PriceBefDi" * o1."Quantity"

   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"  *o1."Quantity"
end) -
((Case when o3."DocCur"!='INR' and o1."Currency"='INR' then   o1."PriceBefDi" *o1."Quantity"
 

   when  o3."DocCur" ='INR' and o1."Currency"='INR' then  o1."PriceBefDi" *o1."Quantity"


   when  o3."DocCur"!='INR'  and o1."Currency"!='INR' then o1."PriceBefDi"*o3."DocRate"   *o1."Quantity"
end)  *o1."DiscPrcnt"/100)) as numeric(16,2)))


*
  (IFNULL((Select "TaxRate" FROM RIN4 where "LineNum"=O1."LineNum" and "StcCode"=O1."TaxCode" and "DocEntry" = O1."DocEntry" and "staType" in (-110,-150) and "RelateType"<>3),0))/100




 end ) --discount rate
 else
 IFNULL((SELECT "TaxSumFrgn" FROM RIN4 WHERE "LineNum" = O1."LineNum" 
 AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
 AND "staType" IN (-110,-150) AND "RelateType" <> 3), 0) 
 end AS "SGST_Sum",
 
0
 AS "OthChg",




cast(O1."StockPrice" as numeric(16,2)) as "Price",

max(IFNULL(case when O2."ManBtchNum" = 'Y'
then case when LENGTH(substring(T5."BatchNum",1,20))<3 then '00'||substring(T5."BatchNum",1,20) 
else substring(T5."BatchNum",1,20) end
when O2."ManSerNum" = 'Y' 
then case when LENGTH(substring(T7."DistNumber",1,20))<3 then '00'||substring(T7."DistNumber",1,20)  
else substring(T7."DistNumber",1,20)  end  end,'')) as "BatchNum",


max(IFNULL(case when O2."ManBtchNum" = 'Y'  then T5."DocDate"
when O2."ManSerNum" = 'Y' then T7."CreateDate" end,'')) as "Expdt",

max(IFNULL(case when O2."ManBtchNum" = 'Y'  then T5."CreateDate"
when O2."ManSerNum" = 'Y' then T7."InDate"  end,'')) as "Wardate"



from RIN1 O1  
inner join ORIN O3 on O1."DocEntry"=O3."DocEntry"
left join OITM O2 on O1."ItemCode"=O2."ItemCode"
left join OITB O4 on O2."ItmsGrpCod"=o4."ItmsGrpCod"
left join RIN12 O12 on O1."DocEntry"=O12."DocEntry"

left join IBT1 T5 on O1."ItemCode"=T5."ItemCode" and O1."LineNum"=T5."BaseLinNum" and O1."DocEntry"=T5."BaseEntry" and T5."BaseType"='14'
left join SRI1 T6 on O1."ItemCode"=T6."ItemCode" and O1."LineNum"=T6."BaseLinNum" and O1."DocEntry"=T6."BaseEntry" and T6."BaseType"='14'
left join OSRN T7 on T6."SysSerial"=T7."SysNumber" and T6."ItemCode"=T7."ItemCode"
--left join OIBT T7 on T5.BatchNum=T7.BatchNum
where O3."DocEntry"=:DocEntry group by O1."DocEntry",O1."VisOrder",
O1."ItemCode",O2."ItemName",O1."Dscription",O2."ItemClass",O3."DocType",
O1."HsnEntry",O1."SacEntry",O2."CodeBars",O2."ManBtchNum",
O2."ManSerNum",O2."InvntryUom",O1."LineNum",O1."TaxCode",O1."Quantity",O1."PriceBefDi",O3."DiscPrcnt",O1."StockPrice",
O1."LineTotal",O12."ImpORExp",o3."DocCur",o1."Currency",o3."DocRate",o1."DiscPrcnt"


)A
order by A."LineNum";
end if;
end;
