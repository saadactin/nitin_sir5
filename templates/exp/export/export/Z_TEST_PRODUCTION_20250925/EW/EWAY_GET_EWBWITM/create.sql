CREATE PROCEDURE "EWAY_GET_EWBWITM"
    (
 
  In DocEntry	integer , 
    
      In Typ	 integer 
)   

  LANGUAGE SQLSCRIPT  
  SQL SECURITY INVOKER
  AS  
BEGIN


if :Typ=133 then

select 
case when O2."ItemClass"=2 
then substring(O2."ItemCode",0,98) 
else 'null' end as "ItemName",
case when O2."ItemClass"=2 
then substring(O1."Dscription",0,98) 
else 'null' end as "ItemDesc",

case when O2."ItemClass"=1  then 'Y' else 'N' end as "InService",
case when O1."HsnEntry" is null
then (select substring(ifnull("ServCode", ''),1,6) from OSAC where "AbsEntry" in(O1."SacEntry")) 
when   O1."HsnEntry" is not null
then (select substring(replace(replace(ifnull("ChapterID", ''),'.',' '),' ',''),1,4) from OCHP where "AbsEntry" 
in(O1."HsnEntry")) 
end as "HSN",
ifnull(cast((O1."Quantity") as numeric(16,2)),0) as Qty,
ifnull(case when O2."InvntryUom" is null then 'NOS' 
else 
(select "U_MAPUOM" from "@NOUOM" where "Code"=O2."InvntryUom") end,'NOS') as "UOM",

cast(ifnull((SELECT "TaxRate" FROM INV4 WHERE "LineNum" = O1."LineNum" 
AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
AND "staType" IN (-110,-150) AND "RelateType" <> 3), 0) as numeric(16,2)) as "SGST_Sum",

cast(ifnull((SELECT "TaxRate" FROM INV4 WHERE "LineNum" = O1."LineNum" 
AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
AND "staType" IN (-100) AND "RelateType" <> 3), 0) as numeric(16,2)) as "CGST_Sum",

case when O6."GSTCode"<>(case when ifnull(O8."ImpORExp", 'N')='N' 
then ifnull(O9."GSTCode", '') else SUBSTRING(O3."U_Transid",0,3) end) 
then
(cast(ifnull((SELECT "TaxRate" FROM INV4 WHERE "LineNum" = O1."LineNum" 
AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
AND "staType" IN (-120) AND "RelateType" <> 3), 0) as numeric(16,2)))
else
0
end
as "IGST_Sum",

cast(ifnull((Select "TaxRate" FROM INV4 where "LineNum"=O1."LineNum" 
and "StcCode"=O1."TaxCode" and  "DocEntry" = O1."DocEntry" 
and "staType" in(select * from "CESS_STATYPE") 
and "RelateType"<>3),0)as numeric(16,2)) as "Cess_Rate",

case when O2."ItemClass"=2  
then 
case when ifnull(O1."AssblValue",0)=0 then
cast(O1."LineTotal"-((O1."LineTotal"*O3."DiscPrcnt")/100) as Numeric(16,2)) 
else  
cast(O1."AssblValue"-((O1."AssblValue"*O3."DiscPrcnt")/100) as Numeric(16,2)) 
end 
else
cast(O1."LineTotal"-((O1."LineTotal"*O3."DiscPrcnt")/100) as Numeric(16,2)) 
end
as "Assval"

from INV1 O1
inner join OINV O3 on O1."DocEntry"=O3."DocEntry"
left join OITM O2 on O1."ItemCode"=O2."ItemCode"
inner join OLCT O5 on O1."LocCode"=O5."Code"
LEFT join OCST O6 on O5."State"=O6."Code" and O5."Country"=O6."Country"
left join INV12 O8 on O3."DocEntry"=O8."DocEntry"
inner join OCRD  O4 on O3."CardCode"=O4."CardCode"
inner join CRD1 O7 on O4."CardCode"=O7."CardCode"
LEFT OUTER JOIN OCST O9 ON O7."State" = O9."Code" and  O8."BpCountry"=O9."Country"
where O1."DocEntry"=:DocEntry and (O7."Address"=O1."ShipToCode");
end if;

if :Typ=179 then

select 
case when O2."ItemClass"=2 
then substring(O2."ItemCode",0,98) 
else 'null' end as "ItemName",
case when O2."ItemClass"=2 
then substring(O1."Dscription",0,98) 
else 'null' end as "ItemDesc",

case when O2."ItemClass"=1  then 'Y' else 'N' end as "InService",
case when O1."HsnEntry" is null
then (select substring(ifnull("ServCode", ''),1,6) from OSAC where "AbsEntry" in(O1."SacEntry")) 
when   O1."HsnEntry" is not null
then (select substring(replace(replace(ifnull("ChapterID", ''),'.',' '),' ',''),1,4) from OCHP where "AbsEntry" 
in(O1."HsnEntry")) 
end as "HSN",
ifnull(cast((O1."Quantity") as numeric(16,2)),0) as Qty,
ifnull(case when O2."InvntryUom" is null then 'NOS' 
else 
(select "U_MAPUOM" from "@NOUOM" where "Code"=O2."InvntryUom") end,'NOS') as "UOM",

cast(ifnull((SELECT "TaxRate" FROM RIN4 WHERE "LineNum" = O1."LineNum" 
AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
AND "staType" IN (-110,-150) AND "RelateType" <> 3), 0) as numeric(16,2)) as "SGST_Sum",

cast(ifnull((SELECT "TaxRate" FROM RIN4 WHERE "LineNum" = O1."LineNum" 
AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
AND "staType" IN (-100) AND "RelateType" <> 3), 0) as numeric(16,2)) as "CGST_Sum",

case when O6."GSTCode"<>(case when ifnull(O8."ImpORExp", 'N')='N' 
then ifnull(O9."GSTCode", '') else SUBSTRING(O3."U_Transid",0,3) end) 
then
(cast(ifnull((SELECT "TaxRate" FROM INV4 WHERE "LineNum" = O1."LineNum" 
AND "StcCode" = O1."TaxCode" AND "DocEntry" = O1."DocEntry" 
AND "staType" IN (-120) AND "RelateType" <> 3), 0) as numeric(16,2)))
else
0
end
as "IGST_Sum",

cast(ifnull((Select "TaxRate" FROM RIN4 where "LineNum"=O1."LineNum" 
and "StcCode"=O1."TaxCode" and  "DocEntry" = O1."DocEntry" 
and "staType" in(select * from "CESS_STATYPE") 
and "RelateType"<>3),0)as numeric(16,2)) as "Cess_Rate",

case when O2."ItemClass"=2  
then 
case when ifnull(O1."AssblValue",0)=0 then
cast(O1."LineTotal"-((O1."LineTotal"*O3."DiscPrcnt")/100) as Numeric(16,2)) 
else  
cast(O1."AssblValue"-((O1."AssblValue"*O3."DiscPrcnt")/100) as Numeric(16,2)) 
end 
else
cast(O1."LineTotal"-((O1."LineTotal"*O3."DiscPrcnt")/100) as Numeric(16,2)) 
end
as "Assval"


from RIN1 O1
inner join ORIN O3 on O1."DocEntry"=O3."DocEntry"
left join OITM O2 on O1."ItemCode"=O2."ItemCode"
inner join OLCT O5 on O1."LocCode"=O5."Code"
LEFT join OCST O6 on O5."State"=O6."Code" and O5."Country"=O6."Country"
left join RIN12 O8 on O3."DocEntry"=O8."DocEntry"
inner join OCRD  O4 on O3."CardCode"=O4."CardCode"
inner join CRD1 O7 on O4."CardCode"=O7."CardCode"
LEFT OUTER JOIN OCST O9 ON O7."State" = O9."Code" and  O8."BpCountry"=O9."Country"
where O1."DocEntry"=:DocEntry and (O7."Address"=O1."ShipToCode");
end if;

end;


