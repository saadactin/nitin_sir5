create PROCEDURE "EWAY_GET_EWBWIRN"
  
  (
 
  In DocEntry	integer , 
    
      In FrmTyp	 integer 
)   

  LANGUAGE SQLSCRIPT  
  SQL SECURITY INVOKER
  AS  
BEGIN




if :FrmTyp=133 then


select *,
Case when A."FRM_GSTIN"=A."To_GSTN" then 'CHL' else 'INV' end as "DocType",
Case when A."FRM_GSTIN"=A."To_GSTN" then '8' else
case when ifnull(A."Export", 'N')='N' then '1' else '3'
end
end
"SubSupplyType" 

from(select 'O' as "SupplyType",case when ifnull(O8."ImpORExp", 'N')='N'  then 'N' else 'Y' end as "Export",
case when ifnull(O3."SeriesName",'')<>'' then cast(O1."DocNum" as nvarchar(250)) else cast(O1."DocNum" as nvarchar(250)) end   as "DocNum",
(O1."DocDate") as "Docdate",
O5."GSTRegnNo" as "FRM_GSTIN",(select "CompnyName" from OADM)as "Frm_TrdName",
cast(ifnull(O5."Block",'') as nvarchar)||' '||cast(ifnull(O5."Building",'') as nvarchar)||' '||
cast(ifnull(O5."Street",'') as nvarchar) as "Frm_Address1",
replace(cast(O5."ZipCode" as nvarchar),' ','') 
as "Frm_PinCode",O6."GSTCode" as "Frm_StateCode",
O6."GSTCode" as "Act_Frm_StateCode",

case when ifnull(O8."ImpORExp", 'N')='N'then 
case when ifnull(O1."UseBilAddr",'N')='N' then
ifnull(O7."GSTRegnNo", '') 
else
(Select ifnull("GSTRegnNo",'') from CRD1 where "CardCode"=O1."CardCode" and "Address"=O1."PayToCode" 
and "AdresType" = 'B')
end
else 'URP'  end AS "To_GSTN",

case when ifnull(O1."UseBilAddr",'N')='N' then O7."Address" else  O1."PayToCode" end as "To_Trdname",
replace(CAST(ifnull(O7."Block", '') AS nvarchar) || ' ' 
|| CAST(ifnull(O7."Building", '') AS nvarchar) || ' ' 
|| CAST(ifnull(O7."StreetNo", '') AS nvarchar),'"','') AS "To_Address1",

case when ifnull(O8."ImpORExp", 'N')='Y' 
then
ifnull(Replace(CAST(O8."PortCode" AS nvarchar), ' ', ''), '')
else
ifnull(Replace(CAST(O7."ZipCode" AS nvarchar), ' ', ''), '')
end
AS "TO_PinCode",

case when ifnull(O8."ImpORExp", 'N')='N'
then 
case when ifnull(O1."UseBilAddr",'N')='N' then
ifnull(O9."GSTCode", '') 
else
(Select ifnull(S2."GSTCode",'') from CRD1 S1 
inner join OCST S2 on S1."State"=S2."Code" and S1."Country"=S2."Country"
where S1."CardCode"=O1."CardCode" and S1."Address"=O1."PayToCode" 
and S1."AdresType" = 'B')
end
else '96' 
end AS "To_stateCode", 

case when ifnull(O8."ImpORExp", 'N')='N' 
then ifnull(O9."GSTCode", '') else SUBSTRING(O1."U_Transid",0,3) end AS "Act_To_stateCode",

case when ifnull(O8."ImpORExp", 'N')='Y' 
then
ifnull(O1."U_Transid", '')
else
ifnull(O7."GSTRegnNo", '') 
end as "Shipto_GSTN",
'4' as "TransType",o7."Address" as "shipToTradeName",

cast((SELECT SUM(O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100))
FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
WHERE O1."DocEntry" = O4."DocEntry") as numeric(16,2))
AS "totalValue",

case when O6."GSTCode"<>(case when ifnull(O8."ImpORExp", 'N')='N' 
then ifnull(O9."GSTCode", '') else SUBSTRING(O1."U_Transid",0,3) end) then
(cast(ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = O4."DocEntry" AND "staType" = -120 
AND "RelateType" NOT IN (3,2)), 0) as numeric(16,2)))
else
0 end
AS "IGST", 

cast(ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = O4."DocEntry" AND "staType" = -100 
AND "RelateType" NOT IN (3,2)), 0)as numeric(16,2)) AS "CGST", 
      
cast(ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = O4."DocEntry" AND "staType" IN (-110,-150) 
AND "RelateType" NOT IN (3,2)), 0)as numeric(16,2)) AS "SGST",

cast(ifnull((SELECT SUM("TaxSum") 
FROM INV4 WHERE "DocEntry" = O4."DocEntry" 
AND "staType" IN (SELECT * FROM CESS_staType) AND "RelateType" NOT IN (3,2)), 0)as numeric(16,2)) AS "Cess",

cast(ifnull((SELECT sum("TaxSum") FROM INV4 WHERE "LineNum" = O4."LineNum" 
AND "StcCode" = O4."TaxCode" AND "DocEntry" = O4."DocEntry" 
AND "staType" IN (SELECT * FROM "TCS_STATYPE") AND "RelateType" NOT IN (3,2)), 0)as numeric(16,2)) as "TCSChg",


cast(ifnull((SELECT SUM("LineTotal") 
FROM INV3 WHERE "DocEntry" = O4."DocEntry"), 0)as numeric(16,2))
AS "Freight", 
      
cast(ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = O4."DocEntry" 
AND "RelateType" IN (3,2)), 0)as numeric(16,2))
AS "F_Ttax",

cast(O1."RoundDif" as numeric(16,2))"RoundDif",

cast(O1."DocTotal" as numeric(16,2))  AS "Net Value",
ifnull(O1."U_Transid",'null')as "Transid",
ifnull(O1."U_TransName",'null')as "TransName",
ifnull(O1."U_TransMode",'')as "TransMode",
ifnull(O1."U_Distance",'')as "Distance",
ifnull(O1."U_TransDocNo",'null')as "TransDocNo",
ifnull("U_TransDocDt",'')as "TransDocDt",
ifnull(O1."U_VehNo",'null')as "VehNo",
ifnull("U_VehType",'')as "VehType"

from OINV O1
inner join OCRD o2 on O1."CardCode"=O2."CardCode"
left join NNM1 o3 on O1."Series"=O3."Series" and O3."ObjectCode"='13'
inner join INV1 O4 on o1."DocEntry"=O4."DocEntry"
inner join OLCT O5 on O4."LocCode"=O5."Code" 
LEFT join OCST O6 on O5."State"=O6."Code" and O5."Country"=O6."Country"
inner join CRD1 O7 on O2."CardCode"=O7."CardCode"
left join INV12 O8 on o1."DocEntry"=O8."DocEntry"
LEFT OUTER JOIN OCST O9 ON O7."State" = O9."Code" and  O8."BpCountry"=O9."Country"
where O1."DocEntry"=:DocEntry and (O7."Address"=O1."ShipToCode"))A

group by 
A."SupplyType",A."Export",A."DocNum",A."Docdate",A."FRM_GSTIN",A."Frm_TrdName",A."Frm_Address1",A."Frm_PinCode",
A."Frm_StateCode",A."Act_Frm_StateCode",A."To_GSTN",A."To_Trdname",A."To_Address1",A."TO_PinCode",A."To_stateCode",
A."Act_To_stateCode",A."Shipto_GSTN",A."TransType",A."shipToTradeName",A."totalValue",A."IGST",A."CGST",A."SGST",A."Cess",A."Net Value",A."Transid",A."TransName",A."TransMode",A."Distance",
A."TransDocNo",A."TransDocDt",A."VehNo",A."VehType",A."TCSChg",A."Freight",A."F_Ttax",A."RoundDif";
end if;

if :FrmTyp=179 then

select *,

Case when A."FRM_GSTIN"=A."To_GSTN" then 'CHL' else 'CNT' end as "DocType",
Case when A."FRM_GSTIN"=A."To_GSTN" then '8' else
case when ifnull(A."Export", 'N')='N' then '1' else '3'
end
end
"SubSupplyType" 

from(select 'O' "SupplyType",case when ifnull(O8."ImpORExp", 'N')='N'  then 'N' else 'Y' end as "Export",
case when ifnull(O3."SeriesName",'')<>'' then cast(O1."DocNum" as nvarchar(250)) else cast(O1."DocNum" as nvarchar(250)) end   as "DocNum",
O1."DocDate" as "Docdate",
O5."GSTRegnNo" as "FRM_GSTIN",(select "CompnyName" from OADM)as "Frm_TrdName",
cast(ifnull(O5."Block",'') as nvarchar)||' '||cast(ifnull(O5."Building",'') as nvarchar)||' '||
cast(ifnull(O5."Street",'') as nvarchar) as "Frm_Address1",
replace(cast(O5."ZipCode" as nvarchar),' ','') 
as "Frm_PinCode",O6."GSTCode" as "Frm_StateCode",
O6."GSTCode" as "Act_Frm_StateCode",
case when ifnull(O8."ImpORExp", 'N')='N'then 
case when ifnull(O1."UseBilAddr",'N')='N' then
ifnull(O7."GSTRegnNo", '') 
else
(Select ifnull("GSTRegnNo",'') from CRD1 where "CardCode"=O1."CardCode" and "Address"=O1."PayToCode" 
and "AdresType" = 'B')
end
else 'URP'  end AS "To_GSTN",
case when ifnull(O1."UseBilAddr",'N')='N' then O7."Address" else  O1."PayToCode" end as "To_Trdname",
CAST(ifnull(O7."Block", '') AS nvarchar) || ' ' 
|| CAST(ifnull(O7."Building", '') AS nvarchar) || ' ' 
|| CAST(ifnull(O7."StreetNo", '') AS nvarchar) AS "To_Address1",
case when ifnull(O8."ImpORExp", 'N')='N' 
then
ifnull(Replace(CAST(O8."PortCode" AS nvarchar), ' ', ''), '')
else
ifnull(Replace(CAST(O7."ZipCode" AS nvarchar), ' ', ''), '')
end
AS "TO_PinCode",
case when ifnull(O8."ImpORExp", 'N')='N'
then 
case when ifnull(O1."UseBilAddr",'N')='N' then
ifnull(O9."GSTCode", '') 
else
(Select ifnull(S2."GSTCode",'') from CRD1 S1 
inner join OCST S2 on S1."State"=S2."Code" and S1."Country"=S2."Country"
where S1."CardCode"=O1."CardCode" and S1."Address"=O1."PayToCode" 
and S1."AdresType" = 'B')
end 
else '96' 
end AS "To_stateCode", 


case when ifnull(O8."ImpORExp", 'N')='N' 
then ifnull(O9."GSTCode", '') else SUBSTRING(O1."U_Transid",0,3) end AS "Act_To_stateCode",
case when ifnull(O8."ImpORExp", 'N')='Y' 
then
ifnull(O1."U_Transid", '')
else
ifnull(O7."GSTRegnNo", '') end as "Shipto_GSTN",
'1' as "TransType",o7."Address" as "shipToTradeName",

cast((SELECT SUM(O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100))
FROM RIN1 O1 INNER JOIN ORIN O2 ON O1."DocEntry" = O2."DocEntry" 
WHERE O1."DocEntry" = O4."DocEntry") as numeric(16,2))
AS "totalValue",

case when O6."GSTCode"<>(case when ifnull(O8."ImpORExp", 'N')='N' 
then ifnull(O9."GSTCode", '') else SUBSTRING(O1."U_Transid",0,3) end) then
(cast(ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = O4."DocEntry" AND "staType" = -120 
AND "RelateType" NOT IN (3,2)), 0) as numeric(16,2)))
else
0 end
AS "IGST", 

cast(ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = O4."DocEntry" AND "staType" = -100 
AND "RelateType" NOT IN (3,2)), 0)as numeric(16,2)) AS "CGST", 
      
cast(ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = O4."DocEntry" AND "staType" IN (-110,-150) 
AND "RelateType" NOT IN (3,2)), 0)as numeric(16,2)) AS "SGST",

cast(ifnull((SELECT SUM("TaxSum") 
FROM RIN4 WHERE "DocEntry" = O4."DocEntry" 
AND "staType" IN (SELECT * FROM "CESS_STATYPE") AND "RelateType" NOT IN (3,2)), 0)as numeric(16,2)) AS "Cess",

cast(ifnull((SELECT sum("TaxSum") FROM RIN4 WHERE "LineNum" = O4."LineNum" 
AND "StcCode" = O4."TaxCode" AND "DocEntry" = O4."DocEntry" 
AND "staType" IN (SELECT * FROM "TCS_STATYPE") AND "RelateType" NOT IN (3,2)), 0)as numeric(16,2)) as "TCSChg",


cast(ifnull((SELECT SUM("LineTotal") 
FROM RIN3 WHERE "DocEntry" = O4."DocEntry"), 0)as numeric(16,2))
AS "Freight", 
      
cast(ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = O4."DocEntry" 
AND "RelateType" IN (3,2)), 0)as numeric(16,2))
AS "F_Ttax",

cast(o1."RoundDif" as numeric(16,2))"RoundDif",


cast(O1."DocTotal" as numeric(16,2))  AS "Net Value",
ifnull(O1."U_Transid",'null')as "Transid",
ifnull(O1."U_TransName",'null')as "TransName",
ifnull(O1."U_TransMode",'')as "TransMode",
ifnull(O1."U_Distance",'')as "Distance",
ifnull(O1."U_TransDocNo",'null')as "TransDocNo",
ifnull("U_TransDocDt",'')as "TransDocDt",
ifnull(O1."U_VehNo",'null')as "VehNo",
ifnull("U_VehType",'')as "VehType"


from ORIN O1
inner join OCRD o2 on O1."CardCode"=O2."CardCode"
left join NNM1 o3 on O1."Series"=O3."Series" and O3."ObjectCode"='14'
inner join RIN1 O4 on o1."DocEntry"=O4."DocEntry"
inner join OLCT O5 on O4."LocCode"=O5."Code" 
LEFT join OCST O6 on O5."State"=O6."Code" and O5."Country"=O6."Country"
inner join CRD1 O7 on O2."CardCode"=O7."CardCode"
left join RIN12 O8 on o1."DocEntry"=O8."DocEntry"
LEFT OUTER JOIN OCST O9 ON O7."State" = O9."Code" and  O8."BpCountry"=O9."Country"
where O1."DocEntry"=:DocEntry and (O7."Address"=O1."ShipToCode"))A

group by 
A."SupplyType",A."Export",A."DocNum",A."Docdate",A."FRM_GSTIN",A."Frm_TrdName",A."Frm_Address1",A."Frm_PinCode",
A."Frm_StateCode",A."Act_Frm_StateCode",A."To_GSTN",A."To_Trdname",A."To_Address1",A."TO_PinCode",A."To_stateCode",
A."Act_To_stateCode",A."Shipto_GSTN",A."TransType",A."shipToTradeName",A."totalValue",A."IGST",A."CGST",A."SGST",A."Cess",A."Net Value",A."Transid",A."TransName",A."TransMode",A."Distance",
A."TransDocNo",A."TransDocDt",A."VehNo",A."VehType",A."TCSChg",A."Freight",A."F_Ttax",A."RoundDif";
end if;

end ;



