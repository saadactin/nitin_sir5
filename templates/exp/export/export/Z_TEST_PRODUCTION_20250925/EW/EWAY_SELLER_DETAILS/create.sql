CREATE PROCEDURE "EWAY_SELLER_DETAILS"  
  (
 
  In DocEntry	integer , 
    
      In FrmTyp	 integer 
)   

  LANGUAGE SQLSCRIPT  
  SQL SECURITY INVOKER
  AS  
BEGIN



if :FrmTyp=133 then

select top 1 O3."GSTRegnNo" as "GSTIN",(select "CompnyName" from OADM)as "LegalName", 
case when O6."DocType"='I' then cast(ifnull(O3."Block",'') as nvarchar)||' '||cast(ifnull(O3."Building",'')
 as nvarchar)||' '||cast(ifnull(O3."Street",'') as nvarchar)else 

cast(ifnull(O3."Block",'') as nvarchar)||' '||cast(ifnull(O3."Building",'') as nvarchar)||' '||
cast(ifnull(O3."Street",'') as nvarchar)

end as "Address1", O3."City" || ','||O4."Name" || ',' ||
O5."Name" as "Address2",O3."Location" as "Location",replace(cast(O3."ZipCode" as nvarchar(50)),' ','') 
as "PinCode",O4."GSTCode" as "StateCode", (select replace(replace("Phone1",' ',''),'-','') from OADM)as 
"Phone",(select "E_Mail" from OADM)as "Email" from INV1  O1 
INNER JOIN OINV O6 on O1."DocEntry"=O6."DocEntry" 
inner join OLCT O3 on O1."LocCode"=O3."Code" 
LEFT join OCST O4 on O3."State"=O4."Code" and O3."Country"=O4."Country"
LEFT join OCRY O5 on O3."Country"=O5."Code" where   O1."DocEntry"=:DocEntry;
end if;

if :FrmTyp=179 then

select top 1 O3."GSTRegnNo" as "GSTIN",(select "CompnyName" from OADM)as "LegalName", 
case when O6."DocType"='I' then cast(ifnull(O3."Block",'') as nvarchar(150))||' '||cast(ifnull(O3."Building",'') as nvarchar(50))||' '||
cast(ifnull(O3."Street",'') as nvarchar(150))else 

cast(ifnull(O3."Block",'') as nvarchar(150))||' '||cast(ifnull(O3."Building",'') as nvarchar(150))||' '||
cast(ifnull(O3."Street",'') as nvarchar(150))

end as "Address1", O3."City"||','||O4."Name"||','||
O5."Name" as "Address2",O3."Location" as "Location",replace(cast(O3."ZipCode" as nvarchar(50)),' ','') 
as "PinCode",O4."GSTCode" as "StateCode", (select replace(replace("Phone1",' ',''),'-','') from OADM)as 
Phone,(select "E_Mail" from OADM)as "Email" from RIN1  O1 
INNER JOIN ORIN O6 on O1."DocEntry"=O6."DocEntry" 
inner join OLCT O3 on O1."LocCode"=O3."Code" 
LEFT join OCST O4 on O3."State"=O4."Code" and O3."Country"=O4."Country"
LEFT join OCRY O5 on O3."Country"=O5."Code" where   O1."DocEntry"=:DocEntry;

end if;
end ;


