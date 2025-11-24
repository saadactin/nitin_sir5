Create PROCEDURE "HANA_PARTB_UPDATE"

  (
 
  In DocEntry	 integer , 
    In typ	 integer

)   

  LANGUAGE SQLSCRIPT  
  SQL SECURITY INVOKER
  AS  
BEGIN



if :typ=133 then

Select O1."U_EWBNO",O1."U_VehNo",O3."Location",O4."GSTCode",O1."U_VehBrkCode",O1."U_VehBRES",
ifnull(O1."U_TransDocNo",'null') as "U_TransDocNo",ifnull(O1."U_TransDocDt",'null') as "U_TransDocDt" ,O1."U_TransMode","U_VehType"
from 
OINV O1
left join INV1 O2 on O1."DocEntry"=O2."DocEntry"
Inner join OLCT O3 on O2."LocCode"=O3."Code"
inner join OCST O4 on O3."State"=O4."Code" and O3."Country"=O4."Country"
where O1."DocEntry"=:DocEntry  and ifnull(O1."U_EWBNO",'')<>'';
end if;

if :typ=179 then

Select O1."U_EWBNO",O1."U_VehNo",O3."Location",O4."GSTCode",O1."U_VehBrkCode",O1."U_VehBRES",
ifnull(O1."U_TransDocNo",'null') as "U_TransDocNo",ifnull(O1."U_TransDocDt",'null') as "U_TransDocDt" ,O1."U_TransMode","U_VehType"
from 
ORIN O1
left join RIN1 O2 on O1."DocEntry"=O2."DocEntry"
Inner join OLCT O3 on O2."LocCode"=O3."Code"
inner join OCST O4 on O3."State"=O4."Code" and O3."Country"=O4."Country"
where O1."DocEntry"=:DocEntry and ifnull(O1."U_EWBNO",'')<>'';
end if;

if :typ=140 then

Select O1."U_EWBNO",O1."U_VehNo",O3."Location",O4."GSTCode",O1."U_VehBrkCode",O1."U_VehBRES",
ifnull(O1."U_TransDocNo",'null') as "U_TransDocNo",ifnull(O1."U_TransDocDt",'null') as "U_TransDocDt" ,O1."U_TransMode","U_VehType"
from  
ODLN O1
left join DLN1 O2 on O1."DocEntry"=O2."DocEntry"
Inner join OLCT O3 on O2."LocCode"=O3."Code"
inner join OCST O4 on O3."State"=O4."Code" and O3."Country"=O4."Country"
where O1."DocEntry"=:DocEntry and ifnull(O1."U_EWBNO",'')<>'';
end if;

end;



