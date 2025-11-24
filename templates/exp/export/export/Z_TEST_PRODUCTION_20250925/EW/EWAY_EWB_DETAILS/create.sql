create PROCEDURE "EWAY_EWB_DETAILS"
  
  (
 
  In DocEntry	integer , 
    
      In FrmTyp	 integer 
)   

  LANGUAGE SQLSCRIPT  
  SQL SECURITY INVOKER
  AS  
BEGIN


if :FrmTyp=133 then

select ifnull(O1."U_Transid",'')as "Transid",ifnull("U_TransName",'')as "TransName",ifnull("U_TransMode",'')as "TransMode",
ifnull(O1."U_Distance",'')as "Distance",ifnull("U_TransDocNo",'')as "TransDocNo",ifnull("U_TransDocDt",null)as "TransDocDt",
ifnull(O1."U_VehNo",'')as "VehNo",ifnull("U_VehType",'')as "VehType"
from OINV O1
where O1."DocEntry"=:DocEntry and O1."U_Distance" is not null;
end if;

if :FrmTyp=179 then

select ifnull(O1."U_Transid",'')as "Transid",ifnull("U_TransName",'')as "TransName",ifnull("U_TransMode",'')as "TransMode",
ifnull(O1."U_Distance",'')as "Distance",ifnull("U_TransDocNo",'')as "TransDocNo",ifnull("U_TransDocDt",null)as "TransDocDt",
ifnull(O1."U_VehNo",'')as "VehNo",ifnull("U_VehType",'')as "VehType"
from ORIN O1
where O1."DocEntry"=:DocEntry and O1."U_Distance" is not null;
end if;
end ;



