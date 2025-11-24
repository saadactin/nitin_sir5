CREATE PROCEDURE "EWAY_DOCDETAIL"
  
  (
 
  In DocEntry	 integer,
      In FrmType	 integer	 
)   

  LANGUAGE SQLSCRIPT  
  SQL SECURITY INVOKER
  AS  
BEGIN


if :FrmType=133 then

select IFNULL(O2."SeriesName",'Manual')||'-'||cast(O1."DocNum" as nvarchar(30)) as "DocNum" from OINV O1
LEFT join NNM1 O2 on O1."Series"=O2."Series" and O2."ObjectCode"='13'
where "DocEntry"=:DocEntry;
end if;

if :FrmType=179 then

select IFNULL(O2."SeriesName",'Manual') ||'-'|| cast(O1."DocNum" as nvarchar(30)) as "DocNum" from ORIN O1
LEFT join NNM1 O2 on O1."Series"=O2."Series" and O2."ObjectCode"='14'
where "DocEntry"=:DocEntry;
end if;

end;

