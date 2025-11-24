CREATE PROCEDURE JOBWORK_DELIVERY_CHALLAN
(IN DOCKEY  int)

LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER

AS
BEGIN
select WT."DocEntry",WT."CardCode" ,WT."CardName",WT."Address",WT."ShipToCode",WT."DocNum",N1."SeriesName",WT."DocDate" 
, WT1."ItemCode", WT1."Dscription", WT1."Quantity",WT1."unitMsr", WT1."U_Subcon_Qty" , OC."Name",OC."E_MailL",OC."Cellolar",
(select "GSTRegnNo" from OLCT)as "CompGSTIN" ,
(SELECT HP."ChapterID" from OCHP HP inner join OITM ITM on HP."AbsEntry"= ITM."ChapterID" where ITM."ItemCode"=WT1."ItemCode") as "HSN"
from OWTR WT 
INNER JOIN WTR1 WT1 on WT."DocEntry"=WT1."DocEntry"
LEFT OUTER JOIN NNM1 N1 ON N1."Series" = WT."Series" 
LEFT OUTER JOIN OCPR OC on WT."CardCode"= OC."CardCode"

where WT."DocEntry"=:DOCKEY;

END;



