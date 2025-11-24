CREATE PROCEDURE "EWAY_Value_DETAILS"
  
  (
 
  In DocEntry	integer , 
    
      In FrmTyp	 integer 
)   

  LANGUAGE SQLSCRIPT  
  SQL SECURITY INVOKER
  AS  
BEGIN



if :FrmTyp=133 then

select DISTINCT (case when A."BaseType"='67' then 'Y' else case when  d."AssblValue">0 then 'Y' else 'N' end end) as "AssVal"

,a."DocNum" as "Invoice No" ,a."NumAtCard" AS "Customer Reference" ,a."Comments", 
a."CardCode" as "Cust.Code", a."CardName" as "Customer Name" ,(Select max (T2."TaxId2") from CRD7 T2 where T2."CardCode" = a."CardCode") "Tin#",  a."DocDate" AS "Invoice Date" ,
(SELECT SUM("Quantity") FROM INV1 where "DocEntry" =a."DocEntry")as "Quantity" ,

--(select sum((O1.LineTotal-((O1.LineTotal*O2.DiscPrcnt)/100))) from INV1  O1 Inner join OINV O2 on O1.DocEntry=O2.DocEntry where O1.docentry = a.docentry)as "Basic Value" ,

 ifnull(case when d."AssblValue"=0 then
   
   case when ifnull(T8."ImpORExp",'N') = 'N' then 
   (SELECT 
   SUM((O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   else
  (SELECT SUM((O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   end
  
    
   when d."AssblValue">0 then
    
   case when ifnull(T8."ImpORExp",'N') = 'N' then 
   (SELECT 
   SUM(((O1."Quantity" * O1."AssblValue") - (((O1."Quantity" * O1."AssblValue") * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   else
  (SELECT SUM((O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   end
  
 
 
   end,0) +
  case when
  (
   (select  ifnull(sum(f1."LineTotal"),0) from inv3 f1 
   
   where f1."DocEntry"=a."DocEntry" )
   )>0 then
   (select  sum(f1."LineTotal") from inv3 f1 
   
   where f1."DocEntry"=a."DocEntry" )
   
   else
    0 end
   
   --(select case when (ifnull("LineTotal",0))>0 then ifnull ("LineTotal",0) else 0 end  from  inv3 where "DocEntry" = a."DocEntry") 
   AS "Basic Value",

case when A."BaseType"='67' then 
(select sum((O1."LineTotal"-((O1."LineTotal"*O2."DiscPrcnt")/100))) from INV1  O1 Inner join OINV O2 on O1."DocEntry"=O2."DocEntry" where O1."DocEntry" = a."DocEntry")
else
A."DiscSum" end as "Discount",



     ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = a."DocEntry" AND "staType" = -90 
     AND "RelateType" NOT IN (3,2)), 0) AS "BED", 
     
     ifnull((SELECT SUM("TaxSum") 
     FROM INV4 WHERE "DocEntry" = a."DocEntry" 
     AND "staType" IN (SELECT * FROM "CESS_STATYPE") AND "RelateType" NOT IN (3,2)), 0) AS "Cess", 
     
     ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = a."DocEntry" AND "staType" = 7 
     AND "RelateType" NOT IN (3,2)), 0) AS "HeCess", 
     
     ifnull((SELECT SUM(case when "TaxSum" <0 then 0 else "TaxSum" end) 
     FROM INV4 WHERE "DocEntry" = a."DocEntry" AND "staType" = 1 AND "RelateType" NOT IN (3,2)), 0) AS "VAT",
     
     ifnull((SELECT SUM(case when "TaxSum" <0 then 0 else "TaxSum" end) FROM INV4 
      WHERE "DocEntry" = a."DocEntry" AND "staType" = 4 AND "RelateType" NOT IN (3,2)), 0) AS "CST", 
      
      
      
      
      
      
      
            case when ifnull(T8."ImpORExp",'')='N' then ifnull((SELECT SUM(case when "TaxSum" <0 then 0 else "TaxSum" end) FROM INV4 WHERE "DocEntry" = a."DocEntry" AND "staType" = -120 
      AND "RelateType" NOT IN (3,2)), 0) 
      
      else
      ifnull((SELECT SUM( case when "TaxSum" <0 then 0 else "TaxSum" end) FROM INV4 WHERE "DocEntry" = a."DocEntry" AND "staType" = -120 
      AND "RelateType" NOT IN (3,2)), 0) 
      end
	  +(
	  
	  ifnull((select sum( case when "TaxSum" <0 then 0 else "TaxSum" end) from inv4 where "DocEntry"=a."DocEntry"  and "staType" =-120  and "ExpnsCode" >0  ) ,0) )
	 
      AS "IGST", 
      
      
      
      
       IFNULL((SELECT SUM(case when "TaxSum" <0 then 0 else "TaxSum" end) FROM INV4 WHERE "DocEntry" = a."DocEntry" AND "staType" = -100 
      AND "RelateType" NOT IN (3,2)), 0)
	  +(IFNULL((select sum(case when "TaxSum" <0 then 0 else "TaxSum" end) from inv4 where "DocEntry"=a."DocEntry"  and "staType"=-100  and   "ExpnsCode" >0  ) ,0)) AS "CGST", 
      
      IFNULL((SELECT SUM(case when "TaxSum" <0 then 0 else "TaxSum" end) FROM INV4 WHERE "DocEntry" = a."DocEntry" AND "staType" IN (-110,-150) 
      AND "RelateType" NOT IN (3,2)), 0)
	   +(IFNULL((select sum(case when "TaxSum" <0 then 0 else "TaxSum" end) from inv4 where "DocEntry"=a."DocEntry"  and "staType" IN (-110,-150)   and   "ExpnsCode" >0  ) ,0)) AS "SGST", 
	   
	   A."VatSum" AS "Total Tax",
      
0
	  AS "Freight", 
      
     /* case when IFNULL(T8."ImpORExp",'')='N' then  
      IFNULL((SELECT  case when SUM("TaxSum") <0 then 0 else sum("TaxSum") end FROM INV4 WHERE "DocEntry" = a."DocEntry" 
      AND "RelateType" IN (3,2)), 0) 
      else
      IFNULL((SELECT  case when SUM("TaxSumFrgn") <0 then 0 else sum("TaxSumFrgn")end  FROM INV4 WHERE "DocEntry" = a."DocEntry" 
      AND "RelateType" IN (3,2)), 0) 
      end */
     0  AS "F_Ttax",
      
      --IFNULL((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = a."DocEntry"
      -- AND "staType" IN (SELECT * FROM TCS_statype) AND "RelateType" NOT IN (3,2)), 0) AS "OthChg",
		
		0 AS "OthChg",
       
       A."RoundDif",case when IFNULL(T8."ImpORExp",'')='N' then
	   (case  when  A."DocTotal">0 then A."DocTotal"
	   else(IFNULL(case when d."AssblValue"=0 then
   
   case when IFNULL(T8."ImpORExp",'N') = 'N' then 
   (SELECT 
   SUM((O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   else
  (SELECT SUM((O1."TotalFrgn" - ((O1."TotalFrgn" * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   end
  
    
   when d."AssblValue">0 then
    
   case when IFNULL(T8."ImpORExp",'N') = 'N' then 
   (SELECT 
   SUM(((O1."Quantity" * O1."AssblValue") - (((O1."Quantity" * O1."AssblValue") * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   else
  (SELECT SUM((O1."TotalFrgn" - ((O1."TotalFrgn" * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   end
  
 
 
   end,0))+

   (A."VatSum")
		
		
		end)
				
		
		
		
		
		
		 else
		  (case  when  	 A."DocTotalFC">0 then A."DocTotal"
	   else(IFNULL(case when d."AssblValue"=0 then
   
   case when IFNULL(T8."ImpORExp",'N') = 'N' then 
   (SELECT 
   SUM((O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   else
  (SELECT SUM((O1."TotalFrgn" - ((O1."TotalFrgn" * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   end
  
    
   when d."AssblValue">0 then
    
   case when IFNULL(T8."ImpORExp",'N') = 'N' then 
   (SELECT 
   SUM(((O1."Quantity" * O1."AssblValue") - (((O1."Quantity" * O1."AssblValue") * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   else
  (SELECT SUM((O1."TotalFrgn" - ((O1."TotalFrgn" * O2."DiscPrcnt") / 100)))
   FROM INV1 O1 INNER JOIN OINV O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   end
  
 
 
   end,0))+

   (A."DocTotal") end)

		 
		  end AS "Net Value",
		  
		  
		  
		  
		  
		  
		  
		  
		   A."DocTotalFC" AS "FC Value"
      
      /*
      ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = a."DocEntry" AND "staType" = -100 
      AND "RelateType" NOT IN (3,2)), 0) AS "CGST", 
      
      ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = a."DocEntry" AND "staType" IN (-110,-150) 
      AND "RelateType" NOT IN (3,2)), 0) AS "SGST", A."VatSum" AS "Total Tax",
      
      
      
      case when ifnull(T8."ImpORExp",'')='N' then ifnull((SELECT SUM("LineTotal") 
      FROM INV3 WHERE "DocEntry" = a."DocEntry"), 0) 
      else
      ifnull((SELECT SUM("TotalFrgn") 
      FROM INV3 WHERE "DocEntry" = a."DocEntry"), 0) 
      end AS "Freight", 
      
      case when ifnull(T8."ImpORExp",'')='N' then  
      ifnull((SELECT SUM("TaxSum") FROM INV4 WHERE "DocEntry" = a."DocEntry" 
      AND "RelateType" IN (3,2)), 0) 
      else
      ifnull((SELECT SUM("TaxSumFrgn") FROM INV4 WHERE "DocEntry" = a."DocEntry" 
      AND "RelateType" IN (3,2)), 0) 
      end AS "F_Ttax",
      
     
		
		0 AS "OthChg",
       
       A."RoundDif",case when ifnull(T8."ImpORExp",'')='N' then A."DocTotal" else A."DocTotalFC" end AS "Net Value", A."DocTotalFC" AS "FC Value"*/


FROM OINV A left OUTER JOIN INV3 B ON A."DocEntry"  = B."DocEntry"  left OUTER JOIN INV4 C 
ON A."DocEntry"  = C."DocEntry"  left outer join INV3 h on A."DocEntry"  = h."DocEntry"  left outer join 
INV1 d on a."DocEntry"  = d."DocEntry"  left outer join crd7 e on a."CardCode" = e."CardCode" Left Outer Join
oitm g On d."ItemCode"=g."ItemCode" left outer join ochp f on f."AbsEntry"= g."ChapterID"
left join INV12 T8 on A."DocEntry" =T8."DocEntry"
where A."DocEntry" =:DocEntry;
end if ;


if :FrmTyp=179 then

select DISTINCT (case when A."BaseType"='67' then 'Y' else case when  d."AssblValue">0 then 'Y' else 'N' end end)
 as "AssVal"

,a."DocNum" as "Invoice No" ,a."NumAtCard" AS "Customer Reference" ,a."Comments", 
a."CardCode" as "Cust.Code", a."CardName" as "Customer Name" ,(Select max (T2."TaxId2") from CRD7 T2 where T2."CardCode" = a."CardCode") "Tin#",  a."DocDate" AS "Invoice Date" ,(SELECT SUM("Quantity") FROM RIN1 where "DocEntry" =a."DocEntry")as "Quantity" ,



 ifnull(case when d."AssblValue"=0 then
   
   case when ifnull(T8."ImpORExp",'N') = 'N' then 
   (SELECT 
   SUM((O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100)))
   FROM RIN1 O1 INNER JOIN ORIN O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   else
  (SELECT SUM((O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100)))
   FROM RIN1 O1 INNER JOIN ORIN O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   end
  
    
   when d."AssblValue">0 then
    
   case when ifnull(T8."ImpORExp",'N') = 'N' then 
   (SELECT 
   SUM(((O1."Quantity" * O1."AssblValue") - (((O1."Quantity" * O1."AssblValue") * O2."DiscPrcnt") / 100)))
   FROM RIN1 O1 INNER JOIN ORIN O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   else
  (SELECT SUM((O1."LineTotal" - ((O1."LineTotal" * O2."DiscPrcnt") / 100)))
   FROM RIN1 O1 INNER JOIN ORIN O2 ON O1."DocEntry" = O2."DocEntry" 
   WHERE O1."DocEntry" = a."DocEntry")
   end
  
 
   end,0)
   AS "Basic Value",

case when A."BaseType"='67' then 
(select sum((O1."LineTotal"-((O1."LineTotal"*O2."DiscPrcnt")/100))) from RIN1  O1 Inner join ORIN O2 on O1."DocEntry"=O2."DocEntry" where O1."DocEntry" = a."DocEntry")
else
A."DiscSum" end as "Discount",


     ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = a."DocEntry" AND "staType" = -90 
     AND "RelateType" NOT IN (3,2)), 0) AS "BED", 
     
     ifnull((SELECT SUM("TaxSum") 
     FROM RIN4 WHERE "DocEntry" = a."DocEntry" 
     AND "staType" IN (SELECT * FROM CESS_staType) AND "RelateType" NOT IN (3,2)), 0) AS "Cess", 
     
     ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = a."DocEntry" AND "staType" = 7 
     AND "RelateType" NOT IN (3,2)), 0) AS "HeCess", 
     
     ifnull((SELECT SUM("TaxSum") 
     FROM RIN4 WHERE "DocEntry" = a."DocEntry" AND "staType" = 1 AND "RelateType" NOT IN (3,2)), 0) AS "VAT",
     
     ifnull((SELECT SUM("TaxSum") FROM RIN4 
      WHERE "DocEntry" = a."DocEntry" AND "staType" = 4 AND "RelateType" NOT IN (3,2)), 0) AS "CST", 
      
      case when ifnull(T8."ImpORExp",'')='N' then ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = a."DocEntry" AND "staType" = -120 
      AND "RelateType" NOT IN (3,2)), 0) 
      
      else
      ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = a."DocEntry" AND "staType" = -120 
      AND "RelateType" NOT IN (3,2)), 0) 
      end AS "IGST", 
      
      ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = a."DocEntry" AND "staType" = -100 
      AND "RelateType" NOT IN (3,2)), 0) AS "CGST", 
      
      ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = a."DocEntry" AND "staType" IN (-110,-150) 
      AND "RelateType" NOT IN (3,2)), 0) AS "SGST", A."VatSum" AS "Total Tax",
      
      
      
      case when ifnull(T8."ImpORExp",'')='N' then ifnull((SELECT SUM("LineTotal") 
      FROM RIN3 WHERE "DocEntry" = a."DocEntry"), 0) 
      else
      ifnull((SELECT SUM("TotalFrgn") 
      FROM RIN3 WHERE "DocEntry" = a."DocEntry"), 0) 
      end AS "Freight", 
      
      
      
      case when ifnull(T8."ImpORExp",'')='N' then  
      ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = a."DocEntry" 
      AND "RelateType" IN (3,2)), 0) 
      else
      ifnull((SELECT SUM("TaxSumFrgn") FROM RIN4 WHERE "DocEntry" = a."DocEntry" 
      AND "RelateType" IN (3,2)), 0) 
      end AS "F_Ttax",
      
      --ifnull((SELECT SUM("TaxSum") FROM RIN4 WHERE "DocEntry" = a."DocEntry"
      -- AND "staType" IN (SELECT * FROM TCS_statype) AND "RelateType" NOT IN (3,2)), 0) AS "OthChg",
		
		0 AS "OthChg",
       
       A."RoundDif",case when ifnull(T8."ImpORExp",'')='N' then A."DocTotal" else A."DocTotal" end AS "Net Value", A."DocTotalFC" AS "FC Value"


FROM ORIN A left OUTER JOIN RIN3 B ON A."DocEntry" = B."DocEntry" left OUTER JOIN RIN4 C 
ON A."DocEntry" = C."DocEntry" left outer join RIN3 h on A."DocEntry" = h."DocEntry" left outer join 
RIN1 d on a."DocEntry" = d."DocEntry" left outer join crd7 e on a."CardCode" = e."CardCode" Left Outer Join
oitm g On d."ItemCode"=g."ItemCode" left outer join ochp f on f."AbsEntry"= g."ChapterID" 
left join RIN12 T8 on A."DocEntry"=T8."DocEntry"
where A."DocEntry"=:DocEntry ;
end if;

end;


