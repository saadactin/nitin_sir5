CREATE PROCEDURE "EWAY_SHIPING_DETAILS"
  
  (
 
  In DocEntry	 integer, 
    In FrmTyp	 integer
    )

  LANGUAGE SQLSCRIPT  
  SQL SECURITY INVOKER
  AS  
BEGIN


if :FrmTyp=133 then

SELECT 
case when ifnull(O3."ImpORExp", 'N')='N' then ifnull(O1."GSTRegnNo", '') else 'URP'  end AS "GSTN",
 O2."CardName", 
 ifnull((SELECT "GSTCode" FROM OCST WHERE "Code" = ifnull(O3."BpStateCod", '')
  and  "Country"=ifnull(O3."BpCountry",'')), '') AS "POS",
  ifnull(O3."ImpORExp", 'N') AS "Export",
   CAST(ifnull(O1."Block", '') AS nvarchar) || ' ' || CAST(ifnull(O1."Building", '') AS nvarchar) || ' ' ||
    CAST(ifnull(O1."StreetNo", '') AS nvarchar) AS "Address1", 
   ifnull(O1."City", '') || ',' || ifnull(O4."Name", '') || ',' || ifnull(O5."Name", '') AS "Address2",
    ifnull(O1."City", '') AS "Location", ifnull(Replace(CAST(O1."ZipCode" AS nvarchar), ' ', ''), '') AS "PinCode", 
   case when ifnull(O3."ImpORExp", 'N')='N' then ifnull(O4."GSTCode", '') else '96' end AS "stateCode", 
    
    substring((REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
       REPLACE(REPLACE(REPLACE(REPLACE( ifnull(O6."Phone1", ''),
        '!',''),'@',''),'+',''),'$',''),'%',''),
        '^',''),'&',''),'*',''),' ',''),'-',''),'/','')),0,11) AS "Phone", 

    ifnull(O6."E_Mail", '') AS "Email" 
    FROM CRD1 O1 INNER JOIN OINV O2 ON O1."CardCode" = O2."CardCode" 
    --and (ifnull(O2."UseBilAddr",'N') = 'N' and O1."Address"=O2."ShipToCode") or (ifnull(O2."UseBilAddr",'N') = 'Y' and O1."Address"=O2."PayToCode")
    INNER JOIN INV12 O3 ON O2."DocEntry" = O3."DocEntry" 
    LEFT OUTER JOIN OCST O4 ON O1."State" = O4."Code" and  O3."BpCountry"=O4."Country"
    LEFT OUTER JOIN OCRY O5 ON O1."Country" = O5."Code" 
    INNER JOIN OCRD O6 ON O2."CardCode" = O6."CardCode" 
   
    WHERE O2."DocEntry"=:DocEntry and   O1."Address"=O2."ShipToCode";
   -- ((ifnull(O2."UseBilAddr",'N') = 'N' AND O1."AdresType" = 'S') OR (ifnull(O2."UseBilAddr",'N') = 'Y' AND O1."AdresType" = 'B'));
	end if;
	end;


