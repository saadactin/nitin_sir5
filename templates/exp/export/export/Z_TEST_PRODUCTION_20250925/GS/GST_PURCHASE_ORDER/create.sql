CREATE VIEW "Z_TEST_PRODUCTION_20250925"."GST_PURCHASE_ORDER" ( "Docentry", "Docnum", "DocCur", "Docseries", "Docdate", "CardCode", "Purchase No", "VName", "VendorAdd", "V_CNCTP_N", "V_mobileNo", "V_CnctP_E", "Block", "Building", "Street", "City", "ZipCode", "country", "Street No_Vendor", "STATE_Vendor", "VShipGSTNo", "VShipGSTType", "SupRefNo", "SupDate", "PR No", "PR Date", "DeliDate", "Deli_Mode", "Deli_Addr", "Deli_GST", "Deli_GSTType", "BuyerName", "DeilName", "SalesPrsn", "salesmob", "SalesEmail", "CnctPrsnEmail", "LineNum", "ItemCode", "Dscription", "U_POAddDesc", "HSN Code", "Service_SAC_Code", "Quantity", "unitMsr", "PriceBefDi", "DiscPrcnt", "TotalAmt", "ItmDiscAmt", "DocDiscAmt", "DiscAmt", "Price", "LineTotal", "Total", "TotalAsseble", "CGSTRate", "CGST", "SGSTRate", "SGST", "IGSTRate", "IGST", "TCSRate", "TCS", "DocTotal", "RoundDif", "Currencyname", "Hundredthname", "Payment Terms", "Remark", "Opening Remark", "Closing Remark", "PrjName", "ShipDate", "Cellolar", "loc_stat", "loc_stat_nam", "Freght remark", "F1_LINE", "F1", "F2_LINE", "F2", "F3_LINE", "F3", "F4_LINE", "F4", "F5_LINE", "F5", "Freight_Cgst", "Freight_Sgst", "Freight_Igst", "Freight_TCS", "Vendor_PAN", "Vendor_GSTIN", "Vendor_State", "Vendor_StaCode", "Revision_No.", "Del_Method" ) AS SELECT
	 POR."DocEntry" AS "Docentry",
	 ifnull(nm1."SeriesName"||'/',
	 '')||cast(POR."DocNum" as nvarchar) AS "Docnum",
	 POR."DocCur",
	 NM1."SeriesName" AS "Docseries",
	 POR."DocDate" AS "Docdate",
	 POR."CardCode",
	 (CASE WHEN nm1."BeginStr" IS NULL 
	THEN ifnull(NM1."BeginStr",
	 '') 
	ELSE ifnull(NM1."BeginStr",
	 '') 
	END || (CASE WHEN nm1."EndStr" IS NULL 
		THEN ifnull(NM1."EndStr",
	 '') 
		ELSE (ifnull(NM1."EndStr",
	 '')) 
		END)|| RTRIM(LTRIM(CAST(POR."DocNum" AS char(20)))) ) AS "Purchase No",
	 POR."CardName" AS "VName",
	 POR."Address" AS "VendorAdd",
	 CPR."Name" AS "V_CNCTP_N",
	 cpr."Cellolar" AS "V_mobileNo",
	 CPR."E_MailL" AS "V_CnctP_E",
	 "VShipFrom"."Block",
	 "VShipFrom"."Building",
	 "VShipFrom"."Street",
	 "VShipFrom"."City",
	 "VShipFrom"."ZipCode",
	 (SELECT
	 DISTINCT "Name" 
	FROM OCRY 
	WHERE "Code" = "VShipFrom"."Country" ) AS "country",
	 "VShipFrom"."StreetNo" AS "Street No_Vendor",
	 (SELECT
	 DISTINCT "Name" 
	FROM OCST 
	WHERE "Code" = "VShipFrom"."State" 
	AND "VShipFrom1"."Country" = OCST."Country") AS "STATE_Vendor",
	 "VShipFrom1"."GSTRegnNo" AS "VShipGSTNo",
	 GTY2."GSTType" AS "VShipGSTType",
	 POR."NumAtCard" AS "SupRefNo",
	 '' AS "SupDate",
	 (SELECT
	 distinct (Cast(OPRQ."DocNum" AS CHAR(7))) 
	FROM OPRQ 
	inner join POR1 on OPRQ."DocEntry"=POR1."BaseEntry" 
	and POR1."DocEntry"=POR."DocEntry" 
	left outer join NNM1 on NNM1."Series"=OPRQ."Series" ) "PR No" ,
	 (SELECT
	 Distinct TO_VARCHAR(OPRQ."DocDate",
	 'DD-MM-YYYY') 
	FROM OPRQ 
	inner join POR1 on OPRQ."DocEntry"=POR1."BaseEntry" 
	where POR1."DocEntry" =POR."DocEntry") "PR Date",
	 POR."DocDueDate" AS "DeliDate",
	 SHP."TrnspName" AS "Deli_Mode",
	 POR."Address2" AS "Deli_Addr",
	 LCT."GSTRegnNo" AS "Deli_GST",
	 GTY."GSTType" AS "Deli_GSTType",
	 POR."PayToCode" AS "BuyerName",
	 POR."ShipToCode" AS "DeilName",
	 CASE WHEN SLP."SlpName" = '-No Sales Employee-' 
THEN '' 
ELSE SLP."SlpName" 
END AS "SalesPrsn",
	 SLP."Mobil" AS "salesmob",
	 SLP."Email" AS "SalesEmail",
	 CPR."E_MailL" AS "CnctPrsnEmail",
	 PR1."LineNum",
	 PR1."ItemCode",
	 PR1."Dscription",
	 PR1."U_POAddDesc",
	 (CASE WHEN ITM."ItemClass" = 1 
	THEN (SELECT
	 "ServCode" 
		FROM OSAC 
		WHERE "AbsEntry" = (CASE WHEN PR1."HsnEntry" IS NULL 
			THEN ITM."SACEntry" 
			ELSE PR1."HsnEntry" 
			END)) WHEN ITM."ItemClass" = 2 
	THEN (SELECT
	 "ChapterID" 
		FROM OCHP 
		WHERE "AbsEntry" = (CASE WHEN PR1."HsnEntry" IS NULL 
			THEN ITM."ChapterID" 
			ELSE PR1."HsnEntry" 
			END)) 
	ELSE '' 
	END) AS "HSN Code",
	 (SELECT
	 "ServCode" 
	FROM OSAC 
	WHERE "AbsEntry" = PR1."SacEntry") AS "Service_SAC_Code",
	 PR1."Quantity",
	 PR1."unitMsr",
	 PR1."PriceBefDi",
	 PR1."DiscPrcnt",
	 (PR1."Quantity" * PR1."PriceBefDi") AS "TotalAmt",
	 ((PR1."PriceBefDi" - PR1."Price") * PR1."Quantity") AS "ItmDiscAmt",
	 ((CASE WHEN OCRN."CurrCode" = 'INR' 
		THEN PR1."LineTotal" 
		ELSE PR1."TotalFrgn" 
		END) * (POR."DiscPrcnt" / 100)) AS "DocDiscAmt",
	 CASE WHEN POR."DiscPrcnt" = 0 
THEN ((PR1."PriceBefDi" - PR1."Price") * PR1."Quantity") 
ELSE ((CASE WHEN OCRN."CurrCode" = 'INR' 
		THEN PR1."LineTotal" 
		ELSE PR1."TotalFrgn" 
		END) * (POR."DiscPrcnt" / 100)) 
END AS "DiscAmt",
	 PR1."Price",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
THEN PR1."LineTotal" 
ELSE PR1."TotalFrgn" 
END AS "LineTotal",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
THEN (CASE WHEN POR."DiscPrcnt" = 0 
	THEN PR1."LineTotal" 
	ELSE (PR1."LineTotal" - (PR1."LineTotal" * POR."DiscPrcnt" / 100)) 
	END) 
ELSE (CASE WHEN POR."DiscPrcnt" = 0 
	THEN PR1."TotalFrgn" 
	ELSE (PR1."TotalFrgn" - (PR1."TotalFrgn" * POR."DiscPrcnt" / 100)) 
	END) 
END AS "Total",
	 CASE WHEN PR1."AssblValue" = 0 
THEN (CASE WHEN POR."DiscPrcnt" = 0 
	THEN (CASE WHEN OCRN."CurrCode" = 'INR' 
		THEN PR1."LineTotal" 
		ELSE PR1."TotalFrgn" 
		END) 
	ELSE ((CASE WHEN OCRN."CurrCode" = 'INR' 
			THEN PR1."LineTotal" 
			ELSE PR1."TotalFrgn" 
			END) - ((CASE WHEN OCRN."CurrCode" = 'INR' 
				THEN PR1."LineTotal" 
				ELSE PR1."TotalFrgn" 
				END) * POR."DiscPrcnt" / 100)) 
	END) 
ELSE (PR1."AssblValue" * PR1."Quantity") 
END AS "TotalAsseble",
	 CGST."TaxRate" AS "CGSTRate",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
THEN CGST."TaxSum" 
ELSE CGST."TaxSumFrgn" 
END AS "CGST",
	 SGST."TaxRate" AS "SGSTRate",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
THEN SGST."TaxSum" 
ELSE SGST."TaxSumFrgn" 
END AS "SGST",
	 IGST."TaxRate" AS "IGSTRate",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
THEN IGST."TaxSum" 
ELSE IGST."TaxSumFrgn" 
END AS "IGST",
	 -----------------------------------TCS
 TCS."TaxRate" AS "TCSRate",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
THEN TCS."TaxSum" 
ELSE TCS."TaxSumFrgn" 
END AS "TCS",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
THEN POR."DocTotal" 
else por."DocTotalFC" 
end "DocTotal",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
THEN POR."RoundDif" 
ELSE POR."RoundDifFC" 
END AS "RoundDif",
	 OCRN."CurrName" AS "Currencyname",
	 OCRN."F100Name" AS "Hundredthname",
	 OCT."PymntGroup" AS "Payment Terms",
	 POR."Comments" AS "Remark",
	 POR."Header" AS "Opening Remark",
	 POR."Footer" AS "Closing Remark",
	 PRJ."PrjName" AS "PrjName",
	 PR1."ShipDate" AS "ShipDate",
	 CPR."Cellolar" ,
	 cst."GSTCode" "loc_stat",
	 cst."Name" "loc_stat_nam" ,
	 (select
	 DISTINCT "Comments" 
	from por3 
	where "DocEntry"=por."DocEntry" 
	and "ExpnsCode"=3) "Freght remark" ,
	 -----------------------	========================
 CASE WHEN OCRN."CurrCode" = 'INR' 
then (select
	 "LineTotal" 
	from POR3 
	where "ExpnsCode"=1 
	and "DocEntry"=POR."DocEntry") 
else (select
	 "TotalFrgn" 
	from POR3 
	where "ExpnsCode"=1 
	and "DocEntry"=POR."DocEntry") 
end F1_Line,
	 (select
	 "ExpnsName" 
	from OEXD 
	where "ExpnsCode"=1) F1 ,
	 CASE WHEN OCRN."CurrCode" = 'INR' 
then (select
	 "LineTotal" 
	from POR3 
	where "ExpnsCode"=2 
	and "DocEntry"=POR."DocEntry") 
else (select
	 "TotalFrgn" 
	from POR3 
	where "ExpnsCode"=2 
	and "DocEntry"=POR."DocEntry") 
end F2_Line,
	 (select
	 "ExpnsName" 
	from OEXD 
	where "ExpnsCode"=2) F2 ,
	 CASE WHEN OCRN."CurrCode" = 'INR' 
then (select
	 "LineTotal" 
	from POR3 
	where "ExpnsCode"=3 
	and "DocEntry"=POR."DocEntry") 
else (select
	 "TotalFrgn" 
	from POR3 
	where "ExpnsCode"=3 
	and "DocEntry"=POR."DocEntry") 
end F3_Line,
	 (select
	 "ExpnsName" 
	from OEXD 
	where "ExpnsCode"=3) F3 ,
	 CASE WHEN OCRN."CurrCode" = 'INR' 
then (select
	 "LineTotal" 
	from POR3 
	where "ExpnsCode"=4 
	and "DocEntry"=POR."DocEntry") 
else (select
	 "TotalFrgn" 
	from POR3 
	where "ExpnsCode"=4 
	and "DocEntry"=POR."DocEntry") 
end F4_Line,
	 (select
	 "ExpnsName" 
	from OEXD 
	where "ExpnsCode"=4) F4 ,
	 CASE WHEN OCRN."CurrCode" = 'INR' 
then (select
	 "LineTotal" 
	from POR3 
	where "ExpnsCode"=5 
	and "DocEntry"=POR."DocEntry") 
else (select
	 "TotalFrgn" 
	from POR3 
	where "ExpnsCode"=5 
	and "DocEntry"=POR."DocEntry") 
end F5_Line,
	 (select
	 "ExpnsName" 
	from OEXD 
	where "ExpnsCode"=5) F5,
	 CASE WHEN OCRN."CurrCode" = 'INR' 
then (select
	 sum("TaxSum") 
	from POR4 
	where "ExpnsCode" in (1,
	 2,
	 3,
	 4,
	 5) 
	and "DocEntry"=POR."DocEntry" 
	and "staType"=-100 
	and "RelateType" in (2,
	 3)) 
else (select
	 sum("TaxSumFrgn") 
	from POR4 
	where "ExpnsCode" in (1,
	 2,
	 3,
	 4,
	 5) 
	and "DocEntry"=POR."DocEntry" 
	and "staType"=-100 
	and "RelateType" in (2,
	 3)) 
end "Freight_Cgst",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
then (select
	 sum("TaxSum") 
	from POR4 
	where "ExpnsCode" in (1,
	 2,
	 3,
	 4,
	 5) 
	and "DocEntry"=POR."DocEntry" 
	and "staType"=-110 
	and "RelateType" in (2,
	 3)) 
else (select
	 sum("TaxSumFrgn") 
	from POR4 
	where "ExpnsCode" in (1,
	 2,
	 3,
	 4,
	 5) 
	and "DocEntry"=POR."DocEntry" 
	and "staType"=-110 
	and "RelateType" in (2,
	 3)) 
end "Freight_Sgst",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
then (select
	 sum("TaxSum") 
	from POR4 
	where "ExpnsCode" in (1,
	 2,
	 3,
	 4,
	 5) 
	and "DocEntry"=POR."DocEntry" 
	and "staType"=-120 
	and "RelateType" in (2,
	 3)) 
else (select
	 sum("TaxSumFrgn") 
	from POR4 
	where "ExpnsCode" in (1,
	 2,
	 3,
	 4,
	 5) 
	and "DocEntry"=POR."DocEntry" 
	and "staType"=-120 
	and "RelateType" in (2,
	 3)) 
end "Freight_Igst",
	 CASE WHEN OCRN."CurrCode" = 'INR' 
then (select
	 sum("TaxSum") 
	from POR4 
	where "ExpnsCode" in (1,
	 2,
	 3,
	 4,
	 5) 
	and "DocEntry"=POR."DocEntry" 
	and "staType"=(select
	 * 
		from TCS_STATYPE) 
	and "RelateType" in (2,
	 3)) 
else (select
	 sum("TaxSumFrgn") 
	from POR4 
	where "ExpnsCode" in (1,
	 2,
	 3,
	 4,
	 5) 
	and "DocEntry"=POR."DocEntry" 
	and "staType"=(select
	 * 
		from TCS_STATYPE) 
	and "RelateType" in (2,
	 3)) 
end "Freight_TCS" ,
	 (select
	 distinct c7."TaxId0" 
	from CRD7 c7 
	where c7."AddrType"='S' 
	and c7."CardCode"= POR."CardCode" 
	and c7."Address" is not null 
	and c7."Address" =POR."ShipToCode")as "Vendor_PAN" ,
	 (select
	 max(c1."GSTRegnNo") 
	from CRD1 c1 
	where c1."CardCode"=CRD."CardCode" 
	and c1."AdresType"='B') as "Vendor_GSTIN" ,
	 (select
	 "Name" 
	from OCST OC 
	where OC."Code" =(select
	 Distinct C1."State" 
		from CRD1 c1 
		where c1."CardCode"=POR."CardCode" 
		and c1."AdresType"='B' 
		and c1."Address" = POR."PayToCode" 
		and C1."Country"= (select
	 Distinct c2."Country" 
			from CRD1 c2 
			where c2."CardCode"=POR."CardCode" 
			and c2."AdresType"='B' 
			and c2."Address" = POR."PayToCode" )) 
	and OC."Country"= (select
	 Distinct c2."Country" 
		from CRD1 c2 
		where c2."CardCode"=POR."CardCode" 
		and c2."AdresType"='B' 
		and c2."Address" = POR."PayToCode" )) as "Vendor_State" --,(select Distinct C1."State" from CRD1 c1 where c1."CardCode"=POR."CardCode"   and c1."AdresType"='B' and  c1."Address" = POR."PayToCode" and C1."Country"= (select Distinct c2."Country" from CRD1 c2 where c2."CardCode"=POR."CardCode" and c2."AdresType"='B' and c2."Address" = POR."PayToCode"  )) as "State"
,
	 ( select
	 OC."GSTCode" 
	from OCST OC 
	where OC."Code"=(select
	 Distinct C1."State" 
		from CRD1 c1 
		where c1."CardCode"=POR."CardCode" 
		and c1."AdresType"='B' 
		and c1."Address" = POR."PayToCode" 
		and C1."Country"= (select
	 Distinct c2."Country" 
			from CRD1 c2 
			where c2."CardCode"=POR."CardCode" 
			and c2."AdresType"='B' 
			and c2."Address" = POR."PayToCode" )) 
	and OC."Country"= (select
	 Distinct c2."Country" 
		from CRD1 c2 
		where c2."CardCode"=POR."CardCode" 
		and c2."AdresType"='B' 
		and c2."Address" = POR."PayToCode" )) as "Vendor_StaCode",POR."U_PoRevN" As "Revision_No.",
	IFNULL((SELECT "Descr" FROM UFD1 Where "FieldID" = 75 and "TableID"='OPOR' and "FldValue"=POR."U_DevMet"),POR."U_DevMet") As "Del_Method"
		 --,(select Distinct c2."Country" from CRD1 c2 where c2."CardCode"=POR."CardCode" and c2."AdresType"='B' and c2."Address" = POR."PayToCode" )  as "Country"
 
FROM OPOR POR 
INNER JOIN POR1 PR1 ON PR1."DocEntry" = POR."DocEntry" 
INNER JOIN NNM1 NM1 ON POR."Series" = NM1."Series" 
LEFT OUTER JOIN OCRD CRD ON por."CardCode" = crd."CardCode" --Left OUTER Join CRD7 c7 on CRD."CardCode" = c7."CardCode" and c7."AddrType"='S'
 
LEFT OUTER JOIN (SELECT
	 * 
	FROM CRD1) AS "VShipFrom" ON "VShipFrom"."Address" = por."ShipToCode" 
AND "VShipFrom"."CardCode" = POR."CardCode" 
AND "VShipFrom"."AdresType" = 'S' 
LEFT OUTER JOIN (SELECT
	 * 
	FROM CRD1) AS "VShipFrom1" ON "VShipFrom1"."Address" = CRD."ShipToDef" 
AND "VShipFrom1"."CardCode" = POR."CardCode" 
AND "VShipFrom1"."AdresType" = 'S' 
LEFT OUTER JOIN OGTY GTY2 ON "VShipFrom1"."GSTType" = GTY2."AbsEntry" 
INNER JOIN OSLP SLP ON POR."SlpCode" = SLP."SlpCode" 
LEFT OUTER JOIN OSHP SHP ON SHP."TrnspCode" = POR."TrnspCode" 
LEFT OUTER JOIN OLCT LCT ON PR1."LocCode" = LCT."Code" 
LEFT OUTER JOIN OCST CST ON CST."Code" = LCT."State" 
AND CST."Country" = LCT."Country" 
LEFT OUTER JOIN OGTY GTY ON LCT."GSTType" = GTY."AbsEntry" 
LEFT OUTER JOIN OCRN ON POR."DocCur" = OCRN."CurrCode" 
LEFT OUTER JOIN OCTG OCT ON POR."GroupNum" = OCT."GroupNum" 
LEFT OUTER JOIN POR12 PR12 ON PR12."DocEntry" = POR."DocEntry" 
LEFT OUTER JOIN OCST CST1 ON CST1."Code" = PR12."StateS" 
AND CST1."Country" = PR12."CountryS" 
LEFT OUTER JOIN OCPR CPR ON POR."CardCode" = CPR."CardCode" 
AND POR."CntctCode" = CPR."CntctCode" 
LEFT OUTER JOIN OITM ITM ON ITM."ItemCode" = PR1."ItemCode" 
LEFT OUTER JOIN POR4 CGST ON PR1."DocEntry" = CGST."DocEntry" 
AND CGST."staType" IN (-100) 
AND PR1."LineNum" = CGST."LineNum" 
AND CGST."RelateType" = 1 
LEFT OUTER JOIN POR4 SGST ON PR1."DocEntry" = SGST."DocEntry" 
AND SGST."staType" IN (-110) 
AND PR1."LineNum" = SGST."LineNum" 
AND SGST."RelateType" = 1 
LEFT OUTER JOIN POR4 IGST ON PR1."DocEntry" = IGST."DocEntry" 
AND IGST."staType" IN (-120) 
AND PR1."LineNum" = IGST."LineNum" 
AND IGST."RelateType" = 1 ---------------------------TCS
 
LEFT OUTER JOIN POR4 TCS ON PR1."DocEntry" = TCS."DocEntry" 
AND PR1."LineNum" = TCS."LineNum" 
AND TCS."staType" IN (select
	 * 
	from TCS_STATYPE) 
AND TCS."RelateType" = 1 
LEFT OUTER JOIN OPRJ PRJ ON PRJ."PrjCode" = POR."Project" order by Pr1."VisOrder" WITH READ ONLY