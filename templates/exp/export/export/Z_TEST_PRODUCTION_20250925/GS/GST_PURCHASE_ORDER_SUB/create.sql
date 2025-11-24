CREATE VIEW "Z_TEST_PRODUCTION_20250925"."GST_PURCHASE_ORDER_SUB" ( "ExpnsCode", "CEX", "CSAC", "ISAC", "DocEntry", "IEX", "CGSTBASESUM", "CGSTTAXSUM", "CGSTTAXRATE", "SGSTBASESUM", "SGSTRATE", "SGSTTAXSUM", "IGSTBASESUM", "IGSTRATE", "IGSTTAXSUM", "TCSBASESUM", "TCSRATE", "TCSBASESUMFRG", "TCSTAXSUM" ) AS SELECT
	 C."ExpnsCode",
	 C."ExpnsName" AS "CEX",
	 C."SacCode" AS "CSAC",
	 I."SacCode" AS "ISAC",
	 POR."DocEntry",
	 I."ExpnsName" AS "IEX",
	 CASE WHEN POR."DocCur" = 'INR' 
THEN CGSTBASESUM 
ELSE CGSTBASESUMFRG 
END AS "CGSTBASESUM",
	 CASE WHEN POR."DocCur" = 'INR' 
THEN CGSTTAXSUM 
ELSE CGSTTAXSUMFRG 
END AS "CGSTTAXSUM",
	 CGSTTAXRATE,
	 CASE WHEN POR."DocCur" = 'INR' 
THEN SGSTBASESUM 
ELSE SGSTBASESUMFRG 
END AS "SGSTBASESUM",
	 SGSTRATE,
	 CASE WHEN POR."DocCur" = 'INR' 
THEN SGSTTAXSUM 
ELSE SGSTTAXSUMFRG 
END AS "SGSTTAXSUM",
	 CASE WHEN POR."DocCur" = 'INR' 
THEN IGSTBASESUM 
ELSE IGSTBASESUMFRGN 
END AS "IGSTBASESUM",
	 IGSTRATE,
	 CASE WHEN POR."DocCur" = 'INR' 
THEN IGSTTAXSUM 
ELSE IGSTTAXSUMFRG 
END AS "IGSTTAXSUM" -------------------TCS
 ,
	TCSBASESUM AS "TCSBASESUM",
	 TCSRATE,
	 TCSBASESUMFRG,
	 CASE WHEN POR."DocCur" = 'INR' 
THEN TCSTAXSUM 
ELSE TCSTAXSUMFRG 
END AS "TCSTAXSUM" 
FROM OPOR POR 
LEFT OUTER JOIN (SELECT
	 DISTINCT i4."DocEntry",
	 ox."ExpnsCode",
	 ox."ExpnsName",
	 ox."SacCode",
	 MAX(i4."TaxRate") AS "IGSTRATE",
	 SUM(i4."TaxSum") AS "IGSTTAXSUM",
	 SUM(i4."TaxSumFrgn") AS "IGSTTAXSUMFRG",
	 SUM(i4."BaseSum") AS "IGSTBASESUM",
	 SUM(i4."BaseSumFrg") AS "IGSTBASESUMFRGN" 
	FROM POR4 i4 
	LEFT OUTER JOIN OEXD ox ON ox."ExpnsCode" = i4."ExpnsCode" 
	WHERE "RelateType" IN (2,
	3) 
	AND "staType" = -120 
	GROUP BY ox."ExpnsCode",
	 ox."ExpnsName",
	 i4."DocEntry",
	 ox."SacCode") AS I ON i."DocEntry" = POR."DocEntry" 
LEFT OUTER JOIN (SELECT
	 DISTINCT i4."DocEntry",
	 ox."ExpnsCode",
	 ox."ExpnsName",
	 ox."SacCode",
	 MAX(i4."TaxRate") AS "CGSTTAXRATE",
	 SUM(i4."TaxSum") AS "CGSTTAXSUM",
	 SUM(i4."TaxSumFrgn") AS "CGSTTAXSUMFRG",
	 SUM(i4."BaseSum") AS "CGSTBASESUM",
	 SUM(i4."BaseSumFrg") AS "CGSTBASESUMFRG" 
	FROM POR4 i4 
	LEFT OUTER JOIN OEXD ox ON ox."ExpnsCode" = i4."ExpnsCode" 
	WHERE "RelateType" IN (2,
	3) 
	AND "staType" = -100 
	GROUP BY ox."ExpnsCode",
	 ox."ExpnsName",
	 i4."DocEntry",
	 ox."SacCode") AS C ON POR."DocEntry" = c."DocEntry" 
LEFT OUTER JOIN (SELECT
	 DISTINCT i4."DocEntry",
	 ox."ExpnsCode",
	 ox."ExpnsName",
	 MAX(i4."TaxRate") AS "SGSTRATE",
	 SUM(i4."TaxSum") AS "SGSTTAXSUM",
	 SUM(i4."TaxSumFrgn") AS "SGSTTAXSUMFRG",
	 SUM(i4."BaseSum") AS "SGSTBASESUM",
	 SUM(i4."BaseSumFrg") AS "SGSTBASESUMFRG" 
	FROM POR4 i4 
	LEFT OUTER JOIN OEXD ox ON ox."ExpnsCode" = i4."ExpnsCode" 
	WHERE "RelateType" IN (2,
	3) 
	AND "staType" = -110 
	GROUP BY ox."ExpnsCode",
	 ox."ExpnsName",
	 i4."DocEntry") AS S ON c."ExpnsCode" = s."ExpnsCode" 
AND s."DocEntry" = POR."DocEntry" -------------------------TCS------------------------------------

LEFT OUTER JOIN (SELECT
	 DISTINCT i4."DocEntry",
	ox."ExpnsCode",
	ox."ExpnsName",
	 MAX(i4."TaxRate") AS "TCSRATE",
	SUM(i4."TaxSum") AS "TCSTAXSUM",
	SUM(i4."TaxSumFrgn") AS "TCSTAXSUMFRG",
	 SUM(i4."BaseSum") AS "TCSBASESUM",
	 SUM(i4."BaseSumFrg") AS "TCSBASESUMFRG" 
	FROM POR4 i4 
	LEFT OUTER JOIN OEXD ox ON ox."ExpnsCode" = i4."ExpnsCode" 
	WHERE "RelateType" IN (2,
	3) 
	AND "staType" = (select
	 * 
		from TCS_STATYPE) 
	GROUP BY ox."ExpnsCode",
	ox."ExpnsName",
	 i4."DocEntry") AS tcS ON tcs."DocEntry" = POR."DocEntry" WITH READ ONLY