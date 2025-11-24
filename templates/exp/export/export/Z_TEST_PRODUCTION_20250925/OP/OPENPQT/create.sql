CREATE VIEW "Z_TEST_PRODUCTION_20250925"."OPENPQT" ( "DOCENTRY", "DOCNUM", "DUEDATE", "DOCSTATUS", "PQTGRPNUM", "NUMATCARD", "SLPCODE", "SLPNAME", "CARDCODE", "CARDNAME", "BPGROUPCODE", "BPGROUPNAME", "ITEMCODE", "ITEMNAME", "ITMSGRPCOD", "ITMSGRPNAM", "CONTCTCODE", "CONTCTNAME", "CONTCTTEL", "PRICE" ) AS Select T0."DocEntry" as "DOCENTRY", T0."DocNum" as "DOCNUM",
 	 T0."DocDueDate" as "DUEDATE", T0."DocStatus" as "DOCSTATUS",
 	 T0."PQTGrpNum" as "PQTGRPNUM",IFNULL(T0."NumAtCard", '') "NUMATCARD",
 	 T0."SlpCode" as "SLPCODE", T6."SlpName" as "SLPNAME", 
 	 T0."CardCode" as "CARDCODE", IFNULL(T1."CardName", '') as "CARDNAME",
 	 T2."GroupCode" as "BPGROUPCODE", T2."GroupName" as "BPGROUPNAME",
 	 T3."ItemCode" as "ITEMCODE", T4."ItemName" as "ITEMNAME",
 	 T4."ItmsGrpCod" as "ITMSGRPCOD", T5."ItmsGrpNam" as "ITMSGRPNAM",
 	 T7."CntctCode" as "CONTCTCODE", T7."Name" as "CONTCTNAME",
 	 IFNULL(T7."Tel1", '') as "CONTCTTEL",
 	 T3."Price" as "PRICE" 
 	 From "OPQT" T0 
 	 INNER JOIN "OCRD" T1 On T0."CardCode" = T1."CardCode"
 	 INNER JOIN "OCRG" T2 On T1."GroupCode" = T2."GroupCode"
 	 INNER JOIN "PQT1" T3 On T0."DocEntry" = T3."DocEntry"
 	 LEFT JOIN "OITM" T4 On T3."ItemCode" = T4."ItemCode"
 	 LEFT JOIN "OITB" T5 On T4."ItmsGrpCod" = T5."ItmsGrpCod"
 	 INNER JOIN "OSLP" T6 On T0."SlpCode" = T6."SlpCode"
 	 LEFT OUTER JOIN "OCPR" T7 On T0."CntctCode" = T7."CntctCode" WITH READ ONLY