CREATE VIEW "Z_TEST_PRODUCTION_20250925"."COMPANY_ADDR" ( "Address" ) AS SELECT
	 ifnull("Building" || ' ',
	 ',') || ifnull("Block" || ' ',
	 '') || ifnull("StreetNo" || ' ',
	 '') || ifnull("Street" || ' ',
	 '') || ifnull("City" || ' - ',	 '') 
 || ifnull("ZipCode" || ' ',
	 '') || ifnull((SELECT
	 "Name" 
		FROM OCST 
		WHERE "Code" = "State") || ',',
	 '') || ifnull(OCRY."Name" || '.',
	 '') AS "Address" 
FROM ADM1 
LEFT OUTER JOIN OCRY ON OCRY."Code" = ADM1."Country" WITH READ ONLY