CREATE VIEW "Z_TEST_PRODUCTION_20250925"."B1_CashFlowForecastDueDateDocumentsView" ( "DocEntry", "DocType", "DocNum", "CardCode", "CtlAccount", "TransId", "JrnlMemo", "ObjType", "OriginalType", "CreateDate", "TaxDate", "ClsDate", "PayDuMonth", "ExtraMonth", "ExtraDays", "TolDays", "GroupNum", "InstID", "InstNum", "InstMonth", "InstDays", "InstPrcnt", "Installmnt", "DocDueDate", "Debit", "Credit", "Group", "PredictFlag", "AvrageLate", "HldCode" ) AS (((((((SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum", 
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt", 
			 T0."Installmnt",
	         T0."DocDueDate", 
	         CASE T2."NegAmount" 
				WHEN 'N' THEN
					0
				WHEN 'Y' THEN
					0 - (T0."DocTotal" - T0."PaidToDate")
			 END								AS "Debit", 
			 CASE T2."NegAmount" 
				WHEN 'N' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN 'Y' THEN
					0
			 END								AS "Credit",
			 'SF'								AS "Group",
			 0									AS "PredictFlag",
			 T1."AvrageLate",
			 IFNULL(T1."HldCode", '')			AS "HldCode"
	    FROM "ORDN" T0
  INNER JOIN "OCRD" T1
		  ON T1."CardCode" = T0."CardCode" 
   LEFT JOIN "OADM" T2
		  ON T2."Code" = T2."Code"
	   WHERE T0."CANCELED" <> 'Y'
	     AND T0."DocStatus" = 'O') UNION ALL (SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T2."DueDate"						AS "DocDueDate", 
	         T2."InsTotal" - T2."PaidToDate"	AS "Debit", 
	         0									AS "Credit",
			 'SF'								AS "Group",
			 0									AS "PredictFlag",
			 T1."AvrageLate",
			 IFNULL(T1."HldCode", '')			AS "HldCode"
	    FROM "ODPI" T0
  INNER JOIN "OCRD" T1
		  ON T1."CardCode" = T0."CardCode" 
  INNER JOIN "DPI6" T2
		  ON T2."DocEntry" = T0."DocEntry"
	   WHERE T0."CANCELED" <> 'Y'
	     AND T0."DocStatus" = 'O'
		 AND T0."Posted" = 'N')) UNION ALL (SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T0."DocDueDate", 
	         0 AS "Debit", 
	         T0."DocTotal" - T0."PaidToDate"	AS "Credit",
			 'EF'								AS "Group",
			 0									AS "PredictFlag",
			 T2."AvrageLate",
			 IFNULL(T1."HldCode", '')			AS "HldCode"
	    FROM "OPDN" T0
   LEFT JOIN "OADM" T1
		  ON T1."Code" = T1."Code"
  INNER JOIN "OCRD" T2
		  ON T2."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> 'Y'
	     AND T0."DocStatus" = 'O')) UNION ALL (SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T0."DocDueDate",
			 CASE T1."NegAmount" 
				WHEN 'N' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN 'Y' THEN
					0
			 END								AS "Debit", 
			 CASE T1."NegAmount" 
				WHEN 'N' THEN
					0
				WHEN 'Y' THEN
					0 - (T0."DocTotal" - T0."PaidToDate")
			 END								AS "Credit",
			 'EF'								AS "Group",
			 0									AS "PredictFlag",
			 T2."AvrageLate",
			 IFNULL(T1."HldCode", '')			AS "HldCode"
	    FROM "ORPD" T0
   LEFT JOIN "OADM" T1
		  ON T1."Code" = T1."Code"
  INNER JOIN "OCRD" T2
		  ON T2."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> 'Y'
	     AND T0."DocStatus" = 'O')) UNION ALL (SELECT T0."DocEntry", 
	  		 T0."ObjType"						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo", 
	         T0."ObjType", 
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T3."DueDate"						AS "DocDueDate", 
	         0									AS "Debit", 
	         T3."InsTotal" - T3."PaidToDate"	AS "Credit",
			 'EF'								AS "Group",
			 0									AS "PredictFlag",
			 T2."AvrageLate",
			 IFNULL(T1."HldCode", '')			AS "HldCode"
	    FROM "ODPO" T0
   LEFT JOIN "OADM" T1
		  ON T1."Code" = T1."Code"
  INNER JOIN "OCRD" T2
		  ON T2."CardCode" = T0."CardCode" 
  INNER JOIN "DPO6" T3
		  ON T3."DocEntry" = T0."DocEntry"
	   WHERE T0."CANCELED" <> 'Y'
	     AND T0."DocStatus" = 'O'
		 AND T0."Posted" = 'N')) UNION ALL (SELECT T0."DocEntry", 
	  		 '112'							AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || 'd'				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",  
			 T0."Installmnt",
	         T0."DocDueDate", 
			 CASE	T0."ObjType" 
				WHEN '20' THEN
					0
				WHEN '21' THEN
					CASE T2."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '19' THEN
					CASE T2."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '16' THEN
					CASE T2."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN '14' THEN
					CASE T2."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
			 END								AS "Debit",
			 CASE	T0."ObjType"
				WHEN '20' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '21' THEN
					CASE T2."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN '19' THEN
					CASE T2."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN '16' THEN
					CASE T2."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '14' THEN
					CASE T2."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN '20' THEN
					'EF'
				WHEN '21' THEN
					'EF'
				WHEN '19' THEN
					'EF'
				WHEN '16' THEN
					'SF'
				WHEN '14' THEN
					'SF'
			 END								AS "Group",
			 0									AS "PredictFlag",
			 T3."AvrageLate",
			 CASE T0."ObjType"
				WHEN '20' THEN
					IFNULL(T2."HldCode", '')
				WHEN '21' THEN
					IFNULL(T2."HldCode", '')
				WHEN '19' THEN
					IFNULL(T2."HldCode", '')
				WHEN '16' THEN
					IFNULL(T3."HldCode", '')
				WHEN '14' THEN
					IFNULL(T3."HldCode", '')
			 END								AS "HldCode"
	    FROM "ODRF" T0
   LEFT JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = 'N'
   LEFT JOIN "OADM" T2
		  ON T2."Code" = T2."Code"
  INNER JOIN "OCRD" T3
		  ON T3."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> 'Y'
	     AND T0."DocStatus" = 'O'
		 AND T1."DraftEntry" IS NULL
		 AND T0."ObjType" IN ('16', '14', '21', '20', '19'))) UNION ALL (SELECT T0."DocEntry", 
	  		 '112'							AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || 'd'				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays", 
			 0									AS "TolDays", 
	         T0."GroupNum",  
			 T2."InstlmntID"					AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0                            		AS "InstMonth", 
			 0                            		AS "InstDays", 
			 100								AS "InstPrcnt", 
			 T0."Installmnt",
	         T2."DueDate"						AS "DocDueDate", 
			 CASE	T0."ObjType" 
				WHEN '204' THEN
					0
				WHEN '18' THEN
					0
				WHEN '163' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							(T2."InsTotal" - T2."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '164' THEN
					0
				WHEN '203' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN '13' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN '165' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T2."InsTotal" - T2."PaidToDate")
					END
				WHEN '166' THEN
					(T2."InsTotal" - T2."PaidToDate")
			 END								AS	"Debit",
			 CASE	T0."ObjType"
				WHEN '204' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN '18' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN '163' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T2."InsTotal" - T2."PaidToDate")
					END
				WHEN '164' THEN
					(T2."InsTotal" - T2."PaidToDate")
				WHEN '203' THEN
					0
				WHEN '13' THEN
					0
				WHEN '165' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							(T2."InsTotal" - T2."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '166' THEN
					0
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN '204' THEN
					'EF'
				WHEN '18' THEN
					'EF'
				WHEN '163' THEN
					'EF'
				WHEN '164' THEN
					'EF'
				WHEN '203' THEN
					'SF'
				WHEN '13' THEN
					'SF'
				WHEN '165' THEN
					'SF'
				WHEN '166' THEN
					'SF'
			 END								AS "Group",
			 0									AS "PredictFlag",
			 T4."AvrageLate",
			 CASE T0."ObjType"
				WHEN '204' THEN
					IFNULL(T3."HldCode", '')
				WHEN '18' THEN
					IFNULL(T3."HldCode", '')
				WHEN '163' THEN
					IFNULL(T3."HldCode", '')
				WHEN '164' THEN
					IFNULL(T3."HldCode", '')
				WHEN '203' THEN
					IFNULL(T4."HldCode", '')
				WHEN '13' THEN
					IFNULL(T4."HldCode", '')
				WHEN '165' THEN
					IFNULL(T4."HldCode", '')
				WHEN '166' THEN
					IFNULL(T4."HldCode", '')
			 END								AS "HldCode"
	    FROM "ODRF" T0
   LEFT JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = 'N'
  INNER JOIN "DRF6" T2
		  ON T2."DocEntry" = T0."DocEntry"
   LEFT JOIN "OADM" T3
		  ON T3."Code" = T3."Code"
  INNER JOIN "OCRD" T4
		  ON T4."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> 'Y'
	     AND T0."DocStatus" = 'O'
		 AND T1."DraftEntry" IS NULL
		 AND T0."ObjType" IN ('203', '13', '165', '166', '204', '18', '163', '164'))) WITH READ ONLY