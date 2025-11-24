CREATE VIEW "Z_TEST_PRODUCTION_20250925"."B1_CashFlowForecastRecurringTrasactionView" ( "DocEntry", "DocType", "DocNum", "CardCode", "CtlAccount", "TransId", "JrnlMemo", "ObjType", "OriginalType", "CreateDate", "TaxDate", "ClsDate", "PayDuMonth", "ExtraMonth", "ExtraDays", "TolDays", "GroupNum", "InstID", "InstNum", "InstMonth", "InstDays", "InstPrcnt", "Installmnt", "DocDueDate", "Debit", "Credit", "Group", "PredictFlag", "AvrageLate", "HldCode" ) AS ((SELECT T0."DocEntry", 
	  		 '540000043'						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || 'r'				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays",
			 IFNULL(T2."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T3."IntsNo", 0)				AS "InstID",
			 IFNULL(T2."InstNum", 0)			AS "InstNum",
			 IFNULL(T3."InstMonth", 0)			AS "InstMonth",
			 IFNULL(T3."InstDays", 0)			AS "InstDays",
			 IFNULL(T3."InstPrcnt", 100)		AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
			 CASE	T0."ObjType" 
				WHEN '22' THEN
					0
				WHEN '204' THEN
					0
				WHEN '18' THEN
					0
				WHEN '163' THEN
					CASE T4."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '164' THEN
					0
				WHEN '17' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '203' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '13' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '165' THEN
					CASE T4."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN '166' THEN
					(T0."DocTotal" - T0."PaidToDate")
			 END								AS "Debit",
			 CASE	T0."ObjType"
				WHEN '22' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '204' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '18' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '163' THEN
					CASE T4."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN '164' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '17' THEN
					0
				WHEN '203' THEN
					0
				WHEN '13' THEN
					0
				WHEN '165' THEN
					CASE T4."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '166' THEN
					0
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN '22' THEN
					'EF'
				WHEN '204' THEN
					'EF'
				WHEN '18' THEN
					'EF'
				WHEN '163' THEN
					'EF'
				WHEN '164' THEN
					'EF'
				WHEN '17' THEN
					'SF'
				WHEN '203' THEN
					'SF'
				WHEN '13' THEN
					'SF'
				WHEN '165' THEN
					'SF'
				WHEN '166' THEN
					'SF'
			 END								AS "Group",
			 CASE T0."ObjType"
				WHEN '17' THEN
					1
				WHEN '22' THEN
					1
				ELSE
					0
			 END								AS "PredictFlag",
			 T5."AvrageLate",
			 CASE T0."ObjType"
				WHEN '22' THEN
					IFNULL(T4."HldCode", '')
				WHEN '204' THEN
					IFNULL(T4."HldCode", '')
				WHEN '18' THEN
					IFNULL(T4."HldCode", '')
				WHEN '163' THEN
					IFNULL(T4."HldCode", '')
				WHEN '164' THEN
					IFNULL(T4."HldCode", '')
				WHEN '17' THEN
					IFNULL(T5."HldCode", '')
				WHEN '203' THEN
					IFNULL(T5."HldCode", '')
				WHEN '13' THEN
					IFNULL(T5."HldCode", '')
				WHEN '165' THEN
					IFNULL(T5."HldCode", '')
				WHEN '166' THEN
					IFNULL(T5."HldCode", '')
			 END								AS "HldCode"
	    FROM "ODRF" T0
  INNER JOIN "ORCP" T1
		  ON T0."ObjType" = ABS(T1."DocObjType")
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = 'N'
  INNER JOIN "OCTG" T2
		  ON T2."GroupNum" = T0."GroupNum"
   LEFT JOIN "CTG1" T3
		  ON T3."CTGCode" = T2."GroupNum"
   LEFT JOIN "OADM" T4
		  ON T4."Code" = T4."Code"
  INNER JOIN "OCRD" T5
		  ON T5."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> 'Y'
		 AND T1."DraftEntry" IS NOT NULL
		 AND T0."ObjType" IN('17', '203', '13', '165', '166', '22', '204', '18', '163', '164')) UNION ALL (SELECT T0."DocEntry", 
	  		 '540000043'						AS "DocType",
	         T0."DocNum", 
	         T0."CardCode", 
	         T0."CtlAccount", 
	         T0."TransId", 
	         T0."JrnlMemo",
	         T0."ObjType" || 'r'				AS "ObjType",
	         T0."ObjType"						AS "OriginalType", 
	         T0."CreateDate", 
	         T0."TaxDate", 
	         T0."ClsDate", 
	         T0."PayDuMonth", 
	         T0."ExtraMonth", 
	         T0."ExtraDays",
			 IFNULL(T2."TolDays", 0)			AS "TolDays", 
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
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '19' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '15' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '16' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN '14' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN '234000031' THEN
                    CASE T3."NegAmount" 
		                WHEN 'N' THEN
			                0
		                WHEN 'Y' THEN
			                0 - (T0."DocTotal" - T0."PaidToDate")
		            END
		        WHEN '234000032' THEN
		            CASE T3."NegAmount" 
		  	            WHEN 'N' THEN
	    	               (T0."DocTotal" - T0."PaidToDate")
	                    WHEN 'Y' THEN
	    	                0
	    	        END
			 END								AS "Debit",
			 CASE	T0."ObjType"
				WHEN '20' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '21' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN '19' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							0
						WHEN 'Y' THEN
							0 - (T0."DocTotal" - T0."PaidToDate")
					END
				WHEN '15' THEN
					0
				WHEN '16' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '14' THEN
					CASE T3."NegAmount" 
						WHEN 'N' THEN
							(T0."DocTotal" - T0."PaidToDate")
						WHEN 'Y' THEN
							0
					END
				WHEN '234000031' THEN
                    CASE T3."NegAmount" 
		                WHEN 'N' THEN
			                (T0."DocTotal" - T0."PaidToDate")
		                WHEN 'Y' THEN
			                0
		            END
		        WHEN '234000032' THEN
		            CASE T3."NegAmount" 
		                WHEN 'N' THEN
	    	                0
	                    WHEN 'Y' THEN
	    	                0 - (T0."DocTotal" - T0."PaidToDate")
	    	        END
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN '20' THEN
					'EF'
				WHEN '21' THEN
					'EF'
				WHEN '19' THEN
					'EF'
				WHEN '15' THEN
					'SF'
				WHEN '16' THEN
					'SF'
				WHEN '14' THEN
					'SF'
				WHEN '234000031' THEN
			        'SF'
		        WHEN '234000032' THEN
				    'EF'
			 END								AS "Group",
			 CASE T0."ObjType"
				WHEN '15' THEN
					1
				WHEN '234000031' THEN
				    1
				WHEN '234000032' THEN
				    1
				ELSE
					0
			 END								AS "PredictFlag",
			 T4."AvrageLate",
			 CASE T0."ObjType"
				WHEN '20' THEN
					IFNULL(T3."HldCode", '')
				WHEN '21' THEN
					IFNULL(T3."HldCode", '')
				WHEN '19' THEN
					IFNULL(T3."HldCode", '')
				WHEN '15' THEN
					IFNULL(T4."HldCode", '')
				WHEN '16' THEN
					IFNULL(T4."HldCode", '')
				WHEN '14' THEN
					IFNULL(T4."HldCode", '')
				WHEN '234000031' THEN
				    IFNULL(T4."HldCode", '')
				WHEN '234000032' THEN
				    IFNULL(T3."HldCode", '')
			 END								AS "HldCode"
	    FROM "ODRF" T0
  INNER JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = 'N'
  INNER JOIN "OCTG" T2
		  ON T2."GroupNum" = T0."GroupNum"
   LEFT JOIN "OADM" T3
		  ON T3."Code" = T3."Code"
  INNER JOIN "OCRD" T4
		  ON T4."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> 'Y'
		 AND T1."DraftEntry" IS NOT NULL
		 AND T0."ObjType" IN ('15', '16', '14', '20', '21', '19', '234000031', '234000032'))) WITH READ ONLY