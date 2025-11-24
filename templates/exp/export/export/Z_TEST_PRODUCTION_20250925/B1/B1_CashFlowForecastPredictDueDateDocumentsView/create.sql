CREATE VIEW "Z_TEST_PRODUCTION_20250925"."B1_CashFlowForecastPredictDueDateDocumentsView" ( "DocEntry", "DocType", "DocNum", "CardCode", "CtlAccount", "TransId", "JrnlMemo", "ObjType", "OriginalType", "CreateDate", "TaxDate", "ClsDate", "PayDuMonth", "ExtraMonth", "ExtraDays", "TolDays", "GroupNum", "InstID", "InstNum", "InstMonth", "InstDays", "InstPrcnt", "Installmnt", "DocDueDate", "Debit", "Credit", "Group", "PredictFlag", "AvrageLate", "HldCode" ) AS ((((((((SELECT T0."DocEntry", 
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
			 IFNULL(T1."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum", 
			 IFNULL(T2."IntsNo", 0)				AS "InstID",
			 IFNULL(T1."InstNum", 0)			AS "InstNum",
			 IFNULL(T2."InstMonth", 0)			AS "InstMonth",
			 IFNULL(T2."InstDays", 0)			AS "InstDays",
			 IFNULL(T2."InstPrcnt", 100)		AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
	         T0."DocTotal" - T0."PaidToDate"	AS "Debit", 
	         0 									AS "Credit",
			 'SF'								AS "Group",
			 1 									AS "PredictFlag",
			 T3."AvrageLate",
			 IFNULL(T3."HldCode", '')			AS "HldCode"
	    FROM "ORDR" T0
  INNER JOIN "OCTG" T1 
		  ON T1."GroupNum" = T0."GroupNum"
   LEFT JOIN "CTG1" T2 
		  ON T2."CTGCode" = T1."GroupNum"
  INNER JOIN "OCRD" T3
		  ON T3."CardCode" = T0."CardCode" 
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
			 IFNULL(T1."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
	         T0."DocTotal" - T0."PaidToDate"	AS "Debit", 
	         0									AS "Credit",
			 'SF'								AS "Group",
			 1									AS "PredictFlag",
			 T3."AvrageLate",
			 IFNULL(T3."HldCode", '')			AS "HldCode"
	    FROM "ODLN" T0
  INNER JOIN "OCTG" T1
		  ON T1."GroupNum" = T0."GroupNum"
  INNER JOIN "OCRD" T3
		  ON T3."CardCode" = T0."CardCode" 
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
			 IFNULL(T1."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T2."IntsNo", 0)				AS "InstID",
			 IFNULL(T1."InstNum", 0)			AS "InstNum",
			 IFNULL(T2."InstMonth", 0)			AS "InstMonth",
			 IFNULL(T2."InstDays", 0)			AS "InstDays",
			 IFNULL(T2."InstPrcnt", 100)		AS "InstPrcnt",
			 T0."Installmnt",
	         T0."DocDueDate", 
	         0									AS "Debit", 
	         T0."DocTotal" - T0."PaidToDate"	AS "Credit",
			 'EF'								AS "Group",
			 1									AS "PredictFlag",
			 T4."AvrageLate",
			 IFNULL(T3."HldCode", '')			AS "HldCode"
	    FROM "OPOR" T0
  INNER JOIN "OCTG" T1 
		  ON T1."GroupNum" = T0."GroupNum"
   LEFT JOIN "CTG1" T2 
		  ON T2."CTGCode" = T1."GroupNum"
   LEFT JOIN "OADM" T3
		  ON T3."Code" = T3."Code"
  INNER JOIN "OCRD" T4
		  ON T4."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> 'Y'
	     AND T0."DocStatus" = 'O')) UNION ALL (SELECT T0."DocEntry", 
	  		 '112' 							AS "DocType",
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
				WHEN '17' THEN
					(T0."DocTotal" - T0."PaidToDate")
			 END								AS "Debit",
			 CASE	T0."ObjType"
				WHEN '22' THEN
					(T0."DocTotal" - T0."PaidToDate")
				WHEN '17' THEN
					0
			 END								AS "Credit",
			 CASE T0."ObjType"
				WHEN '22' THEN
					'EF'
				WHEN '17' THEN
					'SF'
			 END								AS "Group",
			 1									AS "PredictFlag",
			 T5."AvrageLate",
			 CASE T0."ObjType"
				WHEN '22' THEN
					IFNULL(T5."HldCode", '')
				WHEN '17' THEN
					IFNULL(T4."HldCode", '')
			 END								AS "HldCode"
	    FROM "ODRF" T0
   LEFT JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
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
	     AND T0."DocStatus" = 'O'
		 AND T1."DraftEntry" IS NULL
		 AND T0."ObjType" IN ('22', '17'))) UNION ALL (SELECT T0."DocEntry", 
	  		 '112' 							AS "DocType",
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
			 IFNULL(T2."TolDays", 0)			AS "TolDays", 
	         T0."GroupNum",  
			 IFNULL(T0."Installmnt", 0)			AS "InstID", 
			 IFNULL(T0."Installmnt", 0)			AS "InstNum", 
			 0									AS "InstMonth", 
			 0									AS "InstDays", 
			 100								AS "InstPrcnt", 
			 T0."Installmnt",
	         T0."DocDueDate", 
			 (T0."DocTotal" - T0."PaidToDate")	AS "Debit",
			 0									AS "Credit",
			 'SF'								AS "Group",
			 1									AS "PredictFlag",
			 T4."AvrageLate",
			 IFNULL(T4."HldCode", '')			AS "HldCode"
	    FROM "ODRF" T0
   LEFT JOIN "ORCP" T1
		  ON T0."ObjType" = T1."DocObjType"
		 AND T0."DocEntry" = T1."DraftEntry"
		 AND T1."IsRemoved" = 'N'
  INNER JOIN "OCTG" T2
		  ON T2."GroupNum" = T0."GroupNum"
  INNER JOIN "OCRD" T4
		  ON T4."CardCode" = T0."CardCode" 
	   WHERE T0."CANCELED" <> 'Y'
	     AND T0."DocStatus" = 'O'
		 AND T1."DraftEntry" IS NULL
		 AND T0."ObjType" = '15')) UNION ALL (SELECT 
      T0."DocEntry",
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
      IFNULL(T1."TolDays", 0)			AS "TolDays",
      T0."GroupNum", 
	  IFNULL(T0."Installmnt", 0)		AS "InstID", 
	  IFNULL(T0."Installmnt", 0)		AS "InstNum", 
	  0			                        AS "InstMonth",
	  0			                        AS "InstDays",
	  100		                        AS "InstPrcnt",
      T0."Installmnt",
      T0."DocDueDate", 
      CASE T3."NegAmount" 
		WHEN 'N' THEN
			0
		WHEN 'Y' THEN
			0 - (T0."DocTotal" - T0."PaidToDate")
		END								AS "Debit", 
	  CASE T3."NegAmount" 
		WHEN 'N' THEN
			(T0."DocTotal" - T0."PaidToDate")
		WHEN 'Y' THEN
			0
		END								AS "Credit",
      'SF'								AS "Group",
      1									AS "PredictFlag",
      IFNULL(T2."AvrageLate", 0) AS "AvrageLate", 
      IFNULL(T2."HldCode", '')			AS "HldCode"
      FROM  "ORRR" T0 
      INNER JOIN "OCTG" T1 
		  ON T1."GroupNum" = T0."GroupNum"
	  INNER  JOIN "OCRD" T2  ON  T2."CardCode" = T0."CardCode"    
	  LEFT   JOIN "OADM" T3  ON T3."Code" = T3."Code"
      WHERE T0."CANCELED" <> 'Y' AND  T0."DocStatus" = 'O')) UNION ALL (SELECT 
      T0."DocEntry",
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
	  IFNULL(T1."TolDays", 0)			AS "TolDays", 
	  T0."GroupNum", 
	  IFNULL(T0."Installmnt", 0)		AS "InstID", 
	  IFNULL(T0."Installmnt", 0)		AS "InstNum", 
	  0			                        AS "InstMonth",
	  0			                        AS "InstDays",
	  100		                        AS "InstPrcnt",
      T0."Installmnt",
      T0."DocDueDate", 
	  CASE T3."NegAmount" 
	    WHEN 'N' THEN
	    	(T0."DocTotal" - T0."PaidToDate")
	    WHEN 'Y' THEN
	    	0
	    END								AS "Debit", 
	  CASE T3."NegAmount" 
	    WHEN 'N' THEN
	    	0
	    WHEN 'Y' THEN
	    	0 - (T0."DocTotal" - T0."PaidToDate")
	    END								AS "Credit",
      'EF'								AS "Group",
      1									AS "PredictFlag",
      IFNULL(T2."AvrageLate", 0) AS "AvrageLate", 
      IFNULL(T3."HldCode", '')			AS "HldCode"
      FROM  "OPRR" T0
      INNER JOIN "OCTG" T1 
		  ON T1."GroupNum" = T0."GroupNum"  
	  INNER  JOIN "OCRD" T2  ON  T2."CardCode" = T0."CardCode"   
	  LEFT   JOIN "OADM" T3  ON T3."Code" = T3."Code"
      WHERE T0."CANCELED" <> 'Y' AND  T0."DocStatus" = 'O')) UNION ALL (SELECT 
      T0."DocEntry",
      '112'						       AS "DocType", 
      T0."DocNum", 
      T0."CardCode", 
      T0."CtlAccount",
      T0."TransId", 
      T0."JrnlMemo", 
      T0."ObjType" || 'd'               AS "ObjType",
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
          WHEN '234000031' THEN
            CASE T4."NegAmount" 
		        WHEN 'N' THEN
			          0
		        WHEN 'Y' THEN
			          0 - (T0."DocTotal" - T0."PaidToDate")
		    END
		  WHEN '234000032' THEN
		     CASE T4."NegAmount" 
		  	    WHEN 'N' THEN
	    	          (T0."DocTotal" - T0."PaidToDate")
	            WHEN 'Y' THEN
	    	           0
	    	  END
	   END                                      AS "Debit",  
      CASE	T0."ObjType" 
          WHEN '234000031' THEN
            CASE T4."NegAmount" 
		        WHEN 'N' THEN
			          (T0."DocTotal" - T0."PaidToDate")
		        WHEN 'Y' THEN
			          0
		    END
		  WHEN '234000032' THEN
		     CASE T4."NegAmount" 
		  	    WHEN 'N' THEN
	    	          0
	            WHEN 'Y' THEN
	    	          0 - (T0."DocTotal" - T0."PaidToDate")
	    	  END
	   END                                      AS "Credit",
	   CASE T0."ObjType"
		   WHEN '234000031' THEN
			    'SF'
		   WHEN '234000032' THEN
				'EF'
	   END								AS "Group",
      1									AS "PredictFlag",
      IFNULL(T3."AvrageLate", 0) AS "AvrageLate",    
	  CASE T0."ObjType"
		  WHEN '234000031' THEN
				IFNULL(T3."HldCode", '')
		  WHEN '234000032' THEN
				IFNULL(T4."HldCode", '')
	  END								AS "HldCode"	  
      FROM "ODRF" T0
 LEFT JOIN "ORCP" T1 
	     ON T0."ObjType" = T1."DocObjType"
		AND T0."DocEntry" = T1."DraftEntry"
		AND T1."IsRemoved" = 'N'
INNER JOIN "OCTG" T2
		 ON T2."GroupNum" = T0."GroupNum"
INNER JOIN "OCRD" T3  
         ON  T3."CardCode" = T0."CardCode"    
LEFT  JOIN "OADM" T4  
         ON T4."Code" = T4."Code"
      WHERE  T0."CANCELED" <> 'Y' 
        AND  T0."DocStatus" = 'O'
        AND  T1."DraftEntry" IS NULL
        AND  T0."ObjType" IN ('234000031', '234000032'))) WITH READ ONLY