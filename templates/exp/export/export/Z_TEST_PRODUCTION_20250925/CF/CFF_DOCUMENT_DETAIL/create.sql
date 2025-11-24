-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS AFTER:SP:CFF_FILLPAYMENTDATE AFTER:SP:CFF_GETPREDICTDUEDATE
CREATE PROCEDURE CFF_DOCUMENT_DETAIL (IN  dateFrom		DATE, 
									  IN  dateTo		DATE,
									  IN  addRT			NVARCHAR(1),
									  IN  addDD			NVARCHAR(1),
									  IN  addDP			NVARCHAR(1)
									  )
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	predictDueDate	DATE;
	beginPoint		INT;
	hldCode			NVARCHAR(20);
BEGIN
	
	SELECT IFNULL("HldCode", '') INTO hldCode FROM OADM;
	
	--Generate payment date table.
	CALL CFF_FILLPAYMENTDATE;
	
	--Generate holiday info
	CALL CFF_GETLONGESTHOLIDAY;
		
	--Document.
	IF :addRT = 'N' THEN
		dueDates = 
			SELECT	T0."DueDateOnDoc"						AS	"DocDueDate",
					T0."CalDueDate",
					MAP(T0."CardCodePayment", NULL, 0, 1)	AS	"AdjustDocDueDate",
					IFNULL(T0."NextMonth", 0)				AS	"DeltaMonths",
					IFNULL(T0."PmntDate", 0)				AS	"AdjustDay",
					T0."HldCode",
					T0."DocType",
					T0."DocEntry",
					T0."CardCode",
					T0."CtlAccount",
					T0."Debit",
					T0."Credit",
					T0."OriginalType",
					T0."Group",
					T0."InstID",
					T0."DocNum",
					T0."AvrageLate"
			FROM
				(
				SELECT	T0."DocEntry", 
						T0."DocNum", 
						T0."DocType",
						T0."CardCode", 
						T0."CtlAccount", 
						T0."TransId", 
						T0."JrnlMemo", 
						T0."ObjType",
						T0."OriginalType", 
						T0."CreateDate", 
						T0."TaxDate", 
						T0."ClsDate", 
						T0."GroupNum", 
						T0."DueDate", 
						MAP(T1."CardCodePayment", NULL, T0."DueDateOnDoc", ADD_MONTHS( ADD_DAYS( TO_DATE ( YEAR(T0."DueDateOnDoc") || '-' || ( MONTH(T0."DueDateOnDoc") || '-' || '01' ), 'YYYY-MM-DD'), IFNULL(T1."PmntDate", 0) - 1 ), IFNULL(T1."NextMonth", 0) ) ) AS "DueDateOnDoc",
						T0."Debit",
						T0."Credit",
						T0."Group",  
						T0."InstID",
						T0."AvrageLate", 
						T0."HldCode",
						T0."CalDueDate",
						T0."PredictFlag",
						1							AS "Instance",
						T1."CardCodePayment",
						T1."NextMonth",
						T1."PmntDate"
				FROM
					(
						SELECT	T0."DocEntry", 
								T0."DocNum", 
								T0."DocType",
								T0."CardCode", 
								T0."CtlAccount", 
								T0."TransId", 
								T0."JrnlMemo", 
								T0."ObjType",
								T0."OriginalType", 
								T0."CreateDate", 
								T0."TaxDate", 
								T0."ClsDate", 
								T0."GroupNum",
								T0."DocDueDate"				AS "DueDate",
								CASE	T0."PayDuMonth"
									WHEN 'N' THEN
										ADD_DAYS( ADD_MONTHS(T0."DocDueDate", IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
									WHEN 'Y' THEN
										ADD_DAYS( ADD_MONTHS( ADD_DAYS( LAST_DAY(T0."DocDueDate"), 1 ), IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
									WHEN 'H' THEN
										ADD_DAYS( ADD_MONTHS( ADD_DAYS( LAST_DAY (T0."DocDueDate"), 1 - DAYOFMONTH( LAST_DAY(T0."DocDueDate") ) / 2 ), IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
									WHEN 'E' THEN
										ADD_DAYS( ADD_MONTHS( LAST_DAY(T0."DocDueDate"), IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
									ELSE
										NULL
								END							AS "DueDateOnDoc", 
								T0."Debit" * IFNULL(T0."InstPrcnt", 100) * 0.01 AS "Debit", 
								T0."Credit" * IFNULL(T0."InstPrcnt", 100) * 0.01 AS "Credit",
								T0."Group",  
								T0."InstID",
								T0."AvrageLate", 
								T0."HldCode",
								T0."DocDueDate"				AS "CalDueDate",
								T0."PredictFlag"
						  FROM	"B1_CashFlowForecastPredictDueDateDocumentsView" T0
						 WHERE	T0."ObjType" IN (SELECT "ObjType" FROM "CFF_TMP_OBJLIST" WHERE "ObjType" IN ('17', '22', '15', '234000031', '234000032', '17d', '22d', '15d', '234000031d', '234000032d'))
					)	T0
			LEFT JOIN	"CFF_TMP_PAYMENTDATE" T1 
				   ON	T1."CardCodePayment" = T0."CardCode"
				  AND	T1."DueDateDay" = DAYOFMONTH(T0."DueDateOnDoc")
				WHERE	T0."DueDate" IS NOT NULL
				)	T0
		LEFT JOIN	"CFF_TMP_HOLIDAY" T1
			   ON	T0."HldCode" = T1."HldCode"
			WHERE	T0."Group" IN (SELECT T2."Group" FROM "CFF_TMP_GROUP" T2)
			  AND	ADD_DAYS( T0."DueDateOnDoc", IFNULL(T1."Intervals", 0) + IFNULL(T0."AvrageLate", 0) ) >= :dateFrom
			  AND	T0."DueDateOnDoc" <= :dateTo;
		  
	ELSE
			
		--generate recurring trasaction date
		CALL CFF_RECURRINGTRASACTIONDATES(:dateFrom, :dateTo);
		
	--Recurring Transaction. 
	dueDates = 
			SELECT	T0."DueDateOnDoc"						AS	"DocDueDate",
					T0."CalDueDate",
					MAP(T0."CardCodePayment", NULL, 0, 1)	AS	"AdjustDocDueDate",
					IFNULL(T0."NextMonth", 0)				AS	"DeltaMonths",
					IFNULL(T0."PmntDate", 0)				AS	"AdjustDay",
					T0."HldCode",
					T0."DocType",
					T0."DocEntry",
					T0."CardCode",
					T0."CtlAccount",
					T0."Debit",
					T0."Credit",
					T0."OriginalType",
					T0."Group",
					T0."InstID",
					T0."DocNum",
					T0."AvrageLate"
			FROM
				(
				SELECT	T0."DocEntry", 
						T0."DocNum", 
						T0."DocType",
						T0."CardCode", 
						T0."CtlAccount", 
						T0."TransId", 
						T0."JrnlMemo", 
						T0."ObjType",
						T0."OriginalType", 
						T0."CreateDate", 
						T0."TaxDate", 
						T0."ClsDate", 
						T0."GroupNum", 
						T0."DueDate", 
						MAP(T1."CardCodePayment", NULL, T0."DueDateOnDoc", ADD_MONTHS( ADD_DAYS( TO_DATE ( YEAR(T0."DueDateOnDoc") || '-' || ( MONTH(T0."DueDateOnDoc") || '-' || '01' ), 'YYYY-MM-DD'), IFNULL(T1."PmntDate", 0) - 1 ), IFNULL(T1."NextMonth", 0) ) ) AS "DueDateOnDoc",
						T0."Debit",
						T0."Credit",
						T0."Group",  
						T0."InstID",
						T0."AvrageLate", 
						T0."HldCode",
						T0."CalDueDate",
						T0."PredictFlag",
						1							AS "Instance",
						T1."CardCodePayment",
						T1."NextMonth",
						T1."PmntDate"
				FROM
					(
						SELECT	T0."DocEntry", 
								T0."DocNum", 
								T0."DocType",
								T0."CardCode", 
								T0."CtlAccount", 
								T0."TransId", 
								T0."JrnlMemo", 
								T0."ObjType",
								T0."OriginalType", 
								T0."CreateDate", 
								T0."TaxDate", 
								T0."ClsDate", 
								T0."GroupNum",
								T0."DocDueDate"				AS "DueDate",
								CASE	T0."PayDuMonth"
									WHEN 'N' THEN
										ADD_DAYS( ADD_MONTHS(T0."DocDueDate", IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
									WHEN 'Y' THEN
										ADD_DAYS( ADD_MONTHS( ADD_DAYS( LAST_DAY(T0."DocDueDate"), 1 ), IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
									WHEN 'H' THEN
										ADD_DAYS( ADD_MONTHS( ADD_DAYS( LAST_DAY (T0."DocDueDate"), 1 - DAYOFMONTH( LAST_DAY(T0."DocDueDate") ) / 2 ), IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
									WHEN 'E' THEN
										ADD_DAYS( ADD_MONTHS( LAST_DAY(T0."DocDueDate"), IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
									ELSE
										NULL
								END							AS "DueDateOnDoc", 
								T0."Debit" * IFNULL(T0."InstPrcnt", 100) * 0.01 AS "Debit", 
								T0."Credit" * IFNULL(T0."InstPrcnt", 100) * 0.01 AS "Credit",
								T0."Group",  
								T0."InstID",
								T0."AvrageLate", 
								T0."HldCode",
								T0."DocDueDate"				AS "CalDueDate",
								T0."PredictFlag"
						  FROM	"B1_CashFlowForecastPredictDueDateDocumentsView" T0
						 WHERE	T0."ObjType" IN (SELECT "ObjType" FROM "CFF_TMP_OBJLIST" WHERE "ObjType" IN ('17', '22', '15', '234000031', '234000032', '17d', '22d', '15d', '234000031d', '234000032d'))
					)	T0
			LEFT JOIN	"CFF_TMP_PAYMENTDATE" T1 
				   ON	T1."CardCodePayment" = T0."CardCode"
				  AND	T1."DueDateDay" = DAYOFMONTH(T0."DueDateOnDoc")
				WHERE	T0."DueDate" IS NOT NULL
				)	T0
		LEFT JOIN	"CFF_TMP_HOLIDAY" T1
			   ON	T0."HldCode" = T1."HldCode"
			WHERE	T0."Group" IN (SELECT T2."Group" FROM "CFF_TMP_GROUP" T2)
			  AND	ADD_DAYS( T0."DueDateOnDoc", IFNULL(T1."Intervals", 0) + IFNULL(T0."AvrageLate", 0) ) >= :dateFrom
			  AND	T0."DueDateOnDoc" <= :dateTo
	UNION ALL
		SELECT	T0."DueDateOnDoc"						AS	"DocDueDate",
				T0."CalDueDate",
				MAP(T0."CardCodePayment", NULL, 0, 1)	AS	"AdjustDocDueDate",
				IFNULL(T0."NextMonth", 0)				AS	"DeltaMonths",
				IFNULL(T0."PmntDate", 0)				AS	"AdjustDay",
				T0."HldCode",
				T0."DocType",
				T0."DocEntry",
				T0."CardCode",
				T0."CtlAccount",
				T0."Debit",
				T0."Credit",
				T0."OriginalType",
				T0."Group",
				T0."InstID",
				T0."DocNum",
				T0."AvrageLate"
		FROM
			(		
			SELECT 	T0."DocEntry", 
					T0."DocNum", 
					T0."DocType",
					T0."CardCode", 
					T0."CtlAccount", 
					T0."TransId", 
					T0."JrnlMemo", 
					T0."ObjType", 
					T0."OriginalType",
					T0."CreateDate", 
					T0."TaxDate", 
					T0."ClsDate", 
					T0."GroupNum",  
					T0."DueDate", 
					MAP(T1."CardCodePayment", NULL, T0."DueDateOnDoc", ADD_MONTHS( ADD_DAYS( TO_DATE ( YEAR(T0."DueDateOnDoc") || '-' || ( MONTH(T0."DueDateOnDoc") || '-' || '01' ), 'YYYY-MM-DD'), IFNULL(T1."PmntDate", 0) - 1 ), IFNULL(T1."NextMonth", 0) ) ) AS "DueDateOnDoc",
					T0."Debit",
					T0."Credit", 
					T0."Group",	            
					T0."InstID",
					T0."AvrageLate", 
					MAP(T0."OriginalType", '22', :hldCode, '20', :hldCode, '21', :hldCode, '204', :hldCode, '18', :hldCode, '19', :hldCode, '163', :hldCode, '164', :hldCode, T0."HldCode") AS "HldCode",
					T0."CalDueDate",
					T0."PredictFlag",
					T0."Instance",
					T1."CardCodePayment",
					T1."NextMonth",
					T1."PmntDate"					
			FROM(
					SELECT	T0."DocEntry", 
							T0."DocNum", 
							T0."DocType",
							T0."CardCode", 
							T0."CtlAccount", 
							T0."TransId", 
							T0."JrnlMemo", 
							T0."ObjType",
							T0."OriginalType", 
							T0."CreateDate", 
							T0."TaxDate", 
							T0."ClsDate",
							T0."GroupNum",  
							T2."PlanDate"				AS "DueDate", 
							CASE	T0."PayDuMonth"
								WHEN 'N' THEN
									ADD_DAYS( ADD_MONTHS(T2."PlanDate", IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
								WHEN 'Y' THEN
									ADD_DAYS( ADD_MONTHS( ADD_DAYS( LAST_DAY(T2."PlanDate"), 1 ), IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
								WHEN 'H' THEN
									ADD_DAYS( ADD_MONTHS( ADD_DAYS( LAST_DAY (T2."PlanDate"), 1 - DAYOFMONTH( LAST_DAY(T2."PlanDate") ) / 2 ), IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
								WHEN 'E' THEN
									ADD_DAYS( ADD_MONTHS( LAST_DAY(T2."PlanDate"), IFNULL(T0."ExtraMonth", 0) + IFNULL(T0."InstMonth", 0) ), IFNULL(T0."ExtraDays", 0) + IFNULL(T0."InstDays", 0) - IFNULL(T0."TolDays", 0) )
								ELSE
									NULL
							END							AS "DueDateOnDoc",							
							T0."Debit" * IFNULL(T0."InstPrcnt", 100) * 0.01 AS "Debit", 
							T0."Credit" * IFNULL(T0."InstPrcnt", 100) * 0.01 AS "Credit", 
							T0."Group",  
							T0."InstID",
							T0."AvrageLate", 
							T0."HldCode",
							T2."PlanDate"				AS "CalDueDate",
							T0."PredictFlag",
							T2."Instance"
					  FROM	"B1_CashFlowForecastRecurringTrasactionView" T0
				INNER JOIN	
							(
								SELECT * 
								  FROM ORCL
							 UNION ALL
								SELECT *
								  FROM CFF_TMP_ORCL
							)	T2
						ON	T2."DocEntry" = T0."DocEntry" 
					   AND	ABS(T2."DocObjType") = T0."OriginalType" 
					   AND	T2."Status" = 'N'
					 WHERE	T0."ObjType" IN (SELECT "ObjType" FROM "CFF_TMP_OBJLIST")
			)	T0
		LEFT JOIN	"CFF_TMP_PAYMENTDATE" T1 
			   ON	T1."CardCodePayment" = T0."CardCode"
			  AND	T1."DueDateDay" = DAYOFMONTH(T0."DueDateOnDoc")
			WHERE	T0."DueDate" IS NOT NULL				
			)	T0
	LEFT JOIN	"CFF_TMP_HOLIDAY" T1
		   ON	T0."HldCode" = T1."HldCode"
		WHERE	T0."Group" IN (SELECT T2."Group" FROM "CFF_TMP_GROUP" T2)
		  AND	ADD_DAYS( T0."DueDateOnDoc", IFNULL(T1."Intervals", 0) + IFNULL(T0."AvrageLate", 0) ) >= :dateFrom
		  AND	T0."DueDateOnDoc" <= :dateTo;
	END IF;

	numDueDates = CE_Projection(:dueDates, [CE_calc('rownum()', integer) AS "AbsEntry", "DocDueDate", 
								"AdjustDocDueDate", "DeltaMonths", "AdjustDay", "HldCode", "DocType", "DocEntry", "CardCode", "CtlAccount", "Debit", "Credit", "OriginalType", "Group", "InstID", "DocNum", "AvrageLate"] );

	holidays = Select T0."HldCode",
					   "WndFrm",
					   "WndTo",
					   Case "isCurYear"
						 When 'Y' Then
						  1
						 Else
						  0
					   End											AS "IsCurYear",
					   Case "ignrWnd"
						 When 'Y' Then
						  1
						 Else
						  0
					   End											AS "IgnrWnd",
					   IFNULL(T1."StrDate", To_Date('00010101'))	AS "StrDate",
					   IFNULL(T1."EndDate", To_Date('00010101'))	AS "EndDate"
			  From OHLD T0
			  Left Join HLD1 T1
				On T0."HldCode" = T1."HldCode"
			 Order By T0."HldCode", T1."StrDate";
			 
	Call CFF_GETPREDICTDUEDATE(:numDueDates, :holidays, predictDates);

	--Consider Delays in Payments.
	IF :addDP = 'Y' THEN
		INSERT INTO "CFF_TMP_DETAIL"
		SELECT	ADD_DAYS(T0."PredictDueDate", IFNULL(T1."AvrageLate", 0)), 
				T1."DocType", 
				T1."DocEntry", 
				T1."CardCode",
				T1."CtlAccount" AS "ControlAccount", 
				NULL AS "Remarks", 
				T1."Debit", 
				T1."Credit",
				T1."OriginalType",
				T1."Group",
				T1."InstID",
				T1."DocNum"
		  FROM	:numDueDates T1, :predictDates T0
	 	 WHERE	T0."AbsEntry" = T1."AbsEntry"
		   	AND	ADD_DAYS(T0."PredictDueDate", IFNULL(T1."AvrageLate", 0)) >= :dateFrom
		   	AND	ADD_DAYS(T0."PredictDueDate", IFNULL(T1."AvrageLate", 0)) <= :dateTo
		UNION ALL
		SELECT	ADD_DAYS(T1."DocDueDate", IFNULL(T1."AvrageLate", 0)),
				T1."DocType", 
				T1."DocEntry", 
				T1."CardCode",
				T1."CtlAccount" AS "ControlAccount", 
				NULL AS "Remarks", 
				T1."Debit", 
				T1."Credit",
				T1."OriginalType",
				T1."Group",
				T1."InstID",
				T1."DocNum"
		  FROM	"B1_CashFlowForecastDueDateDocumentsView" T1,
		  		"OCRD" T0
		 WHERE	ADD_DAYS(T1."DocDueDate", IFNULL(T0."AvrageLate", 0)) >= :dateFrom
		   AND	ADD_DAYS(T1."DocDueDate", IFNULL(T0."AvrageLate", 0)) <= :dateTo
		   AND	T1."ObjType" IN (SELECT "ObjType" FROM "CFF_TMP_OBJLIST")
		   AND  T1."Group" IN (SELECT T2."Group" FROM "CFF_TMP_GROUP" T2)
		   AND	T1."CardCode" = T0."CardCode";
	ELSE
		INSERT INTO "CFF_TMP_DETAIL"
		SELECT	T0."PredictDueDate", 
				T1."DocType", 
				T1."DocEntry", 
				T1."CardCode",
				T1."CtlAccount" AS "ControlAccount", 
				NULL AS "Remarks", 
				T1."Debit", 
				T1."Credit",
				T1."OriginalType",
				T1."Group",
				T1."InstID",
				T1."DocNum"
		  FROM	:numDueDates T1, :predictDates T0
	 	 WHERE	T0."AbsEntry" = T1."AbsEntry"
	 	   AND	T0."PredictDueDate" >= :dateFrom
		   AND	T0."PredictDueDate" <= :dateTo
		UNION ALL
		SELECT	T1."DocDueDate",
				T1."DocType", 
				T1."DocEntry", 
				T1."CardCode",
				T1."CtlAccount" AS "ControlAccount", 
				NULL AS "Remarks", 
				T1."Debit", 
				T1."Credit",
				T1."OriginalType",
				T1."Group",
				T1."InstID",
				T1."DocNum"
		  FROM	"B1_CashFlowForecastDueDateDocumentsView" T1,
		  		"OCRD" T0
		 WHERE	ADD_DAYS(T1."DocDueDate", IFNULL(T0."AvrageLate", 0)) >= :dateFrom
		   AND	ADD_DAYS(T1."DocDueDate", IFNULL(T0."AvrageLate", 0)) <= :dateTo
		   AND	T1."ObjType" IN (SELECT "ObjType" FROM "CFF_TMP_OBJLIST")
		   AND  T1."Group" IN (SELECT T2."Group" FROM "CFF_TMP_GROUP" T2)
		   AND	T1."CardCode" = T0."CardCode";
	END IF;
END;











