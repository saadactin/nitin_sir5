-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS AFTER:SP:CFF_FILLPAYMENTDATE AFTER:SP:CFF_GETPREDICTDUEDATE

CREATE PROCEDURE CFF_DOCUMENT_OVERVIEW (IN dateFrom		DATE, 
										IN dateTo		DATE, 
										IN interval		NVARCHAR(1),
									    IN addRT		NVARCHAR(1),
									    IN addDD		NVARCHAR(1),
									    IN addDP		NVARCHAR(1)
										) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
BEGIN
	
	CALL CFF_DOCUMENT_DETAIL(:dateFrom, :dateTo, :addRT, :addDD, :addDP);
	
	INSERT INTO "CFF_TMP_OVERVIEW"
	SELECT  T1."Index",
			T1."EndDate", 
			SUM(T0."Debit"), 
			SUM(T0."Credit"), 
			T0."ObjType", 
			T0."Group", 
			COUNT(*)
	  FROM	"CFF_TMP_DETAIL" T0,
			"CFF_TMP_DATE" T1
	 WHERE	T0."DueDate" BETWEEN  T1."StartDate" AND T1."EndDate"
  GROUP BY	T1."EndDate", T0."ObjType", T1."Index", T0."Group";
	
END;











