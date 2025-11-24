-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS AFTER:SP:CFF_GENERAL_OVERVIEW AFTER:SP:CFF_DOCUMENT_OVERVIEW

CREATE PROCEDURE CFF_CASHFLOW_OVERVIEW (IN dateFrom	DATE, 
										IN dateTo	DATE, 
										IN interval	NVARCHAR(1),
										IN objList	NVARCHAR(200),
										IN options	NVARCHAR(50),
										IN groups	NVARCHAR(50))
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
	addRP			NVARCHAR(1);
	addRT			NVARCHAR(1);
	addBA			NVARCHAR(1);
	addJV			NVARCHAR(1);
	addDD			NVARCHAR(1);
	addDP			NVARCHAR(1);
BEGIN 
	CALL CFF_PARSEOBJLIST(:objList, :groups);
	
	CALL CFF_PARSEOPTIONS(:options, :addRP, :addRT, :addBA, :addJV, :addDD, :addDP);

	CALL CFF_GENERAL_OVERVIEW(:dateFrom, :dateTo, :interval, :addRP, :addBA, :addJV, :addDP);
	
	CALL CFF_DOCUMENT_OVERVIEW(:dateFrom, :dateTo, :interval, :addRT, :addDD, :addDP);
	
	SELECT	T1."Index", 
			T1."ObjType", 
			SUM(T1."Debit") as "Debit", 
			SUM(T1."Credit") as "Credit",
			SUM(T1."Number") as "Number"
	FROM	"CFF_TMP_OVERVIEW"	T1
	INNER JOIN  "CFF_TMP_GROUP" T2 ON T2."Group" = T1."Group"
	GROUP BY	T1."Index", T1."Group", T1."ObjType"
	ORDER BY	T1."Index";
END;











