-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS AFTER:SP:CFF_GENERAL_DETAIL
CREATE PROCEDURE CFF_CASHFLOW_DETAIL (IN dateFrom	DATE, 
									  IN dateTo		DATE, 
									  IN objList	NVARCHAR(200),
									  IN options	NVARCHAR(50),
									  IN groups		NVARCHAR(50),
									  IN topNum		INTEGER)
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
	-- Parse Object list
	CALL CFF_PARSEOBJLIST(:objList, :groups);
	
	CALL CFF_PARSEOPTIONS(:options, :addRP, :addRT, :addBA, :addJV, :addDD, :addDP);

	CALL CFF_GENERAL_DETAIL(:dateFrom, :dateTo, :addRP, :addBA, :addJV, :addDP);
	
	CALL CFF_DOCUMENT_DETAIL(:dateFrom, :dateTo, :addRT, :addDD, :addDP);
	
	IF :topNum > 0 THEN
		SELECT TOP :topNum T0."ObjType",
			   T0."DueDate",
			   T0."DocType",
			   T0."DocEntry",
			   T0."DocNum",
			   T0."InstImnt",
			   T0."CtrlAcct",
			   T0."Account",
			   T0."Debit",
			   T0."Credit"
		  FROM "CFF_TMP_DETAIL" T0 
			ORDER BY T0."DueDate";
	ELSE
		SELECT T0."ObjType",
			   T0."DueDate",
			   T0."DocType",
			   T0."DocEntry",
			   T0."DocNum",
			   T0."InstImnt",
			   T0."CtrlAcct",
			   T0."Account",
			   T0."Debit",
			   T0."Credit"
		  FROM "CFF_TMP_DETAIL" T0 
			ORDER BY T0."DueDate";	
	END IF;
END;











