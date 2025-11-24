-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables

CREATE PROCEDURE CRSP_POIL_INTERNAL_ISJOINED (IN line integer, OUT joined nvarchar(1)) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
	i int;	
	CURSOR c_cursor1 FOR SELECT "Joined" FROM "CR_SalesBOMSummaryByItem" ORDER BY "VisOrder";
BEGIN
	i := 1;
	OPEN c_cursor1;
	FETCH c_cursor1 INTO joined;

	WHILE ((NOT c_cursor1::NOTFOUND) AND :i < :line) DO
		i := :i + 1;
		FETCH c_cursor1 INTO joined;
	END WHILE;	
 	
 	CLOSE c_cursor1;
END;











