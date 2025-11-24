-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_Open_Inventory_Counting() 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
selectStr nvarchar(5000);
fromStr nvarchar(1000);
whereStr nvarchar(128);
tmpSQL nvarchar(256);
typeUSR integer := 12;
typeHEM integer := 171;
BEGIN
    tmpSQL := 'SELECT ' ||   :typeUSR  || ' as "TakerType", "USERID" as "TakerId", "U_NAME" as "TakerName" FROM OUSR'
    		  || ' UNION ALL SELECT ' ||  :typeHEM  || ' as "Takertype", "empID" as "Takerid", "lastName" || '' '' || "firstName" as "TakerName" FROM OHEM';
    
    selectStr := 'SELECT T0."DocEntry", T0."CountDate", T0."Time", T0."CountType", T1."TakerName" as "Taker1", T2."TakerName" as "Taker2", T0."Ref2"';
    
    fromStr := ' FROM OINC T0'
    			|| ' LEFT OUTER JOIN (' || :tmpSQL || ' ) T1 on T0."Taker1Type" = T1."TakerType" and T0."Taker1Id" = T1."TakerId"'
    			|| ' LEFT OUTER JOIN (' || :tmpSQL || ' ) T2 on T0."Taker2Type" = T2."TakerType" and T0."Taker2Id" = T2."TakerId"';
    
    whereStr := ' WHERE T0."Status" = ''O'' ';
    execute immediate (:selectStr || :fromStr || :whereStr);
	--select (:selectStr || :fromStr || :whereStr) from dummy;
END;











