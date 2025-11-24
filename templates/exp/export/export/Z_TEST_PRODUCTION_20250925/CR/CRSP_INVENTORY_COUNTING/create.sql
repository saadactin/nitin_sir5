-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_Inventory_Counting
(
in DocKey_in integer
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
AS
selectStr nvarchar(256);
fromStr nvarchar(256);
whereStr nvarchar(64);
docKey integer;
taker1Type integer;
taker2Type integer;
typeUSR integer := 12;
typeHEM integer := 171;
taker1 nvarchar(64);
taker2 nvarchar(64);
joinTbl1 nvarchar(5);
joinTbl2 nvarchar(5);
joinCol1 nvarchar(16);
joinCol2 nvarchar(16);
counter integer;
BEGIN 
    joinTbl2 := '';
    joinCol2 := '';
    docKey := :DocKey_in;
    SELECT count("Taker1Type") into counter FROM OINC where "DocEntry" = :docKey;
    if( (:counter) > 0 ) then
    	SELECT "Taker1Type" INTO taker1Type FROM OINC where "DocEntry" = :docKey;
    end if;
    
    SELECT count("Taker2Type") into counter FROM OINC where "DocEntry" = :docKey;
    if ( (:counter) > 0 ) then
    	SELECT "Taker2Type" INTO taker2Type FROM OINC where "DocEntry" = :docKey;
    end if;
    
    IF (:taker1Type = :typeUSR) THEN 
        taker1 := 'T1."U_NAME"';
        joinTbl1 := 'OUSR';
        joinCol1 := '"USERID"';
    ELSE 
        taker1 := 'T1."lastName"||'' ''||T1."firstName"';
        joinTbl1 := 'OHEM';
        joinCol1 := '"empID"';
    END IF;
    IF (:taker2Type = :typeUSR) THEN 
        taker2 := 'T2."U_NAME"';
        joinTbl2 := 'OUSR';
        joinCol2 := '"USERID"';
    ELSE 
        IF (:taker2Type = :typeHEM) THEN 
            taker2 := 'T2."lastName"||'' ''||T2."firstName"';
            joinTbl2 := 'OHEM';
            joinCol2 := '"empID"';
        ELSE 
            taker2 := :taker1;
        END IF;
    END IF;
    selectStr := 'select T0."DocEntry", T0."DocNum", T0."CountDate", T0."Time", T0."CountType", '
    			|| :taker1 || ' as "Taker1", '
    			|| :taker2 || ' as "Taker2", '
    			|| 'T0."Ref2", T0."Remarks"';
    fromStr := ' from OINC T0';
    fromStr := :fromStr || ' join ' || :joinTbl1 || ' T1 on T0."Taker1Id" = T1.' || :joinCol1;
    IF (:joinTbl2 <> '') THEN 
        fromStr := :fromStr || ' join ' || :joinTbl2 || ' T2 on T0."Taker2Id" = T2.' || :joinCol2;
    END IF;
    whereStr := ' where T0."DocEntry" = ' || :docKey;
    execute immediate(:selectStr || :fromStr || :whereStr);
	--select :selectStr || :fromStr || :whereStr from dummy;
END;











