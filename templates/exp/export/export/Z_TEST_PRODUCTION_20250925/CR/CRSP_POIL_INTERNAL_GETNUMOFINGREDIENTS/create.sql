-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables

CREATE PROCEDURE CRSP_POIL_INTERNAL_GETNUMOFINGREDIENTS (IN paramFatherRow integer, OUT paramNumOfSons integer) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
	treeTyp nvarchar(1);
	CURSOR treeType_Cursor FOR SELECT "TreeType" FROM "CR_BeforeSummaryINV1";
	inc integer;
BEGIN 
	inc := 0;
    paramNumOfSons := 0;
	
    OPEN treeType_Cursor;
    FETCH treeType_Cursor INTO treeTyp;
	
    WHILE NOT treeType_Cursor::NOTFOUND DO 
        inc := :inc + 1;
        IF :inc >= :paramFatherRow + 1 THEN 
            IF :treeTyp <> 'I' THEN 
                BREAK;
            END IF;
            paramNumOfSons := :paramNumOfSons + 1;
        END IF;
        FETCH treeType_Cursor INTO treeTyp;
    END WHILE;
	
    CLOSE treeType_Cursor;    
END;











