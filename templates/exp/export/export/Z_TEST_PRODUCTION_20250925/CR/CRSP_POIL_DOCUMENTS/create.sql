-- B1 DEPENDS: AFTER:SP:CRSP_POIL_INTERNAL_GETINCCOMPAREUNIT AFTER:SP:CRSP_POIL_INTERNAL_ADDTONTHLINE AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_POIL_DOCUMENTS (in DocEntryIn integer, in object nvarchar(4)) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
strSQL nvarchar(5000);
DocEntry integer;
searchRow integer;
searchVal nvarchar(5000);
matchRow integer;
matchVal nvarchar(5000);
searchUnitSize integer;
matchUnitSize integer;
joinRow integer;
lineFrom integer;
lineTo integer;
joined nvarchar(1);
recNum integer;
exists1 integer;
i integer;
tmpStr nvarchar(2000);

CURSOR c_cursor1 FOR SELECT "Joined" FROM "CR_SalesBOMSummaryByItem" ORDER BY "VisOrder";
CURSOR c_cursor2 FOR SELECT "Joined" FROM "CR_SalesBOMSummaryByItem" ORDER BY "VisOrder";

BEGIN 

	delete from "CR_BeforeSummaryINV1";
	delete from "CR_SalesBOMSummaryByItem";
	
	--The main logic code simulates the C++ code
	--in B1 source code function SRIncByItem
	    
    DocEntry := DocEntryIn;
    --Get the Whole Data of INV1
    --Result table

	tmpStr := 'INSERT INTO "CR_BeforeSummaryINV1" (SELECT 
    	"VisOrder", 
    	''N'' AS "Joined",     
    	"ItemCode", 
		IFNULL(' || :object || '."SubCatNum", ''''),
		"Dscription",
		"TreeType",
		"UseBaseUn",
		"Price",
		"DiscPrcnt",
		"Factor1", 
		"Factor2",
		"Factor3",
		"Factor4",
		"PriceAfVAT",
		"TaxOnly",
		IFNULL(' || :object || '."unitMsr", ''''),
		"NumPerMsr",
		"WhsCode",
		"Quantity",
		"PriceBefDi",
		"LineTotal",
		"TotalSumSy",
		"TotalFrgn"    
     FROM ' || :object || ' WHERE ' || :object || '."DocEntry" = ' || :DocEntry || ')';
     
    exec(:tmpStr);

    tmpStr := 'INSERT INTO "CR_SalesBOMSummaryByItem" (SELECT "DocEntry", ''N'' AS "Joined", "VisOrder", "TreeType", "VendorNum", 
        "ItemCode", "Dscription", "Quantity", "unitMsr", "Price", "DiscPrcnt", "PriceBefDi", "LineTotal", 
        "TotalSumSy", "TotalFrgn", "Currency", "WhsCode" FROM ' || :object || ' WHERE ' || :object || '."DocEntry" = ' || :DocEntry || ')';
       
    exec(:tmpStr);
      
    searchRow := 1;
    searchVal := '';
    matchRow := 1;
    matchVal := '';
    searchUnitSize := 1;
    matchUnitSize := 1;
    joinRow := 0;
    SELECT COUNT(*) INTO recNum FROM "CR_BeforeSummaryINV1";
    WHILE :searchRow <= :recNum DO 
        --CALL CRSP_POIL_INTERNAL_ISJOINED(:searchRow, :joined);

		i := 1;
		OPEN c_cursor1;
		FETCH c_cursor1 INTO joined;
		
		WHILE ((NOT c_cursor1::NOTFOUND) AND :i < :searchRow) DO
			i := :i + 1;
			FETCH c_cursor1 INTO joined;
		END WHILE;			
		close c_cursor1;
       
        IF :joined = 'Y' THEN 
            searchUnitSize := 1;
            searchRow := :searchRow + :searchUnitSize;
            CONTINUE;
        END IF;

                  
        CALL CRSP_POIL_INTERNAL_GETINCCOMPAREUNIT(:searchRow, :searchVal, :searchUnitSize);
       
        matchRow := :searchRow + :searchUnitSize;
        WHILE :matchRow <= :recNum DO 
            --CALL CRSP_POIL_INTERNAL_ISJOINED(:matchRow, :joined);
			i := 1;
			OPEN c_cursor2;
			FETCH c_cursor2 INTO joined;
			
			WHILE ((NOT c_cursor2::NOTFOUND) AND :i < :searchRow) DO
				i := :i + 1;
				FETCH c_cursor2 INTO joined;
			END WHILE;			
			close c_cursor2;
			
            IF :joined = 'Y' THEN 
                matchUnitSize := 1;
                matchRow := :matchRow + :matchUnitSize;
                CONTINUE;
            END IF;
            CALL CRSP_POIL_INTERNAL_GETINCCOMPAREUNIT(:matchRow, :matchVal, :matchUnitSize);
            IF :matchUnitSize = :SearchUnitSize AND :searchVal = :matchVal THEN 
                joinRow := 0;
                WHILE :joinRow < :searchUnitSize DO 
                    lineTo := :joinRow + :searchRow;
                    lineFrom := :joinRow + :matchRow;
                    --SELECT 'match ' || CAST(:lineTo AS nvarchar(4)) || ':' || CAST(:lineFrom AS nvarchar(4)) FROM DUMMY;
                    --[Note:Modifier] SAP HANA does not have CONVERT function; you may use CAST function or use implicit casting functions
                    CALL CRSP_POIL_INTERNAL_ADDTONTHLINE(:lineTo, :lineFrom);
                    joinRow := :joinRow + 1;
                END WHILE;
            END IF;
            matchRow := :matchRow + :matchUnitSize;
        END WHILE;
        searchRow := :searchRow + :searchUnitSize;
    END WHILE;

    DELETE FROM "CR_SalesBOMSummaryByItem" WHERE "Joined" = 'Y';
	
    --Result
    SELECT "DocEntry", "Joined", "VisOrder", "TreeType", "VendorNum", "ItemCode", "Dscription", "Quantity", 
        "unitMsr", "Price", "Discprcnt", "PriceBefDi", "LineTotal", "TotalSumSy", "TotalFrgn", "Currency", 
        T0."WhsCode", T1."WhsName" 
    FROM "CR_SalesBOMSummaryByItem" T0 
        LEFT OUTER JOIN OWHS T1 ON T0."WhsCode" = T1."WhsCode";
        
    delete from "CR_BeforeSummaryINV1";
	delete from "CR_SalesBOMSummaryByItem";    
    
END;











