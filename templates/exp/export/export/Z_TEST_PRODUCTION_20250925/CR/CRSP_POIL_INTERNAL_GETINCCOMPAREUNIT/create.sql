-- B1 DEPENDS: AFTER:SP:CRSP_POIL_INTERNAL_GETNUMOFINGREDIENTS AFTER:SP:CRSP_POIL_INTERNAL_GETDOCNTHLINE AFTER:SP:CRSP_POIL_INTERNAL_GETSUMMARYCOMPARECOLSTRING

CREATE PROCEDURE CRSP_POIL_INTERNAL_GETINCCOMPAREUNIT (
	IN paramStartLine integer, 
	OUT outparamString nvarchar(5000), 
	OUT outparamNumOfLines integer) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
	inc integer;
	p_itemCode nvarchar(50);
	p_subCatNum nvarchar(20);
	p_description nvarchar(100);
	p_treeType nvarchar(1);
	p_useBaseUn nvarchar(1);
	p_price DECIMAL(21,6);
	p_discPrcnt DECIMAL(21,6);
	p_factor1 DECIMAL(21,6);
	p_factor2 DECIMAL(21,6);
	p_factor3 DECIMAL(21,6);
	p_factor4 DECIMAL(21,6);
	p_priceAfVAT DECIMAL(21,6);
	p_taxOnly nvarchar(1);
	p_unitMsr nvarchar(20);
	p_numPerMsr DECIMAL(21,6);
	p_whsCode nvarchar(20);
	p_joined nvarchar(1);
	p_quantity DECIMAL(21,6);
	p_priceBefDi DECIMAL(21,6);
	p_lineTotal DECIMAL(21,6);
	p_totalSumSy DECIMAL(21,6);
	p_totalFrgn DECIMAL(21,6);
	lineNumber integer;
	summaryCompareColString nvarchar(1000);
BEGIN 
	CALL CRSP_POIL_INTERNAL_GETNUMOFINGREDIENTS(:paramStartLine, :outparamNumOfLines);

    outparamNumOfLines := :outparamNumOfLines + 1;
    inc := 0;
    outparamString := '';
    WHILE :inc < :outparamNumOfLines DO 
        lineNumber := :inc +:paramStartLine;
		
        CALL CRSP_POIL_INTERNAL_GETDOCNTHLINE(:lineNumber, 1, :p_itemCode, :p_subCatNum, :p_description, :p_treeType, :p_useBaseUn, :p_price,
			:p_discPrcnt, :p_factor1, :p_factor2, :p_factor3, :p_factor4, :p_priceAfVAT, :p_taxOnly, :p_unitMsr, :p_numPerMsr,
			:p_whsCode, :p_joined, :p_quantity, :p_priceBefDi, :p_lineTotal, :p_totalSumSy, :p_totalFrgn);
	
		CALL CRSP_POIL_INTERNAL_GETSUMMARYCOMPARECOLSTRING(IFNULL(:p_itemCode, ''), IFNULL(:p_subCatNum, ''), IFNULL(:p_description, ''), IFNULL(:p_treeType, ''), IFNULL(:p_useBaseUn, ''), IFNULL(:p_price, '0'), IFNULL(:p_discPrcnt, '0'),
			IFNULL(:p_factor1, '0'), IFNULL(:p_factor1, '0'), IFNULL(:p_factor1, '0'), IFNULL(:p_factor1, '0'), IFNULL(:p_priceAfVAT, '0'), :p_taxOnly, IFNULL(:p_unitMsr, ''), IFNULL(:p_numPerMsr, '0'), IFNULL(:p_whsCode,''), :summaryCompareColString);	
	
        inc := :inc + 1;
        outparamString := :outparamString || :summaryCompareColString;
    END WHILE;
END;











