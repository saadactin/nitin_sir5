-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables

CREATE PROCEDURE CRSP_POIL_INTERNAL_GETDOCNTHLINE(
	IN recOffset integer,
	IN isSearchTable integer,
	OUT itemCode nvarchar(50),
	OUT subCatNum nvarchar(20),
	OUT description nvarchar(100),
	OUT treeType nvarchar(1),
	OUT useBaseUn nvarchar(1),
	OUT price DECIMAL(21,6),
	OUT discPrcnt DECIMAL(21,6),
	OUT factor1 DECIMAL(21,6),
	OUT factor2 DECIMAL(21,6),
	OUT factor3 DECIMAL(21,6),
	OUT factor4 DECIMAL(21,6),
	OUT priceAfVAT DECIMAL(21,6),
	OUT taxOnly nvarchar(1),
	OUT unitMsr nvarchar(20),
	OUT numPerMsr DECIMAL(21,6),
	OUT whsCode nvarchar(20),
	OUT joined nvarchar(1),
	OUT quantity DECIMAL(21,6),
	OUT priceBefDi DECIMAL(21,6),
	OUT lineTotal DECIMAL(21,6),
	OUT totalSumSy DECIMAL(21,6),
	OUT totalFrgn DECIMAL(21,6)
) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	i int;	
	
	CURSOR c_cursor1 FOR SELECT "ItemCode", "SubCatNum", "Dscription", "TreeType", "UseBaseUn", "Price", "DiscPrcnt", "Factor1", "Factor2",
		 "Factor3", "Factor4", "PriceAfVAT", "TaxOnly", "unitMsr", "NumPerMsr", "WhsCode", "Joined", "Quantity", "PriceBefDi", "LineTotal",
		 "TotalSumSy", "TotalFrgn" FROM "CR_BeforeSummaryINV1" ORDER BY "VisOrder";
BEGIN
	i := 1;
	OPEN c_cursor1;
	FETCH c_cursor1 INTO itemCode, subCatNum, description, treeType, useBaseUn, 
		price, discPrcnt, factor1, factor2, factor3, factor4, priceAfVAT, taxOnly, unitMsr, numPerMsr, whsCode, joined, 
		quantity, priceBefDi, lineTotal, totalSumSy, totalFrgn;

	WHILE ((NOT c_cursor1::NOTFOUND) AND :i < :recOffset) DO
		i := :i + 1;
		FETCH c_cursor1 INTO itemCode, subCatNum, description, treeType, useBaseUn, 
			price, discPrcnt, factor1, factor2, factor3, factor4, priceAfVAT, taxOnly, unitMsr, numPerMsr, whsCode, joined, 
			quantity, priceBefDi, lineTotal, totalSumSy, totalFrgn;
	END WHILE;	
 	
 	CLOSE c_cursor1;
END;











