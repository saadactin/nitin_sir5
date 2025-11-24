-- B1 DEPENDS: AFTER:SP:CRSP_POIL_INTERNAL_GETDOCNTHLINE

CREATE PROCEDURE CRSP_POIL_INTERNAL_ADDTONTHLINE (IN lineTo integer, IN lineFrom integer) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
itemCode nvarchar(50);
subCatNum nvarchar(20);
description nvarchar(100);
treeType nvarchar(1);
useBaseUn nvarchar(1);
price DECIMAL(21,6);
discPrcnt DECIMAL(21,6);
factor1 DECIMAL(21,6);
factor2 DECIMAL(21,6);
factor3 DECIMAL(21,6);
factor4 DECIMAL(21,6);
priceAfVAT DECIMAL(21,6);
taxOnly nvarchar(1);
unitMsr nvarchar(20);
numPerMsr DECIMAL(21,6);
whsCode nvarchar(20);
joined nvarchar(1);
quantity DECIMAL(21,6);
priceBefDi DECIMAL(21,6);
lineTotal DECIMAL(21,6);
totalSumSy DECIMAL(21,6);
totalFrgn DECIMAL(21,6);

BEGIN 
	CALL CRSP_POIL_INTERNAL_GETDOCNTHLINE(:lineFrom, 1, :itemCode, :subCatNum, :description, :treeType, :useBaseUn, :price, :discPrcnt,
	 :factor1, :factor2, :factor3, :factor4, :priceAfVAT, :taxOnly, :unitMsr, :numPerMsr, :whsCode, :joined,
     :quantity, :priceBefDi, :lineTotal, :totalSumSy, :totalFrgn);

    UPDATE "CR_SalesBOMSummaryByItem" SET "Quantity" = "Quantity" + :quantity, "LineTotal" = "LineTotal" +
         :lineTotal, "TotalSumSy" = "TotalSumSy" + :totalSumSy, "TotalFrgn" = "TotalFrgn" + :totalFrgn 
    WHERE "VisOrder" = :lineTo - 1;

    UPDATE "CR_SalesBOMSummaryByItem" SET "Joined" = 'Y' Where "VisOrder" = :lineFrom -1;
END;











