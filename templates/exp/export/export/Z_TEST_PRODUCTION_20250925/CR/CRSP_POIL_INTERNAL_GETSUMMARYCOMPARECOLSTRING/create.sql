-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables

CREATE PROCEDURE CRSP_POIL_INTERNAL_GETSUMMARYCOMPARECOLSTRING (
	IN itemCode nvarchar(50),
	IN subCatNum nvarchar(20),
	IN description nvarchar(100),
	IN treeType nvarchar(1),
	IN useBaseUn nvarchar(1),
	IN price nvarchar(20),
	IN discPrcnt nvarchar(20),
	IN factor1 nvarchar(20),
	IN factor2 nvarchar(20),
	IN factor3 nvarchar(20),
	IN factor4 nvarchar(20),
	IN priceAfVAT nvarchar(20),
	IN taxOnly nvarchar(1),
	IN unitMsr nvarchar(20),
	IN numPerMsr nvarchar(20),
	IN whsCode nvarchar(20),
	OUT retVal nvarchar(1000)) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
BEGIN
	SELECT :itemCode || ',' || IFNULL(:subCatNum, '') || ',' || IFNULL(:description, '') || ',' || IFNULL(:treeType, '') || ',' || 
		IFNULL(:useBaseUn, '') || ',' || IFNULL(:price, '0') || ',' || 
		IFNULL(:discPrcnt, '0') || ',' || IFNULL(:factor1, '0') || ',' ||
		IFNULL(:factor2, '0') || ',' || IFNULL(:factor3, '0') || ',' || 
		IFNULL(:factor4, '0') || ',' || 
		IFNULL(:priceAfVAT, '0') || ',' || :taxOnly || ',' || IFNULL(:unitMsr, '') || ',' || 
		IFNULL(:numPerMsr, '0') || ',' || IFNULL(:whsCode, '') INTO retVal FROM DUMMY;
END;











