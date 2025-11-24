-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_A2_NORM_TQA(
    IN item NVARCHAR(50),
    IN whs NVARCHAR(8),
    IN check_type INTEGER,
    OUT doc_lines DOC_LINES_VERSIONS,
    OUT result ATP_DATE_QTY
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
READS SQL DATA
AS
-- read TQA from the database
-- quantities in OTQA are positive -> multiply by -1 to get negative quantities
-- only TQAType 0 is relevant for the ATP calculation
-- If check_type == 1 we check against required quantities
-- else if :check_type == 2 --check against confirmed quantities
BEGIN
    docline_series  = 	SELECT "ObjType", "DocEntry", "DocLineNum", TO_DATE("CfmDate") as "Date",
            	CASE WHEN :check_type = 1 THEN -1*"ReqQty" ELSE -1*"CfmQty" END as "Qty"
               	FROM OTQA
               	WHERE "ItemCode" = :item AND "WhsCode" = :whs AND "TQAType" = 0;
	doc_lines = Select "ObjType", "DocEntry", "DocLineNum", 0 as "Version" From :docline_series;
	RESULT = SELECT "Date", cast("Qty" as DECIMAL(21,6)) as "Qty" From :docline_series;
END;











