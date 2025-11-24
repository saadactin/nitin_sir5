-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_C0_ADD_CONFIRMATIONS(
IN check_id INTEGER,
IN doc_entry INTEGER, -- only used when initial in OTQA
IN doc_line_num INTEGER -- only used when DocEntry is initial in OTQA
)

LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
tqa_type SMALLINT;
obj_type NVARCHAR(20);
-- to be used when user accepts the check result
-- copy TQA from OTQA to OSLD, delete TQA
BEGIN
-- if there are TQAs of types 0 and 1, we copy only type 1
	SELECT max("TQAType") INTO tqa_type FROM OTQA WHERE "CheckID" = :check_id;
-- delete existing confirmations (new confirmations replace the existing ones)
	DELETE FROM OSLD WHERE ("ObjType","DocEntry", "DocLineNum") IN
	(SELECT DISTINCT "ObjType","DocEntry", "DocLineNum" FROM OTQA WHERE "CheckID" = :check_id AND "TQAType" = :tqa_type);
    SELECT OSLD_S.NEXTVAL as "AbsEntry", "ObjType",
    CASE WHEN "DocEntry" = 0 THEN :doc_entry ELSE "DocEntry" END as "DocEntry",
    CASE WHEN "DocEntry" = 0 THEN :doc_line_num ELSE "DocLineNum" END as "DocLineNum",
    "SchdLine", "ItemCode", "WhsCode", "CfmDate", "CfmQty", 'N' as "FixedCfm", "ReqQty"
    FROM OTQA WHERE "CheckID" = :check_id AND "TQAType" = :tqa_type AND "CfmQty" != 0 INTO OSLD;
    DELETE FROM OTQA WHERE "CheckID" = :check_id;

END;











