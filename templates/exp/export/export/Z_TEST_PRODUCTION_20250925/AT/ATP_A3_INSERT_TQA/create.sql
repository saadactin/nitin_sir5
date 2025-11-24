-- B1 DEPENDS: AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_A3_INSERT_TQA(
	IN check_id INTEGER,
	IN obj_type INTEGER,
	IN doc_entry INTEGER,
	IN doc_line_num INTEGER,
	IN schd_line INTEGER,
	IN tqa_type SMALLINT, -- 0 normal TQA, 1 invisible TQA (confirmations are already covered by existing OSLD entries)
	IN item NVARCHAR(50),
	IN whs NVARCHAR(8),
	IN cfm_date TIMESTAMP,
	IN cfm_qty DECIMAL(21,6),
	IN req_qty DECIMAL(21,6)
	)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
-- manual manipulation of TQA (caller has to know what he/she does!)
-- inserts one entry with the attributes provided via the interface to table OTQA
-- TODO: replace SELECT ... FROM DUMMY by INSERT
BEGIN
	INSERT INTO OTQA VALUES(:check_id,:obj_type,:doc_entry,:doc_line_num,:schd_line,:tqa_type,:item,:whs,:cfm_date,:cfm_qty,:req_qty
	);
END;











