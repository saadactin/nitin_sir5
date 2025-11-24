-- B1 DEPENDS: AFTER:SP:ATP_B5_CHECK_OPTI_S1 AFTER:SP:ATP_B6_CHECK_OPTI_S2 AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_B7_CHECK_OPTIMISTIC(
    IN obj_type NVARCHAR (20),
    IN item NVARCHAR(50),
    IN whs NVARCHAR(8),
    IN check_id_for_recheck INTEGER,
    IN doc_entry INTEGER,
    IN doc_line_num INTEGER,
    IN LINEITEMS ATP_DATE_QTY,
    IN check_type INTEGER,
    IN includePastReceipt TINYINT,
    OUT check_id INTEGER,
    OUT result ATP_DATE_QTY
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
BEGIN
    CALL ATP_B5_CHECK_OPTI_S1(:obj_type, :item, :whs, :check_id_for_recheck, :doc_entry, :doc_line_num, :LINEITEMS, :check_type, :includePastReceipt, check_id, RESULT1);
     
    CALL ATP_B6_CHECK_OPTI_S2(:obj_type, :item, :whs, :check_id_for_recheck, :doc_entry, doc_line_num, :RESULT1, :check_id, :check_type, :includePastReceipt, RESULT);
END;











