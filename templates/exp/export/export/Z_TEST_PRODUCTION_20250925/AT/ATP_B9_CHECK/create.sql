-- B1 DEPENDS: AFTER:SP:ATP_B4_CHECK_LOCKED AFTER:SP:ATP_A4_CLEANUP_TQA AFTER:SP:ATP_B7_CHECK_OPTIMISTIC AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_B9_CHECK (
       IN obj_type NVARCHAR (20),
       IN item NVARCHAR(50), -- Product
       IN whs NVARCHAR(8), -- Warehouse
       IN date DATE,
       IN qty DECIMAL(21,6),
       IN concurrency_strategy VARCHAR(255), -- `concurrency_strategy` : supported values are `'optimistic'` and `'locked'`
       IN check_id_for_recheck INTEGER, -- check_id of previous check in the same transaction (0 for first check)
       IN doc_entry INTEGER, -- only needed for recheck (0 for first check)
       IN doc_line_num INTEGER, -- only needed for recheck (0 for first check)
       IN check_type INTEGER, -- `check_type` : supported values are 1 (=check against required quantities) and 2 (=check against confirmed quantities)
       IN includePastReceipt TINYINT -- 0: Exclude, 1: Include
       -- check_id of lines in table `TEMPORARY_QUANTITY_ASSIGNMENTS`
       --(needed to remove TQAs when order is added and for recheck in same transaction
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
check_id INTEGER;
BEGIN
    CALL ATP_A4_CLEANUP_TQA(3600); -- deletes all TQA that are older than 1 hour
	LINEITEMS = SELECT :date as "Date", :qty as "Qty" FROM DUMMY;
    IF :concurrency_strategy = 'optimistic'
    THEN
        CALL ATP_B7_CHECK_OPTIMISTIC(:obj_type, :item, :whs, :check_id_for_recheck, :doc_entry, :doc_line_num, :LINEITEMS, :check_type, :includePastReceipt, check_id, RESULT);
        SELECT :check_id as "CheckID", "Date", "Qty" FROM :RESULT;
    ELSE
        CALL ATP_B4_CHECK_LOCKED(:obj_type, :item, :whs, :check_id_for_recheck, :doc_entry, :doc_line_num, :LINEITEMS, :check_type, :includePastReceipt, check_id, RESULT);
        SELECT :check_id as "CheckID", "Date", "Qty" FROM :RESULT;
    END IF;
END;











