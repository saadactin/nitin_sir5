-- B1 DEPENDS: AFTER:SP:ATP_C7_RESCHEDULE_LOCKED AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_C9_RESCHEDULE(
       IN check_id_for_recheck INTEGER, -- CheckId of initial check of the order that needs donations
       IN obj_type NVARCHAR (20),
       IN doc_entry INTEGER,
       IN doc_line_num INTEGER,
       IN item NVARCHAR(50), -- Product
       IN whs NVARCHAR(8), -- Warehouse
       IN date DATE,
       IN qty DECIMAL(21,6),
       IN check_strategy NVARCHAR(1),   -- D: Delivery Proposal, O: One-Time, C: Complete
       IN includePastReceipt TINYINT,
       IN concurrency_strategy VARCHAR(255) -- `concurrency_strategy` : supported values are `'optimistic'` and `'locked'`
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
BEGIN
	IF :concurrency_strategy = 'optimistic'
 	THEN
 		-- optimistic procedure is still under construction, we use locked version instead
  		CALL ATP_C8_RESCHEDULE_OPTIMISTIC(:check_id_for_recheck, :obj_type, :doc_entry, :doc_line_num, :item, :whs, :date, :qty, :check_strategy, :includePastReceipt, RESULT);
 	ELSE
  		CALL ATP_C7_RESCHEDULE_LOCKED(:check_id_for_recheck, :obj_type, :doc_entry, :doc_line_num, :item, :whs, :date, :qty, :check_strategy, :includePastReceipt, RESULT);
 	END IF;
 	SELECT "CheckID","ObjType","DocEntry","DocLineNum",
 		"SchdLine","TQAType",
 	"ItemCode","WhsCode","CfmDate","CfmQty" FROM :RESULT ORDER BY "DocEntry", "CfmDate"; -- sorting is only for test

END;











