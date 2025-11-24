-- B1 DEPENDS: AFTER:SP:ATP_C6_RESCHEDULE_GATEWAY AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_C7_RESCHEDULE_LOCKED(
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
       OUT RESULT OTQA
       )
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
BEGIN
	SELECT * FROM ATP_LOCK WHERE "ItemCode" = :item AND "WhsCode" = :whs FOR UPDATE;
 	CALL ATP_C6_RESCHEDULE_GATEWAY(:check_id_for_recheck, :obj_type, :doc_entry, :doc_line_num, :item, :whs, :date, :qty, :check_strategy, :includePastReceipt, RESULT);
END;











