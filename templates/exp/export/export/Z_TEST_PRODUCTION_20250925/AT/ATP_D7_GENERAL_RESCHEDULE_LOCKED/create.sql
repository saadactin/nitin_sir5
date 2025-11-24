-- B1 DEPENDS: AFTER:SP:ATP_D6_GENERAL_RESCHEDULE_GATEWAY AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_D7_GENERAL_RESCHEDULE_LOCKED(
       IN check_id_for_recheck INTEGER, -- CheckId of initial check of the order that needs donations
       IN item NVARCHAR(50), -- Product
       IN whs NVARCHAR(8), -- Warehouse
       IN check_strategy NVARCHAR(1),   -- D: Delivery Proposal, O: One-Time, C: Complete
       IN includePastReceipt TINYINT,
       OUT check_id INTEGER,
       OUT RESULT OTQA
       )
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
BEGIN
      SELECT * FROM ATP_LOCK WHERE "ItemCode" = :item AND "WhsCode" = :whs FOR UPDATE;
     CALL ATP_D6_GENERAL_RESCHEDULE_GATEWAY(:check_id_for_recheck, :item, :whs, :check_strategy, :includePastReceipt, check_id, RESULT);
END;











