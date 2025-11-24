-- B1 DEPENDS: AFTER:SP:ATP_C6_RESCHEDULE_GATEWAY AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_C8_RESCHEDULE_OPTIMISTIC(
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
	commit_id INT := 0;
BEGIN
	-- under construction!!!
	  CALL ATP_C6_RESCHEDULE_GATEWAY(:check_id_for_recheck, :obj_type, :doc_entry, :doc_line_num, :item, :whs, :date, :qty, :check_strategy, :includePastReceipt, RESULT);
	  SELECT MAX(COMMIT_ID) INTO commit_id FROM TRANSACTION_HISTORY
	  WHERE COMMIT_ID < (SELECT LAST_COMMIT_ID FROM M_TRANSACTIONS WHERE CONNECTION_ID = CURRENT_CONNECTION AND PORT like '%03');
	  EXEC('SET HISTORY SESSION TO COMMIT ID ' || :commit_id);
	  CALL ATP_C6_RESCHEDULE_GATEWAY(:check_id_for_recheck, :obj_type, :doc_entry, :doc_line_num, :item, :whs, :date, :qty, :check_strategy, :includePastReceipt, RESULT);
END;











