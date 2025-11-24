-- B1 DEPENDS: AFTER:SP:ATP_D7_GENERAL_RESCHEDULE_LOCKED AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_D9_GENERAL_RESCHEDULE (
      IN check_id_for_recheck INTEGER, -- CheckId of initial check of the order that needs donations
      IN item NVARCHAR(50), -- Product
      IN whs NVARCHAR(8), -- Warehouse
      IN check_strategy NVARCHAR(1),   -- D: Delivery Proposal, O: One-Time, C: Complete
      IN includePastReceipt TINYINT,
      IN concurrency_strategy VARCHAR(255) -- to be removed
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
-- A general rescheduling algorithm
AS
	check_id Integer;
BEGIN
      CALL ATP_D7_GENERAL_RESCHEDULE_LOCKED(:check_id_for_recheck, :item, :whs, :check_strategy, :includePastReceipt, check_id, RESULT);
	  SELECT :check_id as "CheckID", S."ObjType", S."DocEntry", S."DocLineNum",
	  		 IFNULL(R."SchdLine", 0) as "SchdLine",
	  		 IFNULL(R."TQAType", 0) as "TQAType",
	   		 :item as "ItemCode", :whs as "WhsCode",
	   		 IFNULL(R."CfmDate", S."date") as "CfmDate",
	   		 IFNULL(R."CfmQty", 0) As "CfmQty"
	      FROM :RESULT R Right Join OCAN S On R."ObjType" = S."ObjType" And R."DocEntry" = S."DocEntry" And R."DocLineNum" = S."DocLineNum"
	      ORDER BY S."SequenceID";
END;











