-- B1 DEPENDS: AFTER:SP:ATP_A8_AGG_TIME_SERIES AFTER:SP:ATP_A9_CANDALGO AFTER:SP:ATP_B1_ADD_TQA AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_D6_GENERAL_RESCHEDULE_GATEWAY (
	IN check_id_for_recheck INTEGER, -- CheckId of initial check of the order that needs donations
	IN item NVARCHAR(50), -- Product
	IN whs NVARCHAR(8), -- Warehouse
	IN check_strategy NVARCHAR(1),   -- D: Delivery Proposal, O: One-Time, C: Complete
	IN includePastReceipt TINYINT,
	OUT check_id INTEGER,
	OUT result OTQA
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
-- A general rescheduling algorithm
AS
	i SMALLINT;
	confirmations_size INTEGER;
	available_qty DECIMAL(21,6);
	-- assume TIMESERIES variable
	CURSOR it1 FOR
		SELECT "date" as "Date", "CanQty" as "Qty", "DocEntry", "DocLineNum", "ObjType"
		FROM OCAN
		ORDER BY "SequenceID" ASC;
BEGIN
	DELETE FROM OTQA WHERE "CheckID" = :check_id_for_recheck;
	SELECT ATP_CHECK_ID.NEXTVAL INTO check_id FROM DUMMY;
	-- The donators are all the entries in the OCAN table
	DONATORS = SELECT o."ObjType", o."DocEntry", o."DocLineNum", "SchdLine", "CfmDate", "CfmQty","CanQty"
			   FROM OSLD o, OCAN oc
			   WHERE o."ObjType" = oc."ObjType" AND o."DocEntry" = oc."DocEntry" AND o."DocLineNum" = oc."DocLineNum";
	-- SELECT * FROM :DONATORS;
	-- read planned goods receipts and issues - for general rescheduling we might also reschedule
	-- the current doc entry so we do not need to exclude it from our query of OSLD
	-- explicitly check against confirmed quantity (2)
	CALL ATP_A8_AGG_TIME_SERIES (:item, :whs, '-1', 0, 0, 2, :includePastReceipt, doc_lines, TS);
	CALL ATP_E0_RECORD_LINES_VERSIONS(:doc_lines);
	-- take confirmations of donators out of the time series
	TIMESERIES = SELECT TO_INT(TO_DATS("Date")) as date, SUM("Qty") as qty FROM
				 ((SELECT "CfmDate" as "Date", "CfmQty" as "Qty" FROM :DONATORS) UNION ALL (SELECT * FROM :TS))
				 GROUP BY "Date" ORDER BY "Date";
	--SELECT * FROM :TIMESERIES;
	-- create empty variable to capture new confirmations
	NEW_CONF =
		SELECT "ObjType", "DocEntry", "DocLineNum", "date" as "CfmDate", "CanQty" as "CfmQty"
		FROM OCAN
		WHERE 1 <> 1;
	-- loop OCAN
	FOR cur_row as it1 DO
		-- get current lineitems
		-- somehow, here, the cursor cannot find the right types, so we explicitly specify them here
		LINEITEMS = SELECT TO_INT(TO_DATS(cur_row."Date")) as Date, TO_DECIMAL(cur_row."Qty", 21, 6) as Qty FROM DUMMY;
		-- ATP check
		CALL ATP_A9_CANDALGO(:TIMESERIES, :LINEITEMS, CONFIRMATIONS); -- call of the L routine that does the ATP check

		IF :check_strategy = 'O' THEN
			-- keep earliest delivery only
			CONFIRMATIONS = SELECT Date, Qty FROM :CONFIRMATIONS Where date = TO_INT(TO_DATS(cur_row."Date"));
			select count(*) INTO confirmations_size FROM :CONFIRMATIONS;
			IF :confirmations_size = 0 THEN
				Continue;
			END IF;
	    ELSEIF :check_strategy = 'C'   THEN
			-- complete delivery (at latest delivery date)
			CONFIRMATIONS = SELECT MAX(date) as Date, cast(SUM(qty) as DECIMAL(21,6)) as Qty FROM :CONFIRMATIONS;
			Select Qty into available_qty From :CONFIRMATIONS;
			If :available_qty IS NULL OR :available_qty < cur_row."Qty" Then
				-- Future receipt couldn't satisfy the requirement of the current document line
				-- According to "Complete" strategy, the document line should donate all it has acquired
				Continue;
			END IF;
	    ELSE
			-- normal case: keep confirmations as they are
			CONFIRMATIONS = SELECT Date, Qty  FROM :CONFIRMATIONS;
	    END IF;
		-- timeseries for next calculation
		TIMESERIES = SELECT date, SUM(qty) as qty FROM
					((SELECT Date, Qty FROM :TIMESERIES) UNION ALL (SELECT Date, -Qty FROM :CONFIRMATIONS))
					GROUP BY date ORDER BY date;
	    -- add to new confirmations
		NEW_CONF = (SELECT * FROM :NEW_CONF)
				   UNION ALL
				   SELECT cast(cur_row."ObjType" as nvarchar(20)), cur_row."DocEntry", cur_row."DocLineNum", TO_DATE(TO_NCHAR(date)) AS "CfmDate", cast(qty as DECIMAL(21,6)) as "CfmQty"
				   FROM :CONFIRMATIONS;
	END FOR;
	TQA = SELECT :check_id as "CheckID", "ObjType", "DocEntry", "DocLineNum", :item as "ItemCode", :whs as "WhsCode", "CfmDate", "CfmQty"
	      FROM :NEW_CONF;
	-- calculate schedule lines
	CONS_TQA = SELECT t1."CheckID", t1."ObjType", t1."DocEntry", t1."DocLineNum", COUNT(*) as "SchdLine" ,
	                  1 as "TQAType", :item as "ItemCode", :whs as "WhsCode", TO_DATE(t1."CfmDate") as "CfmDate", t1."CfmQty"
	           FROM :TQA t1, :TQA t2
	           WHERE t1."ObjType" = t2."ObjType" AND t1."DocEntry" = t2."DocEntry" AND t1."DocLineNum" = t2."DocLineNum"
	           AND t2."CfmDate" <= t1."CfmDate"
	           GROUP BY t1."CheckID", t1."ObjType", t1."DocEntry", t1."DocLineNum",  t1."CfmDate", t1."CfmQty";
	-- write TQAs with type 1
	SELECT *, 0 as "ReqQty"
	FROM :CONS_TQA INTO OTQA;
	-- we need a TQA entry for the donators even when there is nothing to confirm to be able to delete the OSLD entries
	-- of the donator
	SELECT DISTINCT :check_id as "CheckID", r."ObjType", r."DocEntry", r."DocLineNum", 0 as "SchdLine",
			1 as "TQAType", :item as "ItemCode", :whs as "WhsCode", '19000101' as "CfmDate", 0 as "CfmQty", 0 as "ReqQty"
	FROM :DONATORS r LEFT OUTER JOIN :TQA t
	ON r."ObjType" = t."ObjType" AND r."DocEntry" = t."DocEntry" AND r."DocLineNum" = t."DocLineNum"
	WHERE t."DocEntry" is null INTO OTQA;
	-- calculate result
	-- RESULT should includes the new scheduled lines and also the lines without scheduled data.
	-- If there are not enough items in time-series, some document lines may get no scheduled data.
	-- The OTQA table stores no delta data so far.
	RESULT = SELECT * FROM OTQA Where "CheckID" = :check_id;
	-- calculate delta
	NEWCONF = SELECT "CfmDate" as "Date", SUM("CfmQty") as "Qty" FROM :NEW_CONF GROUP BY "CfmDate";
	OLDCONF = SELECT "CfmDate" as "Date", SUM("CfmQty") as "Qty" FROM :DONATORS GROUP BY "CfmDate";
	CALL ATP_B1_ADD_TQA(:NEWCONF, :OLDCONF, ADD_TQA);
	-- write TQAs with type 0
	SELECT :check_id as "CheckID", '' as "ObjType", 0 as "DocEntry", 0 as "DocLineNum", "Counter" as "SchdLine",
	       0 as "TQAType", :item as "ItemCode", :whs as "WhsCode", "Date" as "CfmDate", "Qty" as "CfmQty", 0 as "ReqQty"
	FROM :ADD_TQA INTO OTQA;
END;











