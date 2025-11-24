-- B1 DEPENDS: AFTER:SP:ATP_B1_ADD_TQA AFTER:SP:ATP_A3_EXISTING_REQUIREMENTS AFTER:SP:ATP_A8_AGG_TIME_SERIES AFTER:SP:ATP_C5_READ_DONATORS AFTER:SP:ATP_A9_CANDALGO AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:SP:ATP_C2_QTY_WORKAROUND AFTER:PT:PROCESS_END


CREATE PROCEDURE ATP_C6_RESCHEDULE_GATEWAY(
       IN check_id_for_recheck INTEGER, -- CheckId of initial check of the order that needs donations
       IN obj_type NVARCHAR(20), -- ObjType of winner
       IN doc_entry INTEGER, -- DocEntry of winner
       IN doc_line_num INTEGER, -- DocLineNum of winner
       IN item NVARCHAR(50), -- Product
       IN whs NVARCHAR(8), -- Warehouse
       IN date DATE, -- requested date of winner
       IN qty DECIMAL(21,6), -- requested quantity of winner
       IN check_strategy NVARCHAR(1),   -- D: Delivery Proposal, O: One-Time, C: Complete
       IN includePastReceipt TINYINT,
       OUT RESULT OTQA
       )
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	-- recheck one order (called "winner") that has not got a full confirmation after taking back
	-- confirmations of orders in table ODON (called "donators")
	-- donators are rechecked afterwards
	check_id INTEGER;
	i SMALLINT;
	num_recheck SMALLINT;
	confirmations_size INTEGER;
	available_qty DECIMAL(21,6);
	don_obj_type NVARCHAR(20);
	don_doc_entry INTEGER;
	don_doc_line_num INTEGER;
	doc_line_ReqQty DECIMAL(21,6);
BEGIN
	DELETE FROM OTQA WHERE "CheckID" = :check_id_for_recheck;
	SELECT ATP_CHECK_ID.NEXTVAL INTO check_id FROM DUMMY;
	-- TODO: replace SELECT ... FROM DUMMY INTO by INSERT
	SELECT :check_id as "CheckID", :item as "ItemCode", :whs as "WarehouseCode", :date as "Date", :qty as "Qty"
	FROM DUMMY INTO ATP_CHECKS;
	-- workaround because of bug 13906 (see details in comments of procedure)
	CALL ATP_C2_QTY_WORKAROUND(:date, :qty, LINEITEMS);
	
	-- process donators
	CALL ATP_C5_READ_DONATORS(num_recheck, DONATORS, NUMB_RECHECK, STABLE_CONF);
	-- read planned goods receipts and issues
	-- explicitly check against confirmed quantity (2) chosen
	CALL ATP_A8_AGG_TIME_SERIES (:item, :whs, :obj_type, :doc_entry, :doc_line_num, 2, :includePastReceipt, doc_lines, TS);
	CALL ATP_E0_RECORD_LINES_VERSIONS(:doc_lines);
	-- take confirmations of donators out of the time series
	TIMESERIES = SELECT TO_INT(TO_DATS("Date")) as date, cast(SUM("Qty") as DECIMAL(21,6)) as qty
				 FROM (
				 		(SELECT "CfmDate" as "Date", "DonQty" as "Qty" FROM :DONATORS)
				 		UNION ALL
				 		(SELECT * FROM :TS)
				 )
	             GROUP BY "Date" ORDER BY "Date";
	-- execute ATP check for "winner"
	CALL ATP_A9_CANDALGO(:TIMESERIES, :LINEITEMS, CONFIRMATIONS); -- call of the L routine that does the ATP check
	IF :check_strategy = 'O' THEN
		-- keep earliest delivery only
		CONFIRMATIONS = SELECT Date, Qty FROM :CONFIRMATIONS Where Date = To_Int(TO_Dats(:Date));
	ELSEIF :check_strategy = 'C'   THEN
		-- complete delivery (at latest delivery date)
		CONFIRMATIONS = SELECT MAX(date) as Date, cast(SUM(qty) as DECIMAL(21,6)) as Qty FROM :CONFIRMATIONS;
		Select Qty into available_qty From :CONFIRMATIONS;
		If :available_qty < :Qty Then
			-- Future receipt couldn't satisfy the requirement of the current document line
			-- According to "Complete" strategy, the document line should donate all it has acquired
			CONFIRMATIONS = Select Date, Qty From :CONFIRMATIONS Where 1 <> 1;
		END IF;
	ELSE
		-- normal case: keep confirmations as they are
		CONFIRMATIONS = SELECT Date, Qty  FROM :CONFIRMATIONS;
	END IF;
	ALL_CONFIRMATIONS = SELECT :obj_type AS "ObjType", :doc_entry as "DocEntry", :doc_line_num as "DocLineNum",
			TO_DATE(TO_NCHAR(date)) as "CfmDate", qty as "CfmQty" FROM :CONFIRMATIONS;
	TIMESERIES = SELECT date, cast(SUM(qty) as DECIMAL(21,6)) as qty
				 FROM (
				 		(SELECT date, qty*(-1) as qty FROM :CONFIRMATIONS)
						UNION ALL
						(SELECT * FROM :TIMESERIES)
				)
				GROUP BY date ORDER BY date;
-- recheck donators
	FOR i IN 0 .. num_recheck - 1 DO
          SELECT "ObjType", "DocEntry", "DocLineNum" INTO don_obj_type, don_doc_entry, don_doc_line_num
          FROM :NUMB_RECHECK WHERE "Counter" = :i;
          CALL ATP_A3_EXISTING_REQUIREMENTS(:don_obj_type, :don_doc_entry, :don_doc_line_num, :item, :whs, DON_REQ_QTY,DON_CFM_QTY);
          -- local version of timeseries for this iteration, which includes the reserved qtys of this donator (we dont want to block his own qty)
          ITERATION_TIMESERIES = SELECT date, cast(SUM(qty) as DECIMAL(21,6)) as qty
                                FROM ((SELECT TO_INT(TO_DATS("Date")) as DATE, "Qty" as QTY FROM :DON_CFM_QTY)
                                        UNION ALL
                                       (SELECT * FROM :TIMESERIES)
                                        UNION ALL
                                       (SELECT TO_INT(TO_DATS("CfmDate")) as date, "DonQty"*(-1) as qty
                                        FROM :DONATORS
                                        WHERE "ObjType" = :don_obj_type AND "DocEntry" = :don_doc_entry AND "DocLineNum" = :don_doc_line_num))
                                GROUP BY date ORDER BY date;
          DONATOR_REQUIREMENTS = SELECT TO_INT(TO_DATS("Date")) as DATE, "Qty" as QTY FROM :DON_REQ_QTY;
          CALL ATP_A9_CANDALGO(:ITERATION_TIMESERIES, :DONATOR_REQUIREMENTS, CONFIRMATIONS);

		IF :check_strategy = 'O' THEN
			-- keep earliest delivery only
			CONFIRMATIONS = SELECT T0.Date, T0.Qty FROM :CONFIRMATIONS T0, :DONATOR_REQUIREMENTS T1 Where T0.Date = T1.Date;
			select count(*) INTO confirmations_size FROM :CONFIRMATIONS;
			IF :confirmations_size = 0 THEN
				Continue;
			END IF;
	    ELSEIF :check_strategy = 'C'   THEN
			-- complete delivery (at latest delivery date)
			CONFIRMATIONS = SELECT MAX(date) as Date, cast(SUM(qty) as DECIMAL(21,6)) as Qty FROM :CONFIRMATIONS;
			Select Qty into available_qty From :CONFIRMATIONS;
			Select Qty Into doc_line_ReqQty From :DONATOR_REQUIREMENTS;
			If :available_qty < :doc_line_ReqQty Then
				-- Future receipt couldn't satisfy the requirement of the current document line
				-- According to "Complete" strategy, the document line should donate all it has acquired
				Continue;
			END IF;
	    ELSE
			-- normal case: keep confirmations as they are
			CONFIRMATIONS = SELECT Date, Qty  FROM :CONFIRMATIONS;
	    END IF;
		TIMESERIES = (SELECT * FROM :ITERATION_TIMESERIES)
					UNION ALL
					(SELECT date, cast(qty*(-1) as DECIMAL(21,6)) as qty FROM :CONFIRMATIONS);
		-- append confirmations to all_confirmations
		ALL_CONFIRMATIONS = SELECT "ObjType", "DocEntry", "DocLineNum", "CfmDate", cast(sum("CfmQty") as DECIMAL(21,6)) as "CfmQty"
							FROM (
									(SELECT * FROM :ALL_CONFIRMATIONS)
									UNION ALL
									(SELECT :don_obj_type as "ObjType", :don_doc_entry as "DocEntry", :don_doc_line_num as "DocLineNum",
									 TO_DATE(TO_NCHAR(date)) as "CfmDate", qty as "CfmQty"	FROM :CONFIRMATIONS)
							)
							GROUP BY "ObjType", "DocEntry", "DocLineNum", "CfmDate";
END FOR;
	-- create TQA of type 1 (consolidated confirmations of all orders merged with stable confirmations)
	NEW_TQA = SELECT :check_id as "CheckID", "ObjType", "DocEntry", "DocLineNum", "CfmDate", "CfmQty"
    	  FROM :ALL_CONFIRMATIONS;
-- calculate schedule line number
	CONS_TQA = SELECT t1."CheckID", t1."ObjType", t1."DocEntry", t1."DocLineNum", COUNT(*) as "SchdLine" ,
	                  1 as "TQAType", :item as "ItemCode", :whs as "WhsCode", TO_DATE(t1."CfmDate") as "CfmDate", t1."CfmQty"
	           FROM :NEW_TQA t1, :NEW_TQA t2
	           WHERE t1."ObjType" = t2."ObjType" AND t1."DocEntry" = t2."DocEntry" AND t1."DocLineNum" = t2."DocLineNum"
	           AND t2."CfmDate" <= t1."CfmDate"
	           GROUP BY t1."CheckID", t1."ObjType", t1."DocEntry", t1."DocLineNum",  t1."CfmDate", t1."CfmQty";
      -- write TQA of type 1 to DB table
	SELECT *, 0 as "ReqQty" FROM :CONS_TQA INTO OTQA;
     -- we need a TQA entry for the donators even when there is nothing to confirm to be able to delete the OSLD entries
      -- of the donator
	Insert Into OTQA
		SELECT :check_id as "CheckID", "ObjType", "DocEntry", "DocLineNum", 0 as "SchdLine", 1 as "TQAType",
				:item as "ItemCode", :whs as "WhsCode", '19000101' as "CfmDate", 0 as "CfmQty", 0 as "ReqQty"
		FROM :NUMB_RECHECK
		Where ("ObjType", "DocEntry", "DocLineNum") Not In (Select Distinct "ObjType", "DocEntry", "DocLineNum" From :NEW_TQA);
	---------------------------------------------------------------
	-- calculate output which return all TQA of type 1
	RESULT = Select * from OTQA where "CheckID" = :check_id;
	---------------------------------------------------------------
-- delta calculation of cfm qty calculate TQA of type 0 (additional TQA that need to be seen by other transactions) and write them to DB table OTQA
      NEW_TQA_GROUPED_BY_DAY = SELECT "CfmDate" as "Date", SUM("CfmQty") as "Qty"
                    FROM :NEW_TQA
                    GROUP BY "CfmDate";
      OLDCONF = SELECT o."CfmDate" as "Date", SUM(o."CfmQty") as "Qty"
                   FROM OSLD o JOIN :ALL_CONFIRMATIONS c
                   ON (o."ObjType" = c."ObjType" AND o."DocEntry" = c."DocEntry" AND o."DocLineNum" = c."DocLineNum")
                   GROUP BY o."CfmDate";
      CALL ATP_B1_ADD_TQA(:NEW_TQA_GROUPED_BY_DAY, :OLDCONF, ADD_TQA);
-- delta calculation of req qty
	NEWCONF_REQ = SELECT :date as "Date", :qty as "Qty" FROM DUMMY;
	CALL ATP_A3_EXISTING_REQUIREMENTS(:obj_type, :doc_entry, :doc_line_num, :item, :whs, OLDCONF_REQ, OLDCONF_CFM);
	CALL ATP_B1_ADD_TQA(:NEWCONF_REQ, :OLDCONF_REQ, ADD_TQA_REQ);
-- merge cfm qty with req qty & write OTQA
	OTQA_TEMP = SELECT
		:check_id as "CheckID",
		-- not needed
		0 as "ObjType",
		0 as "DocEntry",
		0 as "DocLineNum",
		0 as "TQAType",
		:item as "ItemCode",
		:whs as "WhsCode",
		CASE WHEN c."Date" is null THEN r."Date" ELSE c."Date" END as "CfmDate",
		CASE WHEN c."Qty" is null THEN 0 ELSE c."Qty" END as "CfmQty",
		CASE WHEN r."Qty" is null THEN 0 ELSE r."Qty" END AS "ReqQty"
	FROM :ADD_TQA c FULL OUTER JOIN :ADD_TQA_REQ r ON c."Date" = r."Date";
	OTQA = CE_PROJECTION(:OTQA_TEMP,
	["CheckID", "ObjType", "DocEntry", "DocLineNum", CE_CALC('rownum()', smallint) as "SchdLine",
	 "TQAType", "ItemCode", "WhsCode", "CfmDate", "CfmQty", "ReqQty"
	]);
	SELECT "CheckID", "ObjType", "DocEntry", "DocLineNum", "SchdLine" + 1, -- make SchdLine start from 1
	 "TQAType", "ItemCode", "WhsCode", "CfmDate", "CfmQty", "ReqQty"
	 FROM :OTQA INTO OTQA;
END;











