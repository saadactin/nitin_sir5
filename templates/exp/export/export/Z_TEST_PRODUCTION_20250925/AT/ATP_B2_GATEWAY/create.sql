-- B1 DEPENDS: AFTER:SP:ATP_B1_ADD_TQA AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:SP:ATP_A3_EXISTING_REQUIREMENTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_B2_GATEWAY(
    IN obj_type NVARCHAR (20),
    IN check_id INTEGER,
    IN item NVARCHAR(50),
    IN whs NVARCHAR(8),
    IN check_strategy INTEGER,
    IN check_id_for_recheck INTEGER,
    IN doc_entry INTEGER,
    IN doc_line_num INTEGER,
    IN LINEITEMS ATP_INT_QTY,
    IN check_type INTEGER,
    IN includePastReceipt TINYINT,
    OUT RESULT ATP_DATE_QTY
)
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER
AS
tqa_type SMALLINT;
resSize Integer;
BEGIN
	DELETE FROM OTQA WHERE "CheckID" = :check_id_for_recheck;
    CALL ATP_A8_AGG_TIME_SERIES (:item, :whs, :obj_type, :doc_entry, :doc_line_num, :check_type, :includePastReceipt, doc_lines, TS);
    CALL ATP_E0_RECORD_LINES_VERSIONS(:doc_lines);
    TIMESERIES = SELECT TO_INT(TO_DATS("Date")) AS date, "Qty" AS qty FROM :ts WHERE "Qty" != 0 ORDER BY "Date";
    CALL ATP_A9_CANDALGO(:TIMESERIES, :LINEITEMS, CONFIRMATIONS);
-- adapt confirmations according to check strategy (not used at the moment, strategy 1 is hard coded)
    IF :check_strategy = 1
-- normal case: keep confirmations as they are
    THEN
    RESULT = SELECT TO_DATE(TO_NCHAR(date)) as "Date", qty as "Qty" FROM :CONFIRMATIONS;
    ELSEIF :check_strategy = 2
    THEN
-- keep earliest delivery only
    RESULT = SELECT TOP 1 TO_DATE(TO_NCHAR(date)) as "Date", qty as "Qty" FROM :CONFIRMATIONS ORDER BY date;
    ELSE
-- complete delivery (at latest delivery date)
    RESULT = SELECT MAX(TO_DATE(TO_NCHAR(date))) as "Date", cast(SUM(qty) as DECIMAL(21,6)) as "Qty" FROM :CONFIRMATIONS;
    END IF;
-- read existing confirmations for checked document
   LINEITEMS_TMP = SELECT TO_DATE(TO_NCHAR(date)) as "Date", qty as "Qty" FROM :LINEITEMS;
   TQA_TMP = SELECT
        CASE WHEN o."Date" is null THEN n."Date" ELSE o."Date" END AS "Date",
        CASE WHEN n."Qty" is null THEN 0 ELSE n."Qty" END AS "ReqQty",
        CASE WHEN o."Qty" is null THEN 0 ELSE o."Qty" END AS "CfmQty"
        FROM :LINEITEMS_TMP n FULL OUTER JOIN :RESULT o ON n."Date" = o."Date";
-- create schedule line number
    TQA = CE_PROJECTION(:TQA_TMP, [CE_CALC('rownum()', integer) as "SchdLine", "Date", "ReqQty", "CfmQty"]);
	CALL ATP_A3_EXISTING_REQUIREMENTS(:obj_type, :doc_entry, :doc_line_num, :item, :whs, OLDCONF_REQ, OLDCONF_CFM);
-- TQAType must be 1 when there are already confirmations (otherwise it is 0)
    SELECT COUNT(*) INTO tqa_type FROM (SELECT TOP 1 * FROM (SELECT * FROM :OLDCONF_REQ UNION ALL SELECT * FROM :OLDCONF_CFM));
-- write TQA of type tqa_type (0 if there are no old OSLD entries, 1 if there are OSLD entries) to table OTQA
    SELECT :check_id as "CheckID", :obj_type as "ObjType", :doc_entry as "DocEntry", :doc_line_num as "DocLineNum", "SchdLine"+1, -- Make SchdLine start from 1
     	   :tqa_type as "TQAType", :item as "ItemCode", :whs as "WhsCode", "Date" as "CfmDate", "CfmQty", "ReqQty"
	FROM :TQA INTO OTQA;
-- Align with the OTQA table. RESULT stores the confirmaion data, but OTQA may only have required data.
	Select Count(*) into resSize From :RESULT;
	IF :resSize = 0 Then
    RESULT = SELECT "Date", "Qty" From :RESULT
    		 UNION ALL
    		 SELECT Distinct TO_DATE("CfmDate"), "CfmQty" FROM OTQA Where "CfmQty" = 0 And "ItemCode" =:item And "WhsCode" = :whs
    		 	And "CheckID" = :check_id;
   	End If;
 -- calculate additional TQA of type 0 when tqa_type is 1 (i.e., there are old OSLD or REQUIREMENT entries)
    IF :tqa_type = 1
    THEN
	    NEWCONF_REQ = SELECT "Date", "ReqQty" as "Qty" FROM :TQA;
	    NEWCONF_CFM = SELECT "Date", "CfmQty" as "Qty" FROM :TQA;
	    CALL ATP_B1_ADD_TQA(:NEWCONF_REQ, :OLDCONF_REQ, ADD_TQA_REQ);
	    CALL ATP_B1_ADD_TQA(:NEWCONF_CFM, :OLDCONF_CFM, ADD_TQA_CFM);
	    TQA_DELTA_TMP = SELECT
	        CASE WHEN cfm."Date" is null THEN req."Date" ELSE cfm."Date" END AS "Date",
	        CASE WHEN cfm."Qty" is null THEN 0 ELSE cfm."Qty" END AS "CfmQty",
	        CASE WHEN req."Qty" is null THEN 0 ELSE req."Qty" END AS "ReqQty"
	        FROM :ADD_TQA_REQ req FULL OUTER JOIN :ADD_TQA_CFM cfm ON req."Date" = cfm."Date";
        TQA_DELTA = CE_PROJECTION(:TQA_DELTA_TMP, [CE_CALC('rownum()', integer) as "SchdLine", "Date", "ReqQty", "CfmQty"]);
	-- write additional TQA of type 0 to table OTQA
		SELECT :check_id as "CheckID", :obj_type as "ObjType", :doc_entry as "DocEntry", :doc_line_num as "DocLineNum", "SchdLine"+1, -- make SchdLine start from 1
	    0 as "TQAType", :item as "ItemCode", :whs as "WhsCode", "Date" as "CfmDate", "CfmQty", "ReqQty"
	    FROM :TQA_DELTA INTO OTQA;
-- TODO: we need a TQA entry even when there is nothing to confirm to be able to delete the old OSLD entries (only necessary when tqa_type=1)
    END IF;
END;











