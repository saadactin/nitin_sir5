-- B1 DEPENDS: AFTER:SP:ATP_A2_NORM_TQA AFTER:SP:ATP_A6_NORM_OILM AFTER:SP:ATP_A7_NORM_OSLD AFTER:SP:ATP_A5_NORM_OITW AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END
CREATE PROCEDURE ATP_A8_AGG_TIME_SERIES (
    IN item NVARCHAR(50),
    IN whs NVARCHAR(8),
    IN obj_type NVARCHAR(20),
    IN doc_entry INTEGER,
    IN doc_line_num INTEGER,
    IN check_type INTEGER,
    IN includePastReceipt TINYINT,
    OUT INVOLOVED_DOC_LINES DOC_LINES_VERSIONS,
    OUT RESULT ATP_DATE_QTY)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER

READS SQL DATA
AS
BEGIN
    CALL ATP_A5_NORM_OITW (:item, :whs, norm_oitw);
    CALL ATP_A6_NORM_OILM (:item, :whs, :includePastReceipt, ilm_doc_lines, norm_oilm);
    CALL ATP_A7_NORM_OSLD (:item, :whs, :obj_type, :doc_entry, :doc_line_num, :check_type, sld_doc_lines, norm_osld);
    CALL ATP_A2_NORM_TQA(:item, :whs, :check_type, tqa_doc_lines, norm_tqa);
	UNION_ALL = SELECT * FROM :NORM_OITW
				UNION ALL
				SELECT * FROM :NORM_OILM
				UNION ALL
				SELECT * FROM :NORM_OSLD
				UNION ALL
				SELECT * FROM :NORM_TQA;
    RESULT = SELECT "Date", cast( SUM("Qty") as DECIMAL(21,6)) as "Qty"
	     	 FROM :UNION_ALL GROUP BY "Date";
	INVOLOVED_DOC_LINES =
		Select Distinct * From :ilm_doc_lines
		UNION ALL
		Select Distinct * From :sld_doc_lines
		UNION ALL
		Select Distinct * From :tqa_doc_lines
		UNION ALL
		Select Distinct "ObjType", "DocEntry", "DocLineNum", 0 From ODON
		UNION ALL
		Select Distinct "ObjType", "DocEntry", "DocLineNum", 0 From OCAN
		UNION ALL
		Select :obj_type, :doc_entry, :doc_line_num, 0 From DUMMY;
END;











