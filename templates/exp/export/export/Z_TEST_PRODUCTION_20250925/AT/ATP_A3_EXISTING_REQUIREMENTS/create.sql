-- B1 DEPENDS: AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_A3_EXISTING_REQUIREMENTS (
    IN obj_type NVARCHAR (20),
    IN doc_entry INTEGER,
    IN doc_line_num INTEGER,
    IN item_code NVARCHAR(50),
    IN whs_code NVARCHAR(8),
    OUT result_req ATP_DATE_QTY,
    OUT result_cfm ATP_DATE_QTY)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
READS SQL DATA
AS
BEGIN
	OILM_REQ = SELECT TO_DATE("DocDueDate") as "Date",
	                CASE WHEN ("AccumType" = 2 AND ("ActionType" = 1 or "ActionType" = 13) AND "IsNegLnQty" = 'N' OR
	                	("AccumType" = 3 AND ("ActionType" = 2 or "ActionType" = 14) AND "IsNegLnQty" = 'Y' ))
	                         THEN "EffectQty"
	                         ELSE -"EffectQty"
	                END as "Qty"
	FROM OILM
	WHERE "AccumType" in (2,3) AND "ActionType" in (1,2,13,14) AND
	(("AccumType" = 2 AND ("ActionType" = 1 or "ActionType" = 13) AND "IsNegLnQty" = 'N')
	OR   ("AccumType" = 3 AND ("ActionType" = 2 or "ActionType" = 14) AND "IsNegLnQty" = 'Y' )
	OR   ("AccumType" = 2 AND ("ActionType" = 2 or "ActionType" = 14) AND "IsNegLnQty" = 'N' )
	OR   ("AccumType" = 3 AND ("ActionType" = 1 or "ActionType" = 13) AND "IsNegLnQty" = 'Y' ))
	AND "ItemCode" = :item_code AND "LocCode" = :whs_code AND
		(("TransType" = :obj_type AND "DocEntry" = :doc_entry AND "DocLineNum" = :doc_line_num AND "TransType" NOT IN (18,22,163) and  "ActionType" = 1) OR
		 ("TransType" = :obj_type AND "DocEntry" = :doc_entry AND "DocLineNum" = :doc_line_num and "TransType" in (18,22,163) and  "ActionType" = 2))
		OR ("BaseType"= :obj_type  and "BaseAbsEnt"= :doc_entry and "BaseLine"= :doc_line_num AND "LocCode" = :whs_code );
	-- Aggregate the changes of the document line
	RESULT_REQ = Select "Date", cast(Sum("Qty")  as DECIMAL(21,6)) as "Qty" From :OILM_REQ Group By "Date";
	RESULT_CFM = SELECT TO_DATE("CfmDate") as "Date", "CfmQty" as "Qty"
	         FROM OSLD
	         WHERE "ObjType" = :obj_type AND "DocEntry" = :doc_entry AND "DocLineNum" = :doc_line_num AND
			        "ItemCode" = :item_code AND "WhsCode" = :whs_code;
END;











