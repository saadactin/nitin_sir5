-- B1 DEPENDS: AFTER:SP:ATP_B2_GATEWAY AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_B4_CHECK_LOCKED(
       IN obj_type NVARCHAR (20),
       IN item NVARCHAR(50),
       IN whs NVARCHAR(8),
       IN check_id_for_recheck INTEGER,
       IN doc_entry INTEGER,
       IN doc_line_num INTEGER,
       IN LINEITEMS ATP_DATE_QTY,
       IN check_type INTEGER,
       IN includePastReceipt TINYINT,
       OUT check_id INTEGER,
       OUT RESULT ATP_DATE_QTY
) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
BEGIN
    
    SELECT * FROM ATP_LOCK WHERE "ItemCode" = :item AND "WhsCode" = :whs FOR UPDATE;

SELECT ATP_CHECK_ID.NEXTVAL INTO check_id FROM DUMMY;

SELECT :check_id as "CheckID", :item as "ItemCode", :whs as "WarehouseCode", "Date", "Qty" FROM :LINEITEMS INTO ATP_CHECKS;
SL = SELECT TO_INT(TO_DATS("Date")) as date, "Qty" as qty FROM :LINEITEMS;

CALL ATP_B2_GATEWAY(:obj_type, :check_id, :item, :whs, 1, :check_id_for_recheck, :doc_entry, :doc_line_num, :SL, :check_type, :includePastReceipt, result);
END;











