-- B1 DEPENDS: AFTER:SP:ATP_B2_GATEWAY AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_B6_CHECK_OPTI_S2(
       IN obj_type NVARCHAR (20),
       IN item NVARCHAR(50),
       IN whs NVARCHAR(8),
       IN check_id_for_recheck INTEGER,
       IN doc_entry INTEGER,
       IN doc_line_num INTEGER,
       IN LINEITEMS ATP_DATE_QTY,
       IN check_id INTEGER,
       IN check_type INTEGER,
       IN includePastReceipt TINYINT,
       OUT RESULT ATP_DATE_QTY
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
    commit_id INT := 0;
BEGIN
    
    
    
    
    SELECT MAX(COMMIT_ID) INTO commit_id FROM TRANSACTION_HISTORY WHERE COMMIT_ID < (SELECT LAST_COMMIT_ID FROM M_TRANSACTIONS WHERE CONNECTION_ID = CURRENT_CONNECTION AND PORT like '%03');
    
    IF :commit_id is NOT NULL THEN
        
        DELETE FROM OTQA WHERE "CheckID" = :check_id;
        
        EXEC('SET HISTORY SESSION TO COMMIT ID ' || :commit_id);
        
        SL = SELECT TO_INT(TO_DATS("Date")) as date, "Qty" as qty FROM :LINEITEMS;
        CALL ATP_B2_GATEWAY(:obj_type, :check_id, :item, :whs, 1, :check_id_for_recheck, :doc_entry, :doc_line_num, :SL, :check_type, :includePastReceipt, RESULT);
    END IF;
END;











