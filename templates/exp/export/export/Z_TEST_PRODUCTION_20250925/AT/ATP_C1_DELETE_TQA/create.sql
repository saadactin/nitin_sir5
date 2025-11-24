-- B1 DEPENDS: AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_C1_DELETE_TQA(IN check_id INTEGER)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
-- to be used when transaction is cancelled (e.g., user does not like the check result)
AS
BEGIN
    DELETE FROM OTQA WHERE "CheckID" = :check_id;
END;











