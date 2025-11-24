-- B1 DEPENDS: AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_A4_CLEANUP_TQA(IN keep_seconds INTEGER)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER

AS
cleanup_until varchar(30);
check_id INTEGER;
minCmTime TimeStamp;
-- delete TQA that were not removed properly
BEGIN
	Select Min(Commit_Time) Into minCmTime From "PUBLIC".Transaction_History;
	If :minCmTime Is Not NULL Then
		SELECT TO_CHAR(add_seconds(CURRENT_UTCTIMESTAMP, - :keep_seconds)) INTO cleanup_until FROM DUMMY;
		If To_TimeStamp(:cleanup_until) >= :minCmTime Then
			EXEC('SET HISTORY SESSION TO UTCTIMESTAMP ' || '''' || :cleanup_until || '''');
			SELECT MAX("CheckID") INTO check_id FROM OTQA;
		    EXEC('SET HISTORY SESSION TO NOW');
		    DELETE FROM OTQA WHERE "CheckID" <= :check_id;
		End If;
	End If;
END;











