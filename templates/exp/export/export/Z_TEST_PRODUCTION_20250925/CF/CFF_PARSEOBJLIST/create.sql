-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS

CREATE PROCEDURE CFF_PARSEOBJLIST (IN	objList		NVARCHAR(200), 
								   IN	groups		NVARCHAR(50))
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	objListStr 		NVARCHAR(200);
	tempStr			NVARCHAR(200);
	pos				INTEGER; 
BEGIN
	objListStr := :objList || ',';
	pos := LOCATE(:objListStr, ',');
	WHILE :pos > 0 DO
		tempStr := TRIM( SUBSTRING(:objListStr, 1, :pos-1) );
		objListStr := SUBSTRING(:objListStr, :pos+1);
		INSERT INTO "CFF_TMP_OBJLIST" VALUES(:tempStr);
		pos := LOCATE(:objListStr, ',');
	END WHILE;
	
	objListStr := :groups || ',';
	pos := LOCATE(:objListStr, ',');
	WHILE :pos > 0 DO
		tempStr := TRIM(SUBSTRING(:objListStr, 1, :pos-1));
		objListStr := SUBSTRING(:objListStr, :pos+1);
		INSERT INTO "CFF_TMP_GROUP" VALUES(:tempStr);
		pos := LOCATE(:objListStr, ',');
	END WHILE;
END;











