-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS
CREATE PROCEDURE CFF_PARSEOPTIONS(IN	options		NVARCHAR(50), 
								  OUT	addRP		NVARCHAR(1), 
								  OUT	addRT		NVARCHAR(1), 
								  OUT	addBA		NVARCHAR(1),
								  OUT	addJV		NVARCHAR(1),
								  OUT	addDD		NVARCHAR(1),
								  OUT	addDP		NVARCHAR(1))
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	optionStr	NVARCHAR(50);
	tempStr		NVARCHAR(50);
	pos			INTEGER;
BEGIN
	addRP := 'N';
	addRT := 'N';
	addBA := 'N';
	addJV := 'N';
	addDD := 'N';
	addDP := 'N';
	
	optionStr := :options || ',';
	pos := LOCATE(:optionStr, ',');	
	WHILE :pos > 0 DO
		tempStr := TRIM( SUBSTRING(:optionStr, 1, :pos-1) );
		optionStr := TRIM( SUBSTRING(:optionStr, :pos+1) );
				
		IF(LOCATE(:tempStr, 'RP') > 0) THEN
			addRP := 'Y';
			--INSERT INTO CFF_TEMP VALUES('RP', :addRP);
		ELSEIF (LOCATE(:tempStr, 'RT') > 0) THEN
			addRT := 'Y';
			INSERT INTO "CFF_TMP_OBJLIST"
			SELECT "ObjType" || 'r' FROM "CFF_TMP_OBJLIST" WHERE "ObjType" NOT LIKE '%d' AND "ObjType" NOT LIKE '%r' AND "ObjType" NOT LIKE '-%';
			--INSERT INTO CFF_TEMP VALUES('RT', :addRT);
		ELSEIF (LOCATE(:tempStr, 'BA') > 0) THEN
			addBA := 'Y';
			--INSERT INTO CFF_TEMP VALUES('BA', :addBA);
		ELSEIF (LOCATE(:tempStr, 'JV') > 0) THEN
			addJV := 'Y';
			--INSERT INTO CFF_TEMP VALUES('JV', :addJV);
		ELSEIF (LOCATE(:tempStr, 'DD') > 0) THEN
			addDD := 'Y';
			INSERT INTO "CFF_TMP_OBJLIST"
			SELECT "ObjType" || 'd' FROM "CFF_TMP_OBJLIST" WHERE "ObjType" NOT LIKE '%d' AND "ObjType" NOT LIKE '%r' AND "ObjType" NOT LIKE '-%';
			--INSERT INTO CFF_TEMP VALUES('DD', :addDD);
		ELSEIF (LOCATE(:tempStr, 'DP') > 0) THEN
			addDP := 'Y';
			--INSERT INTO CFF_TEMP VALUES('DP', :addDP);
		END IF;
		
		pos := LOCATE(:optionStr, ',');
		
	END WHILE;
END;











