-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS

CREATE PROCEDURE CFF_FILLDATETABLE (IN dateFrom		DATE, 
									IN dateTo		DATE, 
									IN intervalVal	NVARCHAR(1)) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
	dateIter	DATE;
	dateEnd		DATE;
	iter		INTEGER;
	months		INTEGER;
BEGIN
	dateIter := :dateFrom;
	iter := 1;
	months := 1;
	WHILE (:dateIter <= :dateTo) DO
		IF (:intervalVal = 'D') THEN
			INSERT INTO "CFF_TMP_DATE" VALUES (:dateIter, :dateIter, :iter-1);
			dateIter := ADD_DAYS(:dateIter, 1);
		ELSEIF (:intervalVal = 'W') THEN
			dateEnd := ADD_DAYS(ADD_DAYS(:dateIter, 7), -1);
			IF(:dateEnd > :dateTo) THEN
				dateEnd := :dateTo;
			END IF;
			INSERT INTO "CFF_TMP_DATE" VALUES (:dateIter, :dateEnd, :iter-1);
			dateIter := ADD_DAYS(:dateIter, 7);
		ELSE
			dateEnd := ADD_DAYS(ADD_MONTHS(:dateFrom, :months), -1);
			IF(:dateEnd > :dateTo) THEN
				dateEnd := :dateTo;
			END IF;
			INSERT INTO "CFF_TMP_DATE" VALUES (:dateIter, :dateEnd, :iter-1);
			dateIter := ADD_DAYS(:dateEnd, 1);
			months := :months + 1;
		END IF;
				
		iter := :iter + 1;
	END WHILE;
END;











