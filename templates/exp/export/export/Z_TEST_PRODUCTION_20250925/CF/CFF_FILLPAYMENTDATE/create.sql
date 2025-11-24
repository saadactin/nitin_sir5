-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS

CREATE PROCEDURE CFF_FILLPAYMENTDATE 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	cardCode			NVARCHAR(15);
	cardCodeBeforeNext	NVARCHAR(15);
	siPmntDate			SMALLINT;
	siPmntDateDayFirst	SMALLINT;
	siDueDateDay		SMALLINT;
	
	perCount			INT;
	
	CURSOR c_cursor FOR 
		SELECT T0."CardCode", T0."PmntDate" FROM (SELECT "CardCode", CAST("PmntDate" AS SMALLINT) AS "PmntDate" FROM "CRD5") T0 
		ORDER BY T0."CardCode" ASC, T0."PmntDate" ASC; 
BEGIN
	
	OPEN c_cursor;

	IF c_cursor::ISCLOSED THEN
		RETURN;		
	END IF;	
	
	FETCH c_cursor INTO cardCode, siPmntDate;
	
	perCount := 1;
	siPmntDateDayFirst := :siPmntDate;
	siDueDateDay := 1;
	
	WHILE NOT c_cursor::NOTFOUND DO 
		
		WHILE :siDueDateDay <= :siPmntDate DO
			INSERT INTO "CFF_TMP_PAYMENTDATE" VALUES (:cardCode, :siDueDateDay, :siPmntDate, 0);
			siDueDateDay := :siDueDateDay + 1;
		END WHILE;
		
		cardCodeBeforeNext := :cardCode;
				
		FETCH c_cursor INTO cardCode, siPmntDate;
		IF c_cursor::NOTFOUND THEN
			BREAK;
		END IF;
		perCount := :perCount + 1;
		
		IF :cardCodeBeforeNext <> :cardCode THEN
			WHILE :siDueDateDay <= 31 DO
				INSERT INTO "CFF_TMP_PAYMENTDATE" VALUES (:cardCodeBeforeNext, :siDueDateDay, :siPmntDateDayFirst, 1);
				siDueDateDay := :siDueDateDay + 1;
			END WHILE;
			siPmntDateDayFirst := :siPmntDate;
			siDueDateDay := 1;
		END IF; 
	END WHILE;
	
	WHILE siDueDateDay <= 31 AND cardCodeBeforeNext IS NOT NULL DO
		INSERT INTO "CFF_TMP_PAYMENTDATE" VALUES (:cardCodeBeforeNext, :siDueDateDay, :siPmntDateDayFirst, 1);
		siDueDateDay := :siDueDateDay + 1;
	END WHILE;
	
	CLOSE c_cursor;
	 
END;











