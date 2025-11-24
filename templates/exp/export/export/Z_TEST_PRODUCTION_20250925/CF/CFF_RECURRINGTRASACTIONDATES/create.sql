-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS AFTER:SP:CFF_GETNEXTPLANDATE AFTER:SP:CFF_GETTOTALINSTANCES

CREATE PROCEDURE CFF_RECURRINGTRASACTIONDATES(IN	fromDate	DATE, 
											  IN	toDate		DATE
											 )
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	rcpSize			INT;
	onDay			INT;
	nextInstance	INT;
	startDate		DATE;
	endDate			DATE;
	nextDAte		DATE;
	planSinceDate	DATE;
	tillDay			DATE;
	DAILY_BASE		INT := 100;
	WEEKLY_BASE		INT := 0;
	MONTHLY_BASE	INT := 1000;
	CURSOR c_cursor FOR
		SELECT * FROM ORCP WHERE "IsRemoved" <> 'Y';
BEGIN
	-- get recurring trasaction template
	FOR cur_row as c_cursor DO
	
		onDay := cur_row."Remind";
		--get orcp_abs_entry, orcp_doc_object_type, orcp_draft_entry
		IF cur_row."Frequency" = 'D' THEN
			-- For daily, onDay should be 1, 2, 3, ..., 10, 15, 30, 45, 60
			onDay := :onDay - :DAILY_BASE;
		ELSEIF cur_row."Frequency" = 'W' THEN
			-- For weekly, onDay should be SUN, MON, ..., SAT
			onDay := :onDay - :WEEKLY_BASE;
		ELSEIF cur_row."Frequency" = 'M' THEN
			-- For monthly, onDay should be 1, 2, 3, ..., 31
			onDay := :onDay - :MONTHLY_BASE;
		ELSE
			onDay := 0;
		END IF;
		
		startDate := cur_row."StartDate";
		IF cur_row."EndDate" IS NULL THEN 
			endDate   := :toDate;
		ELSE
			endDate   := cur_row."EndDate";
		END IF;

		SELECT ADD_DAYS(CURRENT_DATE, 1) INTO planSinceDate FROM DUMMY; -- tomorrow
		CALL CFF_GETNEXTPLANDATE(:startDate, :planSinceDate, :onDay, cur_row."Frequency", :nextDate);
		CALL CFF_GETTOTALINSTANCES(:startDate, :nextDate, :onDay, cur_row."Frequency", :nextInstance);
		
		IF :toDate IS NULL THEN
			tillDay := CURRENT_DATE;
		ELSE
			tillDay := :toDate;
		END IF;
		
		IF cur_row."Frequency" = 'O' THEN
			IF :nextDate <= CURRENT_DATE THEN
				INSERT INTO CFF_TMP_ORCL VALUES('1', cur_row."AbsEntry", :nextInstance, :nextDate, 'N', cur_row."DocObjType", cur_row."DraftEntry");
			END IF;
		ELSE
			WHILE :nextDate <= :tillDay AND :nextDate <= :endDate DO

				INSERT INTO CFF_TMP_ORCL VALUES('1', cur_row."AbsEntry", :nextInstance, :nextDate, 'N', cur_row."DocObjType", cur_row."DraftEntry");
				nextDate := ADD_DAYS(:nextDate, 1);
				nextInstance := :nextInstance + 1;
				CFF_GETNEXTPLANDATE(:startDate, :nextDate, :onDay, cur_row."Frequency", :nextDate);
				
			END WHILE;
		END IF;
		
	END FOR;

	SELECT * FROM CFF_TMP_ORCL;
END;











