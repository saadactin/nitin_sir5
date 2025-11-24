-- B1 DEPENDS: AFTER:PT:PROCESS_END

CREATE PROCEDURE CFF_GETTOTALINSTANCES(IN		startDate		DATE, 
									   IN		endDate			DATE,
									   IN		onDay			INT,
									   IN		frequncy		NVARCHAR(1),
									   OUT		nextInstance	INT) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	VAL_DAILY			NVARCHAR(1) := 'D';
	VAL_WEEKLY			NVARCHAR(1) := 'W';
	VAL_MONTHLY			NVARCHAR(1) := 'M';
	VAL_QUARTERLY		NVARCHAR(1) := 'Q';
	VAL_SEMIANNUALLY	NVARCHAR(1) := 'S';
	VAL_ANNUALLY		NVARCHAR(1) := 'A';
	VAL_ONCE			NVARCHAR(1) := 'O';
 
	offset			INT;
	
	realStartDate	DATE;
	circleCount		INT;
	
	startYear		INT;
	startMonth		INT;
	startDay		INT;
	
	endYear			INT;
	endMonth		INT;
	endDay			INT;
	fullMonth		INT;
	
	maxMonthDay		INT;
	
	monthOffset		INT;
	period			INT;
	tempDate		DATE;
	
	yearOffset		INT;
BEGIN 
	IF :frequncy = :VAL_DAILY THEN
		offset := DAYS_BETWEEN (:startDate, :endDate);
		nextInstance := :offset / :onDay + 1;
	ELSEIF :frequncy = :VAL_WEEKLY THEN
		CALL CFF_GETNEXTPLANDATE(:startDate, :startDate, :onDay, :frequncy, :realStartDate);
		IF :realStartDate > :endDate THEN
			nextInstance := 0;
			RETURN;
		END IF;
		
		circleCount := DAYS_BETWEEN (:realStartDate, :endDate) / 7;
		nextInstance := :circleCount + 1;
	ELSEIF :frequncy = :VAL_MONTHLY THEN
		CALL CFF_GETNEXTPLANDATE(:startDate, :startDate, :onDay, :frequncy, :realStartDate);
		IF :realStartDate > :endDate THEN
			nextInstance := 0;
			RETURN;		
		END IF;
		startYear := YEAR(:realStartDate);
		startMonth := MONTH(:realStartDate);
		startDay := DAYOFMONTH(:realStartDate);
		endYear := YEAR(:endDate);
		endMonth := MONTH(:endDate);
		endDay := DAYOFMONTH(:endDate);
		
		fullMonth := 12 * (:endYear - :startYear) + :endMonth - :startMonth;
		maxMonthDay := DAYOFMONTH( LAST_DAY(:endDate) );
		
		IF :onDay > :maxMonthDay THEN
			IF :maxMonthDay <= :endDay THEN
				nextInstance := :fullMonth + 1;
			ELSE
				nextInstance := :fullMonth;
			END IF;
		ELSE
			IF :onDay <= :endDay THEN
				nextInstance := :fullMonth + 1;
			ELSE
				nextInstance := :fullMonth;
			END IF;		
		END IF;
	ELSEIF :frequncy = :VAL_QUARTERLY THEN
		startYear := YEAR(:startDate);
		startMonth := MONTH(:startDate);
		startDay := DAYOFMONTH(:startDate);
		
		endYear := YEAR(:endDate);
		endMonth := MONTH(:endDate);
		endDay := DAYOFMONTH(:endDate);
		
		monthOffset := 12 * (:endYear - :startYear) + :endMonth - :startMonth;
		period := :monthOffset / 3;
		
		tempDate := ADD_MONTHS(:startDate, 3 * period);
		IF :tempDate <= :endDate THEN
			period := :period + 1;
		END IF;
		
		nextInstance := :period;
	ELSEIF :frequncy = :VAL_SEMIANNUALLY THEN
		startYear := YEAR(:startDate);
		startMonth := MONTH(:startDate);
		startDay := DAYOFMONTH(:startDate);
		
		endYear := YEAR(:endDate);
		endMonth := MONTH(:endDate);
		endDay := DAYOFMONTH(:endDate);
		
		monthOffset := 12 * (:endYear - :startYear) + :endMonth - :startMonth;
		period := :monthOffset / 6;
		
		tempDate := ADD_MONTHS(:startDate, 6 * period);
		IF :tempDate <= :endDate THEN
			period := :period + 1;
		END IF;
		
		nextInstance := :period;
	ELSEIF :frequncy = :VAL_ANNUALLY THEN
		startYear := YEAR(:startDate);
		endYear := YEAR(:endDate);
		
		yearOffset := :endYear - :startYear;
		
		tempDate := ADD_YEARS(:startDate, :yearOffset);
		IF :tempDate <= :endDate THEN
			yearOffset := :yearOffset + 1;
		END IF;
		
		nextInstance := :yearOffset;
	ELSEIF :frequncy = :VAL_ONCE THEN
		nextInstance := 1;
	END IF;
END;











