-- B1 DEPENDS: AFTER:PT:PROCESS_END

CREATE PROCEDURE CFF_GETNEXTPLANDATE (IN	startDate		DATE, 
									  IN 	fromDate		DATE,
									  IN	onDay			INT,
									  IN	frequncy		NVARCHAR(1),
									  OUT	nextPlanDate	DATE)
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
									  
	dayDiff		INT;

	weekOfDay	INT;
	offset		INT;

	yearOfDay	INT;
	monthOfDay	INT;
	maxMonthDay	INT;
	
	monthPeriod	INT;
	
	startYear	INT;
	startMonth	INT;
	startDay	INT;
	
	fromYear	INT;
	fromMonth	INT;
	fromDay		INT;
	
	realYear	INT;
	realMonth	INT;
	realDay		INT;
	
	monthOffset	INT;
BEGIN 
	IF :frequncy = :VAL_DAILY THEN
	
		IF :fromDate <= :startDate THEN
			nextPlanDate := :startDate;
			RETURN;
		END IF;
	
		dayDiff := DAYS_BETWEEN (:startDate, :fromDate);
		IF MOD(:dayDiff, :onDay) > 0 THEN
			nextPlanDate := ADD_DAYS(:fromDate, :onDay - MOD(:dayDiff, :onDay) );
		ELSE
			nextPlanDate := fromDate;
		END IF;
		
	ELSEIF :frequncy = :VAL_WEEKLY THEN
	
		IF :fromDate < :startDate THEN
			nextPlanDate := :startDate;
		ELSE
			nextPlanDate := :fromDate;
		END IF;
		
		weekOfDay := MOD( WEEKDAY(:nextPlanDate) + 2, 7 );
		IF :onDay - :weekOfDay >= 0 THEN
			offset := :onDay - :weekOfDay; 
		ELSE
			offset := 7 + :onDay - :weekOfDay;
		END IF;
		
		nextPlanDate := ADD_DAYS(:nextPlanDate, :offset);
		
	ELSEIF :frequncy = :VAL_MONTHLY THEN
		
		IF :fromDate <= :startDate THEN
			nextPlanDate := :startDate;
		ELSE
			nextPlanDate := :fromDate;
		END IF;
		
		yearOfDay := YEAR(:nextPlanDate);
		monthOfDay := MONTH(:nextPlanDate);
		maxMonthDay := DAYOFMONTH( LAST_DAY(:nextPlanDate) );
		
		IF :onDay > :maxMonthDay THEN
			nextPlanDate := TO_DATE(:yearOfDay || '-' || :monthOfDay || '-' || :maxMonthDay, 'YYYY-MM-DD');
		ELSE
			nextPlanDate := TO_DATE(:yearOfDay || '-' || :monthOfDay || '-' || :onDay, 'YYYY-MM-DD');
		END IF;
		
		IF :nextPlanDate < fromDate THEN
			nextPlanDate := ADD_MONTHS(:nextPlanDate, 1);
		END IF;
	
	ELSEIF :frequncy = :VAL_ONCE THEN
	
		IF :fromDate > :startDate THEN
			nextPlanDate := CURRENT_DATE; 
		END IF;
		
		nextPlanDate := :startDate;
	
	ELSE
		
		IF :fromDate <= :startDate THEN
			nextPlanDate := :startDate;
			RETURN;
		END IF;
	
		IF :frequncy = :VAL_QUARTERLY THEN
			monthPeriod := 3;
		ELSEIF :frequncy = :VAL_SEMIANNUALLY THEN
			monthPeriod := 6;
		ELSEIF :frequncy = :VAL_ANNUALLY THEN
			monthPeriod := 12;
		END IF;
		
		startYear := YEAR(:startDate);
		startMonth := MONTH(:startDate);
		startDay := DAYOFMONTH(:startDate);
		
		fromYear := YEAR(:fromDate);
		fromMonth := MONTH(:fromDate);
		fromDay := DAYOFMONTH(:fromDate);
		
		IF :fromDay > :startDay THEN
			fromMonth := :fromMonth + 1;
		END IF;
		
		monthOffset := MOD(:startMonth, :monthPeriod) - MOD(:fromMonth, :monthPeriod);
		
		-- can't go back in time
		IF :monthOffset < 0 THEN
			monthOffset := :monthOffset + :monthPeriod;
		END IF;
		
		realMonth := :fromMonth + :monthOffset;
		realYear := :fromYear;
		IF :realMonth > 12 THEN 
			realMonth := :realMonth - 12;
			realYear := :realYear + 1;
		END IF;
		
		maxMonthDay := DAYOFMONTH( LAST_DAY( TO_DATE(:realYear || '-' || :realMonth || '-' || '01', 'YYYY-MM-DD') ) );
		
		IF :startDay > :maxMonthDay THEN
			realDay := :maxMonthDay;
		ELSE
			realDay := :startDay;
		END IF;
		
		nextPlanDate := TO_DATE(:realYear || '-' || :realMonth || '-' || :realDay, 'YYYY-MM-DD');
		
	END IF;
END;











