-- B1 DEPENDS: BEFORE:PT:PROCESS_START

CREATE PROCEDURE MOBILE_KPI_IncomingPayments
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
BEGIN

declare FirstDayOfLastMonth DATE;
declare FirstDayOfThisMonth DATE;
declare FirstDayOfNextMonth DATE;
declare LastMonth FLOAT; 
declare ThisMonth FLOAT; 
declare Gauge FLOAT; 
declare IndicatorSign VARCHAR(1);

SELECT ADD_DAYS(ADD_MONTHS(CURRENT_DATE, -1), -(DAYOFMONTH(CURRENT_DATE)-1)) INTO FirstDayOfLastMonth FROM DUMMY;
SELECT ADD_DAYS(CURRENT_DATE, -(DAYOFMONTH(CURRENT_DATE)-1)) INTO FirstDayOfThisMonth FROM DUMMY;
SELECT ADD_DAYS(ADD_MONTHS(CURRENT_DATE, 1), -(DAYOFMONTH(CURRENT_DATE)-1)) INTO FirstDayOfNextMonth FROM DUMMY;   

SELECT IFNULL(SUM("DocTotal"), 0) INTO LastMonth FROM ORCT WHERE "DocDate">=FirstDayOfLastMonth and "DocDate"<FirstDayOfThisMonth and "Canceled"='N';
SELECT IFNULL(SUM("DocTotal"), 0) INTO ThisMonth FROM ORCT WHERE "DocDate">=FirstDayOfThisMonth and "DocDate"<FirstDayOfNextMonth and "Canceled"='N';
	  
IF LastMonth > 0 THEN
	SELECT (ThisMonth-LastMonth)/LastMonth * 100 INTO Gauge FROM DUMMY; 
ELSEIF ThisMonth > 0 THEN
	SELECT 100 INTO Gauge FROM DUMMY; 
ELSE   
	SELECT 0 INTO Gauge FROM DUMMY;
END IF;  

SELECT 'P' INTO IndicatorSign FROM DUMMY; 
SELECT 'Incoming Payments' AS Title, CONCAT(TO_VARCHAR(TO_DECIMAL(Gauge, 20, 2)),'%') AS Gauge, IndicatorSign AS Indicator, 'This Month' AS SubTitle1, TO_VARCHAR(TO_DECIMAL(ThisMonth, 20, 2)) AS SubValue1, 'Last Month' AS SubTitle2, TO_VARCHAR(TO_DECIMAL(LastMonth, 20, 2)) AS SubValue2 FROM DUMMY;

END











