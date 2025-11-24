-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS

CREATE PROCEDURE CFF_ADDDATE (IN origDate		DATE, 
							  IN endDate		DATE,
							  IN frequency		NVARCHAR(1), 
							  OUT newDate		DATE) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
AS 
BEGIN
	IF(:frequency = 'D') THEN
		newDate := ADD_DAYS(:origDate, 1);
	ELSEIF (:frequency = 'W') THEN
		newDate := ADD_DAYS(:origDate, 7);
	ELSEIF (:frequency = 'A') THEN
		newDate := ADD_YEARS(:origDate, 1);
	ELSEIF (:frequency = 'M') THEN
		newDate := ADD_MONTHS(:origDate, 1);
	ELSEIF (:frequency = 'S') THEN
		newDate := ADD_MONTHS(:origDate, 6);
	ELSEIF (:frequency = 'Q') THEN
		newDate := ADD_MONTHS(:origDate, 3);
	ELSE
		newDate := ADD_DAYS(:endDate, 1);
	END IF; 
END;











