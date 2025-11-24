-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END
CREATE PROCEDURE ATP_C2_QTY_WORKAROUND(
       IN date DATE,
       IN qty DECIMAL(21,6),
       OUT LINEITEMS ATP_INT_QTY)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
READS SQL DATA
AS
-- workaround because of bug 13906 (remove the procedure as soon as the bug is fixed)
-- Problem: type of scalar variables is not set properly when they are read into a table variable
-- Workaround: Put the SELECT statement into a procedure
BEGIN
LINEITEMS = SELECT TO_INT(TO_DATS(:date)) as date, :qty as qty FROM DUMMY;
END;











