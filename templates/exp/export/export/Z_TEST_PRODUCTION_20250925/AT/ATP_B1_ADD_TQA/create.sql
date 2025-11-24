-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END
CREATE PROCEDURE ATP_B1_ADD_TQA(
    IN NEW_CONF ATP_DATE_QTY, -- time series of new confirmations
    IN OLD_CONF ATP_DATE_QTY, -- time series of old confirmations
    OUT RESULT ATP_COUNTER_DATE_QTY) -- time series of additional temporary quantity assignments (TQA)
LANGUAGE SQLSCRIPT
SQL SECURITY INVOKER
READS SQL DATA
AS
-- calculate additional TQA of type 0 that are necessary to make OLD_CONF as strong as NEW_CONF
BEGIN
-- combine the two input time series into one time series
MATCH = SELECT
        CASE WHEN o."Date" is null THEN n."Date" ELSE o."Date" END AS "Date",
        CASE WHEN o."Qty" is null THEN 0 ELSE o."Qty" END AS "OldQty",
        CASE WHEN n."Qty" is null THEN 0 ELSE n."Qty" END AS "NewQty"
        FROM :NEW_CONF n FULL OUTER JOIN :OLD_CONF o ON n."Date" = o."Date";
-- cumulate old and new confirmations,
-- put difference of cumulated rows to column Diff if greater than 0,
-- get a row number into column Counter to be able to join with the previous row
NUM_AGG = SELECT count(*) as "Counter", m1."Date", GREATEST(sum(m2."NewQty") - sum(m2."OldQty"),0) as "Diff"
          FROM :MATCH m1, :MATCH m2 WHERE m2."Date" <= m1."Date" GROUP BY m1."Date" ORDER BY m1."Date";
-- calculate final result [Qty(n) = Diff(n) - Diff(n-1)]
RESULT = SELECT cast("Counter" as int) as "Counter", "Date", cast("Qty" as DECIMAL(21,6)) as "Qty" FROM
         (SELECT a1."Counter", a1."Date", CASE WHEN a2."Counter" is null THEN a1."Diff" ELSE a1."Diff" - a2."Diff" END as "Qty"
          FROM :NUM_AGG a1 LEFT OUTER JOIN  :NUM_AGG a2 ON a2."Counter" = a1."Counter" - 1)
         WHERE "Qty" != 0;
END;











