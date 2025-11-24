-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END
CREATE PROCEDURE ATP_A5_NORM_OITW (
    IN item NVARCHAR(50),
    IN whs NVARCHAR(8),
    OUT result ATP_DATE_QTY)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER

READS SQL DATA
AS
-- read stock from the database
BEGIN
	result = select TO_DATE('19000101') as "Date", "OnHand" as "Qty"
	from OITW
	where "ItemCode" = :item and "WhsCode" = :whs;
END;











