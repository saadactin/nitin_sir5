-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

CREATE PROCEDURE ATP_C5_READ_DONATORS(
	OUT num_recheck smallint, -- number of lines of table DONATORS (needed for FOR loop)
	OUT DONATORS ATP_T_DONATORS, -- existing confirmations of the donating orders enriched by donated quantity
	OUT NUMB_RECHECK ATP_T_NUMB_RECHECK, -- order lines of donating orders that are to be checked again
	OUT STABLE_CONF ATP_T_DONATORS -- confirmations of donating orders that remain unchanged
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
-- read data of donating orders
-- calculate confirmations of donating orders that remain unchanged
BEGIN
	DONATORS = SELECT d."ObjType", d."DocEntry", d."DocLineNum", o."SchdLine", TO_DATE(d."CfmDate") as "CfmDate", cast(o."CfmQty" as DECIMAL(21,6)) as "CfmQty",
					  Case When d."DonQty" <= o."CfmQty" Then d."DonQty" Else cast(o."CfmQty" as DECIMAL(21,6)) End as "DonQty"
	           FROM OSLD o, ODON d
	           WHERE o."ObjType" = d."ObjType" And
	           		o."DocEntry" = d."DocEntry" And
	           		o."DocLineNum" = d."DocLineNum" And
	           		o."ItemCode" = d."ItemCode" And
	           		o."WhsCode" = d."WhsCode" And
	           		o."CfmDate" = d."CfmDate"
	            order by "SequenceID" asc;
	-- get the documnet lines which need recheck
	RECHECK = SELECT "ObjType", "DocEntry", "DocLineNum"
	          FROM :DONATORS GROUP BY "ObjType", "DocEntry", "DocLineNum";
	-- get a row number for the documnet lines need recheck
	NUMB_RECHECK = CE_PROJECTION(:RECHECK,[CE_CALC('rownum()', smallint) as "Counter", "ObjType", "DocEntry","DocLineNum"]);
	SELECT COUNT(*) INTO num_recheck FROM :NUMB_RECHECK;
	-- get the OSLD records those need recheck
	DONATOR_CONF = SELECT o."ObjType", o."DocEntry", o."DocLineNum", o."ItemCode", o."WhsCode", TO_DATE(o."CfmDate") as "CfmDate"
					FROM OSLD o, :RECHECK r
					WHERE o."ObjType" = r."ObjType" And o."DocEntry" = r."DocEntry" AND o."DocLineNum" = r."DocLineNum";
	UNTOUCHED_CONF = SELECT "ObjType", "DocEntry", "DocLineNum", "ItemCode", "WhsCode", TO_DATE("CfmDate") as "CfmDate" FROM :DONATOR_CONF
					 Except
					 SELECT "ObjType", "DocEntry", "DocLineNum", "ItemCode", "WhsCode", TO_DATE("CfmDate") as "CfmDate" FROM ODON;
	STABLE_CONF = (SELECT "ObjType", "DocEntry", "DocLineNum", "SchdLine", TO_DATE("CfmDate") as "CfmDate", cast("CfmQty" - "DonQty" as DECIMAL(21,6)) as "CfmQty", 0 as "DonQty"
	               FROM :DONATORS WHERE "DonQty" < "CfmQty")
	              UNION ALL
	              (SELECT o."ObjType", o."DocEntry", o."DocLineNum", o."SchdLine", TO_DATE(o."CfmDate") as "CfmDate", o."CfmQty", 0 as "DonQty"
	               FROM OSLD o, :UNTOUCHED_CONF u
	               WHERE o."ObjType" = u."ObjType" And o."DocEntry"=u."DocEntry" And o."DocLineNum"=u."DocLineNum" And
	               		 o."ItemCode" = u."ItemCode" And o."WhsCode" = u."WhsCode" And o."CfmDate"=u."CfmDate");
END;











