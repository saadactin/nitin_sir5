-- B1 DEPENDS: AFTER:PT:PROCESS_END
CREATE PROCEDURE MOBILE_ServiceTicket_SalesOrders
(
IN serviceCallId INTEGER, 
IN line INTEGER
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
  _salesOrders nvarchar(100) ARRAY;
  _salesOrderText nvarchar(200);
 _index integer;
BEGIN

	 DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 1299
	 BEGIN
		select * from 
		(
			select * from RDR1 t0 
			inner join ORDR t1 on t0."DocEntry" = t1."DocEntry" 
			left join OITM t2 on t0."ItemCode" = t2."ItemCode" where 1<>1
		),(select '' as C from dummy);
	 END;
	 _salesOrderText:='';
	 SELECT  IFNULL("SaleOrders",'') into _salesOrderText from SCL6 where "SrcvCallID" = :serviceCallId and "Line" = :line;
	 IF LENGTH(:_salesOrderText) > 0 then
  		_index := 1;
  		WHILE LOCATE(:_salesOrderText,',') > 0 DO
	  		_salesOrders[:_index] := SUBSTR_BEFORE(:_salesOrderText,',');
 	 		_salesOrderText := SUBSTR_AFTER(:_salesOrderText,',');
  			_index := :_index + 1;
		END WHILE;
		_salesOrders[:_index] := :_salesOrderText;
  		rst = UNNEST(:_salesOrders) AS ("SalesOrdersEntries");
	     select * from 
	     (
	     	select * from RDR1 t0 
	     	inner join ORDR t1 on t0."DocEntry" = t1."DocEntry" 
	     	left join OITM t2 on t0."ItemCode" = t2."ItemCode" 
	     	where t0."DocEntry" in (SELECT * FROM :rst)
	     ),(select '' as C from dummy);
	  ELSE
	  	---select * from RDR1 t0 inner join ORDR t1 on t0."DocEntry" = t1."DocEntry" where 1<>1;
		select * from RDR1 t0 
		inner join ORDR t1 on t0."DocEntry" = t1."DocEntry" 
		left join OITM t2 on t0."ItemCode" = t2."ItemCode" 
		right outer join (select null as "C" from dummy) as s2 on s2."C"=t0."DocEntry";
	END IF;
END;











