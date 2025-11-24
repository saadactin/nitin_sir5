-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS AFTER:SP:CFF_ADDDATE
CREATE PROCEDURE CFF_BLANKETAGREEMENT_DETAIL (IN dateFrom		DATE, 
											  IN dateTo			DATE)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
	curAmnt		DECIMAL(21,6);
	occurrence	INTEGER;
	newDate		DATE;
	nextDate	DATE;
	docSum		DECIMAL(21,6);
	recCount	INTEGER;
	--- Variable FOR curser BAQry
	fromDate	TIMESTAMP;
	toDate		TIMESTAMP;
	frequency	NVARCHAR(1);
	quantity	DECIMAL(21,6);
	agrEfctNum 	INTEGER;
	agrLineNum	INTEGER;
	unitPrice	DECIMAL(21,6);
	currency	NVARCHAR(5);
	bpCode		NVARCHAR(15);
	absID		INTEGER;
	cardType	NVARCHAR(1);
	addSF		INTEGER;
	addEF		INTEGER; 
CURSOR BAQry(qyDateFrom DATE, qyDateTo DATE) FOR
	SELECT T2."FromDate", T2."ToDate", T2."DatePeriod", T2."Quantity", T2."AgrEfctNum", T1."AgrLineNum", T1."UnitPrice", T1."Currency", T0."BpCode", T0."AbsID", T3."CardType"
	FROM "OOAT" T0
	INNER JOIN "OAT1" T1 ON T1."AgrNo" = T0."AbsID"
	INNER JOIN "OAT2" T2 ON T1."AgrNo" = T2."AgrNo" AND T1."AgrLineNum" = T2."AgrLnNum"
	INNER JOIN "OCRD" T3 ON T3."CardCode" = T0."BpCode"
	WHERE T0."Status" = 'A' 
		AND T0."Cancelled" = 'N'
		AND T2."FromDate" <= :qyDateTo
		AND T2."ToDate" >= :qyDateFrom
		AND (T3."CardType" = 'C' or T3."CardType" = 'S')
	ORDER BY T0."AbsID", T1."AgrLineNum", T2."AgrEfctNum";
BEGIN 

	SELECT COUNT(*) INTO addSF FROM "CFF_TMP_GROUP" WHERE "Group" = 'SF';
	SELECT COUNT(*) INTO addEF FROM "CFF_TMP_GROUP" WHERE "Group" = 'EF';
	
	OPEN BAQry(:dateFrom, :dateTo);
	FETCH BAQry INTO fromDate, toDate, frequency, quantity, agrEfctNum, agrLineNum, unitPrice, currency, bpCode, absID, cardType;
	WHILE not BAQry::NOTFOUND DO
		-- The date inteval is out of range, do not need to process the BA
		IF :fromDate > :dateTo OR :toDate < :dateFrom THEN
			CONTINUE;
		END IF;
		
		occurrence := 0;
		newDate := :fromDate;
		WHILE :newDate <= :toDate DO
			occurrence := :occurrence + 1;
			CALL CFF_ADDDATE(:newDate, :toDate, :frequency, :newDate);
		END WHILE; -- End date loop
			
		newDate := :fromDate;
		WHILE :newDate <= :toDate DO
			IF :newDate >= :dateFrom AND :newDate <= :dateTo THEN
				docSum := 0;
				curAmnt := :unitPrice * :quantity / :occurrence;
			
				IF (:cardType = 'C') THEN
					IF (:addSF > 0) THEN
						-- Get BP document related to this BA
						SELECT COUNT(*), SUM(T1."LineTotal") INTO recCount, docSum 
						FROM "ORIN" T0
						INNER JOIN "RIN1" T1 ON T1."DocEntry" = T0."DocEntry"
						WHERE T0."DocDate" BETWEEN :newDate AND :nextDate
							AND T1."AgrNo" = :absID
							AND T1."AgrLnNum" = :agrLineNum;
						
						IF(:recCount > 0) THEN
							curAmnt := :curAmnt - :docSum;
						END IF;
						
						SELECT COUNT(*), SUM(T1."LineTotal") INTO recCount, docSum
						FROM "OINV" T0
						INNER JOIN "INV1" T1 ON T1."DocEntry" = T0."DocEntry"
						WHERE T0."DocDate" BETWEEN :newDate AND :nextDate
							AND T1."AgrNo" = :absID
							AND T1."AgrLnNum" = :agrLineNum;
						
						
						IF(:recCount > 0) THEN
							curAmnt := :curAmnt - :docSum;
						END IF;
						
						IF(:curAmnt < 0) THEN
							curAmnt := 0;
						END IF;
						--INSERT INTO TmpDocSum VALUES(:newDate, :nextDate, :bpCode, :curAmnt-:DocTotal, :DocTotal, :curAmnt);								
						-- End
						INSERT INTO "CFF_TMP_DETAIL_BA" VALUES(:newDate, '1250000025', :absID, :absID, '', :bpCode, '', :curAmnt, 0, '17', 'SF');
					END IF;
				ELSE
					IF (:addEF > 0) THEN
						-- Get BP document related to this BA
						SELECT COUNT(*), SUM(T1."LineTotal") INTO recCount, docSum 
						FROM "ORPC" T0
						INNER JOIN "RPC1" T1 ON T1."DocEntry" = T0."DocEntry"
						WHERE T0."DocDate" BETWEEN :newDate AND :nextDate
							AND T1."AgrNo" = :absID
							AND T1."AgrLnNum" = :agrLineNum;
						
						IF(:recCount > 0) THEN
							curAmnt := :curAmnt - :docSum;
						END IF;
						
						SELECT COUNT(*), SUM(T1."LineTotal") INTO recCount, docSum
						FROM "OPCH" T0
						INNER JOIN "PCH1" T1 ON T1."DocEntry" = T0."DocEntry"
						WHERE T0."DocDate" BETWEEN :newDate AND :nextDate
							AND T1."AgrNo" = :absID
							AND T1."AgrLnNum" = :agrLineNum;
						
						IF(:recCount > 0) THEN
							curAmnt := :curAmnt - :docSum;
						END IF;
						IF(:curAmnt < 0) THEN
							curAmnt := 0;
						END IF;				
						-- End			
						INSERT INTO "CFF_TMP_DETAIL_BA" VALUES(:newDate, '1250000025', :absID, :absID, '', :bpCode, '', 0, :curAmnt, '22', 'EF');
					END IF;
				END IF;	
			END IF;
			
			CALL CFF_ADDDATE(:newDate, :toDate, :frequency, :nextDate);
			newDate := :nextDate;
		END WHILE; -- End date loop
		
		FETCH BAQry INTO fromDate, toDate, frequency, quantity, agrEfctNum, agrLineNum, unitPrice, currency, bpCode, absID, cardType;
	END WHILE; -- End BAQry loop
	CLOSE BAQry;
END;











