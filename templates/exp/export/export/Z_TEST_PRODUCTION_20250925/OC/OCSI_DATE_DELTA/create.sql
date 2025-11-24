Create Procedure OCSI_DATE_DELTA()
As
CURSOR	CUR_DOCENTRY FOR
		SELECT "DocEntry"
		FROM OCSI_DATE_DELTA_TABLE TEMP
		WHERE TEMP.OPT = 'U';
ENTRY INTEGER;
Begin
		OPEN CUR_DOCENTRY;
		FETCH CUR_DOCENTRY INTO ENTRY;
		WHILE NOT CUR_DOCENTRY::NOTFOUND DO
			Update ETL_SALES_LINES
			Set SalesEmployee = (Select "SlpCode" From OCSI Where "DocEntry" = :ENTRY),
				POSTINGDATE = (Select "DocDate" From OCSI where "DocEntry" = :ENTRY),
				DUEDATE = (Select "DocDueDate" From OCSI Where "DocEntry" = :ENTRY),
				DOCUMENTDATE = (Select "TaxDate" From OCSI Where "DocEntry" = :ENTRY)
			Where DocType = 165 And DocEntry = :ENTRY;
			FETCH CUR_DOCENTRY INTO ENTRY;
		END WHILE;
		CLOSE CUR_DOCENTRY;
End
