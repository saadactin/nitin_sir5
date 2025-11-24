Create Procedure OINV_DATE_DELTA()
As
CURSOR	CUR_DOCENTRY FOR
		SELECT "DocEntry"
		FROM OINV_DATE_DELTA_TABLE TEMP
		WHERE TEMP.OPT = 'U';
ENTRY INTEGER;
Begin
		OPEN CUR_DOCENTRY;
		FETCH CUR_DOCENTRY INTO ENTRY;
		WHILE NOT CUR_DOCENTRY::NOTFOUND DO
			Update ETL_SALES_LINES
			Set SalesEmployee = (Select "SlpCode" From OINV Where "DocEntry" = :ENTRY),
				POSTINGDATE = (Select "DocDate" From OINV where "DocEntry" = :ENTRY),
				DUEDATE = (Select "DocDueDate" From OINV Where "DocEntry" = :ENTRY),
				DOCUMENTDATE = (Select "TaxDate" From OINV Where "DocEntry" = :ENTRY)
			Where DocType = 13 And DocEntry = :ENTRY;
			FETCH CUR_DOCENTRY INTO ENTRY;
		END WHILE;
		CLOSE CUR_DOCENTRY;
End
