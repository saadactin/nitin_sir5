Create Procedure OWHS_DELTA()
As
Begin
		Delete From ETL_OWHS Where WhsCode IN (SELECT "WhsCode" FROM  OWHS_DELTA_TABLE);
		INSERT INTO ETL_OWHS(WhsCode, WhsName) 
		Select T0."WhsCode", T0."WhsName" From OWHS T0
		INNER JOIN OWHS_DELTA_TABLE TEMP ON TEMP."WhsCode" = T0."WhsCode"
		WHERE TEMP."OPT" IN ('I', 'U');
End
