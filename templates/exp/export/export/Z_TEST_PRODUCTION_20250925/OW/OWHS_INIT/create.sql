Create Procedure OWHS_INIT()
As
Begin
	Insert Into ETL_OWHS(WhsCode, WhsName) 
		Select "WhsCode", "WhsName" From OWHS;
End
