-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:PT:PROCESS_END

Create Procedure CRSP_GET_ITEM_GROUPS(
	IN Items NCLOB
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
Begin
	Delete From  CR_TEMP_ITME_GOUPS;
	exec 'Insert into CR_TEMP_ITME_GOUPS
	Select Distinct T1."ItmsGrpNam",T0."ItemCode" 
	From "OITM" T0 
	Join "OITB" T1	 On T0."ItmsGrpCod" = T1."ItmsGrpCod"
	Where ' || :Items;
	Select * from CR_TEMP_ITME_GOUPS;
		
End











