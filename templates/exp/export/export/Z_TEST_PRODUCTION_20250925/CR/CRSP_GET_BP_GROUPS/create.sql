-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam


Create Procedure CRSP_GET_BP_GROUPS(
	IN custs NCLOB
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
retCount integer;
BEGIN    
	call _TmSp_ValidateSpParam(custs);
	
	DELETE FROM CRSP_TEMP_OCRD;
	exec ('INSERT INTO CRSP_TEMP_OCRD SELECT T0."CardCode", T0."CardName", T0."GroupCode", T0."Balance" FROM OCRD T0 WHERE ' || :custs );
	
	Select T1."GroupCode", T1."GroupName"
	From CRSP_TEMP_OCRD T0
	Join OCRG T1 On T0."GroupCode" = T1."GroupCode";
End











