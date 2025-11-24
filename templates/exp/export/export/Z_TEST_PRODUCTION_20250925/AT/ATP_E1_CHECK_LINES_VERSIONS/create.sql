-- B1 DEPENDS: AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

Create Procedure ATP_E1_CHECK_LINES_VERSIONS()
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
rec_size Integer;
changed TinyInt;
Begin
		-- Check the update of Documetn Lines Versions
	VersionDiff = Select T0."ObjType", T0."DocEntry", T0."DocLineNum", Max(T1."MessageID") as "NewVersion"
		From DOC_LINES_VERSIONS T0
		Join OILM T1 On T0."ObjType" = T1."TransType" And
						T0."DocEntry" = T1."DocEntry" And
						T0."DocLineNum" = T1."DocLineNum"
		Group By T0."ObjType", T0."DocEntry", T0."DocLineNum", T0."Version"
		Having T0."Version" < Max(T1."MessageID");
	Select Count(*) into rec_size From :VersionDiff;
	IF :rec_size > 0 Then
		changed := 1;
	Else
		changed := 0;
	End If;
	Select :changed From Dummy;
End;











