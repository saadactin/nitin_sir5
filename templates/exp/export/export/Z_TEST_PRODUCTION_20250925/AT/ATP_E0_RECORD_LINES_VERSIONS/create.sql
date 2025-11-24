-- B1 DEPENDS: AFTER:SP:ATP_A1_CREATE_DB_OBJECTS AFTER:PT:PROCESS_END

Create Procedure ATP_E0_RECORD_LINES_VERSIONS(
	IN involved_doc_lines DOC_LINES_VERSIONS
	)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
Begin
	Delete From DOC_LINES_VERSIONS;
	Insert Into DOC_LINES_VERSIONS
		Select T0."ObjType", T0."DocEntry", T0."DocLineNum", Max(T1."MessageID")
			From :involved_doc_lines T0 JOIN OILM T1 On T0."ObjType" = T1."TransType" And T0."DocEntry"=T1."DocEntry" And T0."DocLineNum"=T1."DocLineNum"
			Group By T0."ObjType", T0."DocEntry", T0."DocLineNum";
End;











