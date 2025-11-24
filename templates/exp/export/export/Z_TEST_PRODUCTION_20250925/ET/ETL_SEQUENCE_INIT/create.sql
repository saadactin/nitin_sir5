Create Procedure ETL_SEQUENCE_INIT()
As
seq_count integer;
Begin
	Select Count(*) Into seq_count From Sequences Where Schema_Name = CURRENT_SCHEMA And Sequence_name = 'ETL_S';
	If :seq_count = 0 Then
		Exec 'Create Sequence ETL_S Start With 1 Increment By 1 minvalue 1 maxvalue 4611686018427387903 no cycle cache 256';
	End IF;
	
End
