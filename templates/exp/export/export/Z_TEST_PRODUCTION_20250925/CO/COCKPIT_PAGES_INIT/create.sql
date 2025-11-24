-- B1 DEPENDS: BEFORE:PT:PROCESS_START

CREATE procedure COCKPIT_PAGES_INIT
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
	seq_count integer;
BEGIN

	Select Count(*) Into seq_count From Sequences Where Schema_Name = CURRENT_SCHEMA And Sequence_name = 'COCKPIT_PAGES_S';
	If :seq_count = 0 Then
		Exec 'Create Sequence COCKPIT_PAGES_S Start With 20 Increment By 1 minvalue 1 maxvalue 4611686018427387903 no cycle';
	End IF;

END;











