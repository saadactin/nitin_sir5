-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END

Create Procedure CRSP_Public_Calendar_TimeSection(In MinutesPerRow SmallInt)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
	time_var int;
	span int;
	entry int;
begin
	delete from "CRSPPubCTS_Calendar";

	entry := 0;
	span := :MinutesPerRow;
	time_var := 0;
	
	while :time_var < 2400 do
		insert into "CRSPPubCTS_Calendar" values(:entry, :time_var);
		
		entry := :entry + 1; 
	
		if (:span >= 60) then
			time_var := :time_var + floor(:span /60) * 100;
		else 
			time_var := time_var + :span;
			
			if mod(:time_var,100) >= 60 then
				time_var := :time_var - mod(:time_var,100) + 100;
			end if;
		end if;
		
	end while;

 	select "AbsEntry", "TimeSection" From "CRSPPubCTS_Calendar";
end;











