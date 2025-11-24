-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

Create Procedure CRSP_Calendar_Daily_Across_Items(
In DisplayDate NVarChar(8),
In HolidaysCode NVarChar(20), 
In EmployeeIDs CLOB, 
In UserIDs CLOB, 
In MinutesPerRow SmallInt, 
In RecurringInstances NVarChar(1000), 
In ShowEmployeeAbsEdu TinyInt,
In ShowPhoneCall TinyInt, 
In ShowMeeting TinyInt, 
In ShowOther TinyInt, 
In ShowTask TinyInt, 
In ShowPersonal TinyInt, 
In ShowServiceCalls TinyInt)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
	RecInstances nvarchar(4800);
	timev int;
	span int;
	entry int;
	tmpStr nvarchar(4800);
	curr int;
	prev int;
	Code_Sep int;
	Instance nvarchar(50);
	tmpRecontact nvarchar(100);
    tmpEndDate nvarchar(100);
    tmpClgCode nvarchar(100);
Begin

	call _TmSp_ValidateSpParam(DisplayDate);
	call _TmSp_ValidateSpParam(HolidaysCode);
	call _TmSp_ValidateSpParam(EmployeeIDs);
	call _TmSp_ValidateSpParam(UserIDs);
	call _TmSp_ValidateSpParam(RecurringInstances);
		
	delete from "CRSPCalendarDailyAcrossItemsUsers";
	delete from "CRSPCalendarDailyAcrossItemsEmployees";
	delete from "CRSPCalendarDailyAcrossItemsCalendarMD";
	delete from "CRSPCalendarDailyAcrossItemsEmployeesAbsEdu";
	delete from "CRSPCalendarDailyAcrossItemsServiceCalls";
	delete from "CRSPCalendarDailyAcrossItemsInstances";

	RecInstances := RecurringInstances;
	
	tmpStr := 'insert into "CRSPCalendarDailyAcrossItemsUsers" select "USERID", "USER_CODE", "U_NAME" from OUSR' ||
		' where "USERID" in ' || :UserIDs;	
	exec(:tmpStr);

	tmpStr := 'insert into "CRSPCalendarDailyAcrossItemsEmployees" select "empID", "firstName", "middleName", "lastName"' ||
		' from OHEM where "empID" in ' || :EmployeeIDs;
	exec(:tmpStr);

	entry := 0;
	span := :MinutesPerRow;
	timev := 0;
	
	-- Insert the Holidays
	if (length(:HolidaysCode) > 0) then
		insert into "CRSPCalendarDailyAcrossItemsCalendarMD" (HldCode, Recontact, EndDate, Details)
			select "HldCode", "StrDate", "EndDate", "Rmrks" From HLD1 where "HldCode" = :HolidaysCode
			And (("StrDate" = :DisplayDate Or "EndDate" = :DisplayDate) Or ("StrDate" < :DisplayDate And "EndDate" > :DisplayDate));
	end if;
	
	
	-- Store Employee Absence and Education to the temporary table
	if (:ShowEmployeeAbsEdu = 1) then
	
		tmpStr := 'insert into "CRSPCalendarDailyAcrossItemsEmployeesAbsEdu"(UserId, EmployeeId, StartDate, EndDate, Details)' ||
		-- Absence
			' select T1."userId", T0."empID", T0."fromDate", T0."toDate", T0."reason"' ||
			' from HEM1 T0' || 
			' inner join OHEM T1 On T1."empID" = T0."empID" ' ||
			' where ' || 
			' ((T0."fromDate" >= '''|| :DisplayDate || ''' And T0."fromDate" <= '''|| :DisplayDate || ''') Or' ||
	 		' (T0."toDate" >= '''|| :DisplayDate || ''' And T0."toDate" <= '''|| :DisplayDate || ''') Or' ||
	 		' (T0."fromDate" < '''|| :DisplayDate || ''' And T0."toDate" > '''|| :DisplayDate || ''')) And' ||
			' (T1."userId" in '|| :UserIDs || ' Or T1."empID" in '|| :EmployeeIDs || ')' ||
			' union all' ||
			-- Education
			' select T1."userId", T0."empID", T0."fromDate", T0."toDate", T2."name"' || 
			' from HEM2 T0' ||
			' inner join OHEM T1 On T1."empID" = T0."empID"' ||
			' inner join OHED T2 On T2."edType" = T0."type"' ||
			' where ' ||
			' ((T0."fromDate" >= '''|| :DisplayDate || ''' And T0."fromDate" <= '''|| :DisplayDate || ''') Or' ||
			' (T0."toDate" >= '''|| :DisplayDate || ''' And T0."toDate" <= '''|| :DisplayDate || ''') Or' ||
			' (T0."fromDate" < '''|| :DisplayDate || ''' And T0."toDate" > '''|| :DisplayDate || ''')) And' ||
			' (T1."userId" in '|| :UserIDs || ' Or T1."empID" in '|| :EmployeeIDs || ')' ||
			' order By T0."fromDate", T0."toDate" Desc';
			
		exec(:tmpStr);
	
		-- Insert Employee Absence and Education to #CalendarMD
		insert into "CRSPCalendarDailyAcrossItemsCalendarMD"(AbsEduSeq, UserId, EmployeeId, Recontact, EndDate, Details)
			select AbsEduSeq, UserId, EmployeeId, StartDate, EndDate, Details 
			from "CRSPCalendarDailyAcrossItemsEmployeesAbsEdu";
	end if;
	
	if :ShowServiceCalls = 1 then
		tmpStr := 'insert into "CRSPCalendarDailyAcrossItemsServiceCalls"(CallID, StartDate, EndDate, BeginTime, EndTime, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID, Instance)' ||
			' select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee", T2."manager", T1."userId",' || 
			' T0."technician", ''A''' ||
			' from OSCL T0 left Outer Join OHEM T1 On T1."empID" = T0."technician"' ||
			' left Outer Join OQUE T2 On T2."queueID" = T0."Queue"' ||
			' where T0."DisplInCal" = ''Y'' And' ||
			' ((T0."StartDate" >= '''|| :DisplayDate || ''' And T0."StartDate" <= '''|| :DisplayDate || ''') Or' ||
	 		' (T0."EndDate" >= '''|| :DisplayDate || ''' And T0."EndDate" <= '''|| :DisplayDate || ''') Or' ||
	 		' (T0."StartDate" < '''|| :DisplayDate || ''' And T0."EndDate" > '''|| :DisplayDate || ''')) And' ||
    		' T0."StartDate" <> T0."EndDate" And' ||
    		' (T0."assignee" in ' || :UserIDs || ' Or T2."manager" in ' || :UserIDs || ')' || 
			' union all' ||
			' select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee",'||
			' T2."manager", T1."userId", T0."technician", ''T''' ||
			' from OSCL T0 left outer join OHEM T1 On T1."empID" = T0."technician"' ||
			' left outer join OQUE T2 On T2."queueID" = T0."Queue"' ||
			' where T0."DisplInCal" = ''Y'' And' ||
			' ((T0."StartDate" >= '''|| :DisplayDate || ''' And T0."StartDate" <= '''|| :DisplayDate || ''') Or' ||
	 		' (T0."EndDate" >= '''|| :DisplayDate || ''' And T0."EndDate" <= '''|| :DisplayDate || ''') Or' ||
	 		' (T0."StartDate" < '''|| :DisplayDate || ''' And T0."EndDate" > '''|| :DisplayDate || ''')) And' ||
    		' T0."StartDate" <> T0."EndDate" And' ||
    		' (T1."userId" in ' || :UserIDs || ' Or T0."technician" in '|| :EmployeeIDs || ')' || 
			' order by T0."StartDate", T0."EndDate" Desc, T0."StartTime", T0."EndTime" Desc';
		
		exec(:tmpStr);

		-- Insert Service calls to #CalendarMD
		insert into "CRSPCalendarDailyAcrossItemsCalendarMD"(CallID, AssigneeUID, QueueManagerUID, TechUID, TechEID, SCInstance, Recontact, 
			EndDate, BeginTime, EndTime, Details)
			select CallID, AssigneeUID, QueueManagerUID, TechUID, TechEID, Instance, StartDate, EndDate, BeginTime, EndTime, Details 
			from "CRSPCalendarDailyAcrossItemsServiceCalls" order by StartDate, EndDate Desc, BeginTime, EndTime Desc, Instance;
	end if;    
	
	tmpStr := 'insert into "CRSPCalendarDailyAcrossItemsCalendarMD"(ClgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal,' ||
		' Details, UserId, EmployeeId)' ||
		' select T0."ClgCode", T0."Recontact", T0."endDate", T0."BeginTime", T0."ENDTime", T0."Action", T0."personal", T0."Details", T0."AttendUser", T0."AttendEmpl"' ||
		' from OCLG T0 join "CRSPCalendarDailyAcrossItemsUsers" T1 On T0."AttendUser" = T1.UserId where ' ||
		' (((T0."Recontact" >= '''|| :DisplayDate || ''' AND T0."Recontact" <= '''|| :DisplayDate || ''' )  or' ||
		' (T0."endDate" >= '''|| :DisplayDate || ''' AND T0."endDate" <= '''|| :DisplayDate || ''' )) or' ||
		' ( T0."Recontact" < '''|| :DisplayDate || '''  AND  T0."endDate" > '''|| :DisplayDate || ''' )' ||
		' ) AND T0."endDate" <> T0."Recontact" And (T0."RecurPat" = ''N'' Or T0."SeriesNum" is not null) And' ||
		' (T0."personal" = ''N'' ';
		
	if :ShowPersonal = 1 then
		tmpStr := :tmpStr || 'OR T0."personal" = ''Y''';
	end if;

	tmpStr := :tmpStr || ') And (T0."Action" = ''_'' ';

	if :ShowPhoneCall = 1 then 
		tmpStr := :tmpStr || 'Or T0."Action" = ''C'' ';
	end if;
	
	if :ShowMeeting = 1 then
		tmpStr := :tmpStr || 'Or T0."Action" = ''M'' ';
	end if;
	
	if :ShowOther = 1 then
		tmpStr := :tmpStr || 'Or T0."Action" = ''N'' ';
	end if;
	
	if :ShowTask = 1 then
		tmpStr := :tmpStr || 'Or T0."Action" = ''T'' ';
	end if;

	tmpStr := :tmpStr || ') union all' ||
		' select T0."ClgCode", T0."Recontact", T0."endDate", T0."BeginTime", T0."ENDTime", T0."Action", T0."personal", T0."Details", T0."AttendUser", T0."AttendEmpl"' ||
		' from OCLG T0 join "CRSPCalendarDailyAcrossItemsEmployees" T2 On T0."AttendEmpl" = T2.EmployeeID where' ||
		' (((T0."Recontact" >= '''|| :DisplayDate || ''' AND T0."Recontact" <= '''|| :DisplayDate || ''' )  OR' ||
		' (T0."endDate" >= '''|| :DisplayDate || ''' AND T0."endDate" <= '''|| :DisplayDate || ''' ) ) OR' ||
		' ( T0."Recontact" < '''|| :DisplayDate || '''  AND  T0."endDate" > '''|| :DisplayDate || ''' )' ||
		' ) AND T0."endDate" <> T0."Recontact" And' || 
		' (T0."RecurPat" = ''N'' Or T0."SeriesNum" is not null) And (T0."personal" = ''N'' ';

	if :ShowPersonal = 1 then
		tmpStr := :tmpStr || 'OR T0."personal" = ''Y''';
	end if;	
		
	tmpStr := :tmpStr || ') And (T0."Action" = ''_'' ';

	if :ShowPhoneCall = 1 then
		tmpStr := :tmpStr || 'Or T0."Action" = ''C'' ';
	end if;
	
	if :ShowMeeting = 1 then 
		tmpStr := :tmpStr || 'Or T0."Action" = ''M'' ';
	end if;
	
	if :ShowOther = 1 then
		tmpStr := :tmpStr || 'Or T0."Action" = ''N'' ';
	end if;
		
	if :ShowTask = 1 then
		tmpStr := :tmpStr || 'Or T0."Action" = ''T'' ';
	end if;
	
	tmpStr := :tmpStr || ') order by T0."Recontact", T0."BeginTime"';

	exec (:tmpStr);
	
		while length(:RecInstances) > 0 do
       curr := locate(:RecInstances, ' ');
      
       if :curr > 0 then
			Instance := SUBSTRING(:RecInstances, 0, :curr);
		else
			Instance := :RecInstances;
		end if;
		
		tmpClgCode := substr(:Instance, 0, locate(:Instance, ',')-1);
        Instance := substr_after(:Instance, ',');
        tmpRecontact := substr(:Instance, 0, locate(:Instance, ',')-1);
        tmpEndDate := rtrim(substr_after(:Instance, ','), ' ');
                      
        insert into "CRSPCalendarDailyAcrossItemsInstances" (ClgCode, Recontact, EndDate) 
       	values(:tmpClgCode, :tmpRecontact, :tmpEndDate);

        RecInstances := substr_after(RecInstances, ' ');
    end While;
    
    -- Get other information from OCLG
	update "CRSPCalendarDailyAcrossItemsInstances" T0
	set AttendUser = (select MIN(T1."AttendUser") from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
		AttendEmployee = (select MIN(T1."AttendEmpl") from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
		BeginTime = (select MIN(T1."BeginTime") from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
		EndTime = (select MIN(T1."ENDTime") from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
		Action = (select MIN(T1."Action") from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
		Personal = (select MIN(T1."personal") from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
		Details = (select MIN(T1."Details") from OCLG T1 where T0.ClgCode = T1."ClgCode");

	-- Delete the original recurring Activity, because the instances also include it
	delete from "CRSPCalendarDailyAcrossItemsCalendarMD" where clgCode in (Select Distinct clgCode From "CRSPCalendarDailyAcrossItemsInstances");
	
	-- Insert the instances filtered by users
	insert into "CRSPCalendarDailyAcrossItemsCalendarMD"(ClgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, UserId, EmployeeId)
	select clgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, AttendUser, AttendEmployee 
	From "CRSPCalendarDailyAcrossItemsInstances" T0 
	Join "CRSPCalendarDailyAcrossItemsUsers" T1 On T0.AttendUser = T1.UserID
	Where T0.Recontact = :DisplayDate Or T0.endDate = :DisplayDate Or T0.Recontact < :DisplayDate And T0.EndDate > :Displaydate;

	-- Insert the instances filtered by employees
	Insert Into "CRSPCalendarDailyAcrossItemsCalendarMD"(ClgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, UserId, EmployeeId)
	Select clgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, AttendUser, AttendEmployee 
	From "CRSPCalendarDailyAcrossItemsInstances" T0 
	Join "CRSPCalendarDailyAcrossItemsEmployees" T2 On T0.AttendEmployee = T2.EmployeeID
	Where T0.Recontact = :DisplayDate Or T0.endDate = :DisplayDate Or T0.Recontact < :DisplayDate And T0.EndDate > :Displaydate;
	    
	select * from "CRSPCalendarDailyAcrossItemsCalendarMD";   -- Order By Recontact, BeginTime

	delete from "CRSPCalendarDailyAcrossItemsUsers";
	delete from "CRSPCalendarDailyAcrossItemsEmployees";
	delete from "CRSPCalendarDailyAcrossItemsCalendarMD";
	delete from "CRSPCalendarDailyAcrossItemsEmployeesAbsEdu";
	delete from "CRSPCalendarDailyAcrossItemsServiceCalls";
	delete from "CRSPCalendarDailyAcrossItemsInstances";
End;











