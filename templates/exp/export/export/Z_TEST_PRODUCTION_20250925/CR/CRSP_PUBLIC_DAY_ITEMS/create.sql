-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

--drop procedure CRSP_Public_Day_Items;           
create procedure CRSP_Public_Day_Items (
	in DisplayDate nvarchar(8),
	in EmployeeIDs clob,
	in UserIDs clob,
	in MinBegin_in int,				-- Minimum Begin Worktime in the week
	in MaxEnd_in int,				-- Maximum End Worktime in the week
	in WorkBegin_in int,
	in WorkEnd_in int,
	in MinutesPerRow smallint,
	in RecurringInstances_in nvarchar(4000),
	in Visible tinyint,
	in ShowPhoneCall tinyint,
	in ShowMeeting tinyint,
	in ShowOther tinyint,
	in ShowTask tinyint,
	in ShowPersonal tinyint,
	in ShowServiceCall tinyint
) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
	timev int; 
	span int;
	entry int;
	tmpStr nvarchar(4000);
	--sqlStr nvarchar(2000);
	curr int;
	prev int;
	Code_Sep int;
	Instance nvarchar(50);
	RecurringInstances nvarchar(4000);
	ds int;
	MinBegin int;
	MaxEnd int;
	WorkBegin int;
	WorkEnd int;
	tmpClgCode nvarchar(100);
    tmpRecontact nvarchar(100);
    tmpEndDate nvarchar(100);
    ClgCode int; 
    Recontact TIMESTAMP; 
    BeginTime int;  
    EndTime int; 
    Actionv nvarchar(1); 
    Personal nvarchar(1); 
    Details nvarchar(120); 
    Userv int;
    Employee int;
    CallID int;
    StartDate TIMESTAMP;
    EndDate TIMESTAMP;
    AssigneeUID int;
    QueueManagerUID int;
    TechUID int; 
    TechEID int; 
    SCInstance nvarchar(1);
	cnt int;
	i int;
    
cursor activityCursor for select * from "CRSPPubDayItems_Activities" order by "Recontact", "BeginTime";
cursor serviceCallCursor for select * from "CRSPPubDayItems_ServiceCalls" 
	order by "StartDate", "EndDate" Desc, "BeginTime", "EndTime" Desc, "Instance";
cursor actCursor(WorkBegin int, WorkEnd int) for select "ClgCode", "Action", "Personal", "Details", "UserId", "EmployeeId" from "CRSPPubDayItems_Activities"
	where "EndTime" <= :WorkBegin Or "BeginTime" >= :WorkEnd order by "BeginTime";
cursor scCursor(WorkBegin int, WorkEnd int) for select "CallID", "Instance", "Details", "AssigneeUID", "QueueManagerUID", "TechUID", "TechEID" from "CRSPPubDayItems_ServiceCalls" 
	where "EndTime" <= :WorkBegin Or "BeginTime" >= :WorkEnd order by "StartDate", "EndDate" Desc, "BeginTime", "EndTime" Desc, "Instance";
Begin
	
    call _TmSp_ValidateSpParam(DisplayDate);
	call _TmSp_ValidateSpParam(RecurringInstances_in);
	call _TmSp_ValidateSpParam(EmployeeIDs);
	call _TmSp_ValidateSpParam(UserIDs);
	
	entry := 0;
	span := :MinutesPerRow;
	timev := 0;
	
	--delete from "CRSPPubDayItems_OffTimeActs";
	--delete from "CRSPPubDayItems_OffTimeSC";
	delete from "CRSPPubDayItems_Instances";
	delete from "CRSPPubDayItems_Activities";
	delete from "CRSPPubDayItems_ServiceCalls";
	delete from "CRSPPubDayItems_Calendar";
	delete from "CRSPPubDayItems_Users";
	delete from "CRSPPubDayItems_Employees";
	
	RecurringInstances := :RecurringInstances_in;
	
	MinBegin := :MinBegin_in;
	MaxEnd := :MaxEnd_in;
	WorkBegin := :WorkBegin_in;
	WorkEnd := :WorkEnd_in;
	
	if :Visible = 0 then
		return;
	end if;
	
	tmpStr := 'insert into "CRSPPubDayItems_Users" select "USERID", "USER_CODE", "U_NAME" from OUSR where "USERID" in ' || :UserIDs;
	exec (:tmpStr);

	tmpStr := 'insert into "CRSPPubDayItems_Employees" select "empID", "firstName", "middleName", "lastName" from OHEM where "empID" in ' ||
		 :EmployeeIDs;
	exec (:tmpStr);
	
	While :timev < 2400 do
		insert into "CRSPPubDayItems_Calendar" ("AbsEntry", "TimeSection") Values(:entry, :timev);
		entry := :entry + 1;

		if :span >= 60 then
			timev := :timev + floor(:span / 60) * 100;
		else
			timev := timev + :span;
			if mod(timev,100) >= 60 then
				timev := :timev - mod(:timev, 100) + 100;
			end if;
		end if;
	end while;

	-- Round the Begin and End times
	if :span < 60 then 
		MinBegin := :MinBegin - mod(mod(:MinBegin, 100), :span);
		MaxEnd := :MaxEnd - mod(mod(:MaxEnd, 100), :span);
	
		WorkBegin := :WorkBegin - mod(mod(:WorkBegin, 100), :span);
		WorkEnd := :WorkEnd - mod(mod(:WorkEnd, 100), :span);
	else
	    ds := floor(:span/60);
		MinBegin := floor(floor(MinBegin/100)/:ds) * :ds * 100;
		MaxEnd := floor(floor(MaxEnd/100)/:ds) * :ds * 100;
	
		WorkBegin := floor(floor(:WorkBegin/100)/:ds) * :ds * 100;
		WorkEnd := floor(floor(:WorkEnd/100)/:ds) * :ds * 100;
	end if;
	
	-- Store the filtered activites to the temporary table
	tmpStr := 'insert into "CRSPPubDayItems_Activities" select T0."ClgCode", T0."Recontact", T0."BeginTime", T0."ENDTime", T0."Action",' ||
		' T0."personal", T0."Details", T0."AttendUser", T0."AttendEmpl" From OCLG T0' ||
		' join "CRSPPubDayItems_Users" T1 On T0."AttendUser" = T1."UserID"' ||
		' where (T0."RecurPat" = ''N'' Or T0."SeriesNum" is not null) And T0."IsRemoved" = ''N'' And' ||
		' T0."inactive" = ''N'' And T0."Recontact" = ''' || :DisplayDate || ''' And T0."Recontact" = T0."endDate"' ||
		' And (T0."personal" = ''N'' ';
				
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

	tmpStr := :tmpStr || ') union all ';
	tmpStr := :tmpStr || 'select T0."ClgCode", T0."Recontact", T0."BeginTime", T0."ENDTime", T0."Action", T0."personal",' ||
		 ' T0."Details", T0."AttendUser", T0."AttendEmpl" From OCLG T0' ||
		 ' join "CRSPPubDayItems_Employees" T2 On T0."AttendEmpl" = T2."EmployeeID"' ||
		 ' where (T0."RecurPat" = ''N'' Or T0."SeriesNum" is not null) And T0."IsRemoved" = ''N'' And T0."inactive" = ''N''' ||
		 ' And T0."Recontact" = ''' || :DisplayDate || ''' And T0."Recontact" = T0."endDate" And (T0."personal" = ''N'' ';

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

	tmpStr := :tmpStr || ') Order By T0."Recontact", T0."BeginTime"';
		
	--insert into TST values(:tmpStr);
	exec (:tmpStr);
	
	-- Insert the recurring instances
	-- Store the recurring instances in the temporary table
	
	while length(:RecurringInstances) > 0 do
       curr := locate(:RecurringInstances, ' ');
      
       if :curr > 0 then
			Instance := SUBSTRING(:RecurringInstances, 0, :curr);
		else
			Instance := :RecurringInstances;
		end if;
		
		tmpClgCode := substr(:Instance, 0, locate(:Instance, ',')-1);
        Instance := substr_after(:Instance, ',');
        tmpRecontact := substr(:Instance, 0, locate(:Instance, ',')-1);
        tmpEndDate := rtrim(substr_after(:Instance, ','), ' ');
                      
        insert into "CRSPPubDayItems_Instances" ("ClgCode", "Recontact", "EndDate") 
       	values(:tmpClgCode, :tmpRecontact, :tmpEndDate);

        RecurringInstances := substr_after(RecurringInstances, ' ');
    end While;
    
    	-- Get other information from OCLG
	update "CRSPPubDayItems_Instances" T0
	set ("AttendUser", "AttendEmployee", "BeginTime", "EndTime", "Action", "Personal", "Details")  = 
		(select min(T1."AttendUser"), min(T1."AttendEmpl"), min(T1."BeginTime"), min(T1."ENDTime"), 
			min(T1."Action"), min(T1."personal"), min(T1."Details") from OCLG T1 where T1."ClgCode" = T0."ClgCode")
		where exists (select 1 from OCLG TX where TX."ClgCode" = T0."ClgCode");
		
	-- Delete the original recurring Activity, because the instances also include it
	delete from "CRSPPubDayItems_Activities"
	where "ClgCode" in (select distinct "ClgCode" from "CRSPPubDayItems_Instances");

	-- Insert the instances for users
	insert into "CRSPPubDayItems_Activities"
	select "ClgCode", "Recontact", "BeginTime", "EndTime", "Action", "Personal", "Details", "AttendUser", "AttendEmployee"
	from "CRSPPubDayItems_Instances" T0 
	join "CRSPPubDayItems_Users" T1 On T0."AttendUser" = T1."UserID"
	where T0."Recontact" = :DisplayDate;

	-- Insert the instances for employees
	insert into "CRSPPubDayItems_Activities" 
	select "ClgCode", "Recontact", "BeginTime", "EndTime", "Action", "Personal", "Details", "AttendUser", "AttendEmployee"
	from "CRSPPubDayItems_Instances" T0 
	join "CRSPPubDayItems_Employees" T2 On T0."AttendEmployee" = T2."EmployeeID"
	where T0."Recontact" = :DisplayDate;
    
 	--select * From "CRSPPubDayItems_Calendar";
	--select * from "CRSPPubDayItems_Instances";

	open activityCursor;
	fetch activityCursor into ClgCode, Recontact, BeginTime, EndTime, Actionv, Personal, Details, Userv, Employee;
	
	while not activityCursor::NOTFOUND do
	
		if :span < 60 then 
			BeginTime := :BeginTime - mod(mod(:BeginTime, 100), :span);
			EndTime := (:EndTime - 1) - mod(mod((:EndTime - 1), 100), :span); -- If an activity?s @EndTime happens to be equal to the @WorkBegin, it should not show in the working time.
		else
			ds := floor(:span/60);
			BeginTime := floor(floor(:BeginTime/100)/:ds) * :ds * 100;
			EndTime := floor(floor(:EndTime/100)/:ds) * :ds * 100;			
		end if;
	
		if :EndTime >= :WorkBegin And :BeginTime < :WorkEnd then
			update "CRSPPubDayItems_Calendar"
			set "ClgCode1" = :ClgCode, "Action1" = :Actionv, "Personal1" = :Personal, "Details1" = :Details, "User1" = :Userv, "Employee1" = :Employee
			where "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime 
			And (select max("ClgCode1") from "CRSPPubDayItems_Calendar" where "TimeSection" >= :BeginTime and "TimeSection" <= :EndTime) is null;
		
			update "CRSPPubDayItems_Calendar"
			set "ClgCode2" = :ClgCode, "Action2" = :Actionv, "Personal2" = :Personal, "Details2" = :Details, "User2" = :Userv, "Employee2" = :Employee
			where "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime
			And ("ClgCode1" <> :ClgCode Or "ClgCode1" Is NULL)
			And	(select max("ClgCode2") from "CRSPPubDayItems_Calendar" where "TimeSection" >= :BeginTime and "TimeSection" <= :EndTime) is null;

			update "CRSPPubDayItems_Calendar"
			set "ClgCode3" = :ClgCode, "Action3" = :Actionv, "Personal3" = :Personal, "Details3" = :Details, "User3" = :Userv, "Employee3" = :Employee
			where "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime
			And ("ClgCode1" <> :ClgCode Or "ClgCode1" Is NULL) And ("ClgCode2" <> :ClgCode  Or "ClgCode2" is null)
			And	(select max("ClgCode3") from "CRSPPubDayItems_Calendar" where "TimeSection" >= :BeginTime and "TimeSection" <= :EndTime) is null;
		end if;
		
		fetch activityCursor into ClgCode, Recontact, BeginTime, EndTime, Actionv, Personal, Details, Userv, Employee;
	end while;
	
	close activityCursor;
	
	-- Store Service calls to the temporary table
	If :ShowServiceCall = 1 then
		--(CallID, StartDate, EndDate, BeginTime, EndTime, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID, Instance)
		tmpStr := 'insert into "CRSPPubDayItems_ServiceCalls"' ||
			' select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime",' ||
			' T0."subject", T0."assignee", T2."manager", T1."userId", T0."technician", ''A''' ||
			' From OSCL T0' ||
			' left outer join OHEM T1 On T1."empID" = T0."technician"' ||
			' left outer join OQUE T2 On T2."queueID" = T0."Queue"' ||
			' where T0."DisplInCal" = ''Y'' And' ||
			' T0."StartDate" =''' || :DisplayDate || ''' And' || 
			' T0."EndDate" =''' || :DisplayDate || ''' And' ||
    		' (T0."assignee" in ' || :UserIDs || ' Or T2."manager" in ' || :UserIDs || ')' 
			' Union All' ||
			' Select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", ' ||
			' T0."subject", T0."assignee", T2."manager", T1."userId", T0."technician", ''T''' ||
			' From OSCL T0' ||
			' Left Outer Join OHEM T1 On T1."empID" = T0."technician"' ||
			' Left Outer Join OQUE T2 On T2."queueID" = T0."Queue"' ||
			' where T0."DisplInCal" = ''Y'' And' ||
			' T0."StartDate" = ''' || :DisplayDate || ''' And' || 
			' T0."EndDate" =''' || :DisplayDate || ''' And' ||
    		' (T1."userId" in ' || :UserIDs || ' Or T0."technician" in ' || :EmployeeIDs || ')' 
			' order by T0."StartDate", T0."EndDate" Desc, T0."StartTime", T0."EndTime" Desc';
		
		--insert into TST values(:tmpStr);
		exec(:tmpStr);
	end if;    
	
	-- Rollover Service Calls and update #Calendar
	open serviceCallCursor;
	fetch serviceCallCursor Into CallID, StartDate, EndDate, BeginTime, EndTime, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID, SCInstance;

	while not serviceCallCursor::NOTFOUND do
	
		if :span < 60 then 
			BeginTime := :BeginTime - mod(mod(:BeginTime, 100), :span);
			EndTime := (:EndTime - 1) - mod(mod((:EndTime - 1), 100), :span); -- If an service call's @EndTime happens to be equal to the @WorkBegin, it should not show in the working time.
		else
			ds := floor(:span/60);
			BeginTime := floor(floor(:BeginTime/100)/:ds) * :ds * 100;
			EndTime := floor(floor(:EndTime/100)/:ds) * :ds * 100;			
		end if;

		update "CRSPPubDayItems_Calendar"
		set "CallID1" = :CallID, "SCInstance1" = :SCInstance, "Details1" = :Details, "AssigneeUID1" = :AssigneeUID, 
			"QueueManagerUID1" = :QueueManagerUID, "TechUID1" = :TechUID, "TechEID1" = :TechEID
		where :EndTime >= :WorkBegin And :BeginTime < :WorkEnd And "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime 
			And (Select Max("ClgCode1") From "CRSPPubDayItems_Calendar" where "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime) is null
			And (select max("CallID1") from "CRSPPubDayItems_Calendar" where "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime) is null;
	
		update "CRSPPubDayItems_Calendar"
 		set "CallID2" = :CallID, "SCInstance2" = :SCInstance, "Details2" = :Details, "AssigneeUID2" = :AssigneeUID, 
 			"QueueManagerUID2" = :QueueManagerUID, "TechUID2" = :TechUID, "TechEID2" = :TechEID
 		where :EndTime >= :WorkBegin And :BeginTime < :WorkEnd And "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime
			And ("CallID1" <> :CallID Or "SCInstance1" <> :SCInstance Or "CallID1" is null)
			And	(select max("ClgCode2") from "CRSPPubDayItems_Calendar" where "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime) is null
			And	(select max("CallID2") from "CRSPPubDayItems_Calendar" where "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime) is null;

		update "CRSPPubDayItems_Calendar"
 		set "CallID3" = :CallID, "SCInstance3" = :SCInstance, "Details3" = :Details, "AssigneeUID3" = :AssigneeUID, 
 			"QueueManagerUID3" = :QueueManagerUID, "TechUID3" = :TechUID, "TechEID3" = :TechEID
 		where :EndTime >= :WorkBegin And :BeginTime < :WorkEnd And "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime
			And ("CallID1" <> :CallID Or "SCInstance1" <> :SCInstance Or "CallID1" is null) 
			And ("CallID2" <> :CallID Or "SCInstance2" <> :SCInstance Or "CallID2" is null)
			And	(select max("CallID3") from "CRSPPubDayItems_Calendar" where "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime) is null
			And	(select max("ClgCode3") from "CRSPPubDayItems_Calendar" where "TimeSection" >= :BeginTime And "TimeSection" <= :EndTime) is null;

		fetch ServiceCallCursor into CallID, StartDate, EndDate, BeginTime, EndTime, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID, SCInstance;
	end while;

	close ServiceCallCursor;
	
	-- Clear the activities and service calls in offtime
	update "CRSPPubDayItems_Calendar"
	set "EntryCount"=NULL, 
	"ClgCode1"=NULL, "Action1"=NULL, "Personal1"=NULL, "Details1"=NULL, "User1"=NULL, "Employee1"=NULL, "CallID1"=NULL, "SCInstance1"=NULL, "AssigneeUID1"=NULL, 
	"QueueManagerUID1"=NULL, "TechUID1"=NULL, "TechEID1"=NULL,
	"ClgCode2"=NULL, "Action2"=NULL, "Personal2"=NULL, "Details2"=NULL, "User2"=NULL, "Employee2"=NULL, "CallID2"=NULL, "SCInstance2"=NULL, "AssigneeUID2"=NULL, 
	"QueueManagerUID2"=NULL, "TechUID2"=NULL, "TechEID2"=NULL,
	"ClgCode3"=NULL, "Action3"=NULL, "Personal3"=NULL, "Details3"=NULL, "User3"=NULL, "Employee3"=NULL, "CallID3"=NULL, "SCInstance3"=NULL, "AssigneeUID3"=NULL, 
	"QueueManagerUID3"=NULL, "TechUID3"=NULL, "TechEID3"=NULL
	where "TimeSection" < :WorkBegin Or "TimeSection" >= :WorkEnd;
	
	--------------Move the Off-time activities to the bottom (Begin)------------------
	
	open actCursor(:WorkBegin,:WorkEnd);
	fetch actCursor into ClgCode, Actionv, Personal, Details, Userv, Employee;
	i := 1;
	
	while not actCursor::NOTFOUND  do
		--insert into "CRSPPubDayItems_OffTimeActs" values(:i, 1, :ClgCode, :Actionv, :Personal, :Details, :Userv, :Employee); 
		
		update "CRSPPubDayItems_Calendar"
		set "EntryCount" = 1, "ClgCode1" = :ClgCode, "Action1" = Actionv, "Personal1" = Personal, "Details1" = Details, "User1" = :Userv, "Employee1" = :Employee
		where "TimeSection" = :MaxEnd + floor((:i-1)*:span/60)*100 + mod((:i-1)*:span, 60);
		i := :i + 1;
	
		--insert into TST values(:i || ' ' || :WorkBegin ||' ' || :WorkEnd ||' ' || floor((:i-1)*:span/60)*100 + mod((:i-1)*:span, 60) || ' ' || :MaxEnd);
				
		fetch actCursor into ClgCode, Actionv, Personal, Details, Userv, Employee;
	end while;
	
	close actCursor;
	--------------Move the Off-time activities to the bottom (End)------------------
	
	--------------Move the Off-time service calls to the bottom (Begin)------------------
	open scCursor(:WorkBegin, :WorkEnd);
	fetch scCursor into CallID, Instance, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID;
	i := 1;
	
	select count(1) into cnt from "CRSPPubDayItems_ServiceCalls" where "EndTime" <= :WorkBegin Or "BeginTime" >= :WorkEnd;
	
	while not scCursor::NOTFOUND  do
		--insert into "CRSPPubDayItems_OffTimeSC" values (:i, 1, :CallID, :Instance, :Details, :AssigneeUID, :QueueManagerUID, :TechUID, :TechEID);
	
		update "CRSPPubDayItems_Calendar"	
		set "EntryCount" = 1, "CallID1" = :CallID, "SCInstance1" = :Instance, "Details1" = :Details, "AssigneeUID1" = :AssigneeUID, 
			"QueueManagerUID1" = :QueueManagerUID, "TechUID1" = :TechUID, "TechEID1" = :TechEID
		where "TimeSection" = :MaxEnd + floor((:cnt + :i - 1)*:span/60)*100 + mod((:cnt + :i - 1)*:span, 60);
	
		i := :i + 1;
		fetch scCursor into CallID, Instance, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID;
	end while;
	close scCursor;

	--------------Move the Off-time service calls to the bottom (End)------------------

	delete from "CRSPPubDayItems_CalT";
	delete from "CRSPPubDayItems_CalM";
	
	insert into "CRSPPubDayItems_CalT"
	select "AbsEntry", null, ifnull("CallID1",0), ifnull("CallID2",0), ifnull("CallID3",0),
	 ifnull("ClgCode1",0), ifnull("ClgCode2",0), ifnull("ClgCode3",0) from "CRSPPubDayItems_Calendar";

	insert into "CRSPPubDayItems_CalM" select ifnull(T1."CallID1",0), ifnull(T1."ClgCode1",0),
		(1 + (case when COUNT(distinct T1."ClgCode2") > 0 then 1 else (case when COUNT(Distinct T1."CallID2") > 0 then 1 else 0 end) end) +
		(case When COUNT(distinct T1."ClgCode3") > 0 then 1 else (case when COUNT(Distinct T1."CallID3") > 0 then 1 else 0 end) end))
	from "CRSPPubDayItems_Calendar" T1 
	where T1."ClgCode1" is not null or T1."CallID1" is not null
	group by T1."ClgCode1", T1."CallID1";
	
	update "CRSPPubDayItems_CalT" TX
	set "EntryCount" = (select "EntryCount" from "CRSPPubDayItems_CalM" T1 
							where T1."ClgCode" = TX."ClgCode1" and T1."CallID" = TX."CallID1")
	where TX."ClgCode1" in (select "ClgCode" from "CRSPPubDayItems_CalM") and 
		TX."CallID1" in (select "CallID" from "CRSPPubDayItems_CalM");
	
	delete from "CRSPPubDayItems_CalM";
	insert into "CRSPPubDayItems_CalM"
	select ifnull(T1."CallID2",0), ifnull(T1."ClgCode2",0),
		(1 + (case when COUNT(distinct T1."ClgCode3") > 0 then 1 else (case when COUNT(Distinct T1."CallID3") > 0 then 1 else 0 end) end) +
		(case When COUNT(distinct T1."ClgCode1") > 0 then 1 else (case when COUNT(Distinct T1."CallID1") > 0 then 1 else 0 end) end))
	from "CRSPPubDayItems_Calendar" T1 
	where T1."ClgCode2" is not null or T1."CallID2" is not null
	group by T1."ClgCode2", T1."CallID2";
	
	update "CRSPPubDayItems_CalT" TX
	set "EntryCount" = (select "EntryCount" from "CRSPPubDayItems_CalM" T1 where T1."ClgCode" = TX."ClgCode2" and T1."CallID" = TX."CallID2")
	where TX."ClgCode2" in (select "ClgCode" from "CRSPPubDayItems_CalM") and TX."CallID2" in (select "CallID" from "CRSPPubDayItems_CalM");

	delete from "CRSPPubDayItems_CalM";
	insert into "CRSPPubDayItems_CalM" select ifnull(T1."CallID3",0), ifnull(T1."ClgCode3",0),
		(1 + (case when COUNT(distinct T1."ClgCode2") > 0 then 1 else (case when COUNT(Distinct T1."CallID2") > 0 then 1 else 0 end) end) +
		(case When COUNT(distinct T1."ClgCode1") > 0 then 1 else (case when COUNT(Distinct T1."CallID1") > 0 then 1 else 0 end) end))
	from "CRSPPubDayItems_Calendar" T1 
	where T1."ClgCode3" is not null or T1."CallID3" is not null
	group by T1."ClgCode3", T1."CallID3";
	
	update "CRSPPubDayItems_CalT" TX
	set "EntryCount" = (select "EntryCount" from "CRSPPubDayItems_CalM" T1 where T1."ClgCode" = TX."ClgCode3" and T1."CallID" = TX."CallID3")
	where TX."ClgCode3" in (select "ClgCode" from "CRSPPubDayItems_CalM") and TX."CallID3" in (select "CallID" from "CRSPPubDayItems_CalM");  

	update "CRSPPubDayItems_CalT" TC
	set TC."EntryCount" = (select MAX(TX."EntryCount") from "CRSPPubDayItems_CalT" TX 
						where TX."ClgCode1" = TC."ClgCode1" and TX."CallID1" = TC."CallID1" 
						group by TX."ClgCode1", TX."CallID1")
	where TC."CallID1" > 0 or TC."ClgCode1" > 0; 

	update "CRSPPubDayItems_CalT" TC
	set TC."EntryCount" = (select MAX(TX."EntryCount") from "CRSPPubDayItems_CalT" TX 
						where TX."ClgCode2" = TC."ClgCode2" and TX."CallID2" = TC."CallID2" 
						group by TX."ClgCode2", TX."CallID2")
	where TC."CallID2" > 0 or TC."ClgCode2" > 0; 

	update "CRSPPubDayItems_CalT" TC
	set TC."EntryCount" = (select MAX(TX."EntryCount") from "CRSPPubDayItems_CalT" TX 
						where TX."ClgCode3" = TC."ClgCode3" and TX."CallID3" = TC."CallID3" 
						group by TX."ClgCode3", TX."CallID3")
	where TC."CallID3" > 0 or TC."ClgCode3" > 0; 
	
	update "CRSPPubDayItems_Calendar" TC
		set "EntryCount" = (select TX."EntryCount" from "CRSPPubDayItems_CalT" TX where TX."AbsEntry" = TC."AbsEntry"); 
	
	delete from "CRSPPubDayItems_CalT";
	delete from "CRSPPubDayItems_CalM";
							
	-------------Set the begintimes and endtimes---------------------------
	-- Activities
	update "CRSPPubDayItems_Calendar" T0
		set ("BeginTime1", "EndTime1")  = (select MIN("BeginTime"), MIN("ENDTime") from OCLG where "ClgCode" = T0."ClgCode1")
		where exists (select 1 from OCLG T1 where T1."ClgCode" = T0."ClgCode1");
			
	update "CRSPPubDayItems_Calendar" T0
		set ("BeginTime2", "EndTime2")  = (select MIN("BeginTime"), MIN("ENDTime") from OCLG where "ClgCode" = T0."ClgCode2")
		where exists (select 1 from OCLG T1 where T1."ClgCode" = T0."ClgCode2");
			
	update "CRSPPubDayItems_Calendar" T0
		set ("BeginTime3", "EndTime3")  = (select MIN("BeginTime"), MIN("ENDTime") from OCLG where "ClgCode" = T0."ClgCode3")
		where exists (select 1 from OCLG T1 where T1."ClgCode" = T0."ClgCode3");
			
	-- Service Calls	 	
	update "CRSPPubDayItems_Calendar" T0 
		set ("BeginTime1", "EndTime1")  = (select MIN ("BeginTime"), MIN("EndTime") from "CRSPPubDayItems_ServiceCalls" where "CallID" = T0."CallID1")
		where exists (select 1 from "CRSPPubDayItems_ServiceCalls" where "CallID" = T0."CallID1");
	
	update "CRSPPubDayItems_Calendar" T0 
		set ("BeginTime2", "EndTime2")  = (select MIN ("BeginTime"), MIN("EndTime") from "CRSPPubDayItems_ServiceCalls" where "CallID" = T0."CallID2")
		where exists (select 1 from "CRSPPubDayItems_ServiceCalls" where "CallID" = T0."CallID2");
	
	update "CRSPPubDayItems_Calendar" T0 
		set ("BeginTime3", "EndTime3")  = (select MIN ("BeginTime"), MIN("EndTime") from "CRSPPubDayItems_ServiceCalls" where "CallID" = T0."CallID3")
		where exists (select 1 from "CRSPPubDayItems_ServiceCalls" where "CallID" = T0."CallID3");
		 		
	select * from "CRSPPubDayItems_Calendar";
	
	--delete from "CRSPPubDayItems_OffTimeActs";
	--delete from "CRSPPubDayItems_OffTimeSC";
	delete from "CRSPPubDayItems_Instances";
	delete from "CRSPPubDayItems_Activities";
	delete from "CRSPPubDayItems_ServiceCalls";
	delete from "CRSPPubDayItems_Calendar";
	delete from "CRSPPubDayItems_Users";
	delete from "CRSPPubDayItems_Employees";
End;











