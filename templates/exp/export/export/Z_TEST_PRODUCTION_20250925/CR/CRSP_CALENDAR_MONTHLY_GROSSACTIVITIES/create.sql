-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

Create Procedure CRSP_Calendar_Monthly_GrossActivities (
in ActivityTypes int, 
in EmployeeIDList nclob, 
in FirstWeekDay int, 
in HolidaysCode nvarchar(40), 
in GotoDate nvarchar(30),
in RecurringInstancesMonth nclob, 
in ShowEmployeeEdu tinyint, 
in ShowPersonal tinyint, 
in ShowServiceCalls int, 
in UserIDList nclob)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER 
As
	RdsLmt int;
	ClgCode int;
	AbsEduSeq int;
	CallID int; 
	SCInstance nvarchar(1);
	Diff int;
	Iter int;
	IsClg int;
	IsAbsEdu int;
	IsScl int;
	DateStartPoint TIMESTAMP;
	DateEndPoint TIMESTAMP;
	DateFrom TIMESTAMP;
	HldCode nvarchar(40);
	QryStr nvarchar(400);
	Personal nvarchar(1);
	PhoneCall nvarchar(1);
	Meeting nvarchar(1);
	Other nvarchar(1);
	Task nvarchar(1);
-- recurring scenatio
	RecurringInstances nclob;
	Curr int;
	Prev int;
	Code_Sep int;
	Instance nvarchar(50);
	StartDate TIMESTAMP;
	EndDate TIMESTAMP;
	ActTypes int;
	ShowPhoneCall tinyint;
	ShowMeeting tinyint;
	ShowOther tinyint;
	ShowTask tinyint;
	Counter int;
	recLen int;
	tmpClgCode nvarchar(100);
	tmpRecontact nvarchar(100);
	tmpEndDate nvarchar(100);
	tmpStr NCLOB;
	
-- Cursor for Holidays
cursor CursorHld (DateStartPoint TIMESTAMP, DateEndPoint TIMESTAMP) for select "StrDate", "EndDate" From HLD1 
	Where "HldCode" = :HolidaysCode And  (("StrDate" >= :DateStartPoint And "StrDate" <= :DateEndPoint) Or
	 	("EndDate" >= :DateStartPoint And "EndDate" <= :DateEndPoint) Or ("StrDate" < :DateStartPoint And "EndDate" > :DateEndPoint)) 
	order by "StrDate", "EndDate";
-- Cursor for Employee absence and education
cursor CursorAbsEdu for select "AbsEduSeq", "StartDate", "EndDate" From "CRSPCalMGA_EmployeesAbsEdu"
	order by "StartDate", "EndDate" Desc;
-- Cursor for Service calls
cursor CursorScl for select "CallID", "Instance", "StartDate", "EndDate" From "CRSPCalMGA_ServiceCalls"
	order by "StartDate", "EndDate" Desc, "BeginTime", "EndTime" Desc, "Instance";
-- Cursor for Activities
cursor CursorClg for select "ClgCode", "ReContact", "EndDate" From "CRSPCalMGA_Clg"
	order by "ReContact", "EndDate" Desc, "BeginTime", "EndTime" Desc;	
Begin

	call _TmSp_ValidateSpParam(EmployeeIDList);
	call _TmSp_ValidateSpParam(HolidaysCode);
	call _TmSp_ValidateSpParam(GotoDate);
	call _TmSp_ValidateSpParam(RecurringInstancesMonth);
	call _TmSp_ValidateSpParam(UserIDList);
	
	RdsLmt := 7;
	Iter := 0;
	ActTypes := :ActivityTypes;
	Personal := 'N';
	PhoneCall := '';
	Task := '';
	Meeting := '';
	Other := '';
	Curr := 1;
	Prev := 1;
	RecurringInstances := :RecurringInstancesMonth;
	Counter := 0;
		
	if :ShowPersonal = 1 then
		Personal := 'Y';
	end if;
	
	ShowTask := mod(floor(:ActTypes/1000), 10);
	ShowPhoneCall := mod(floor(:ActTypes/100), 10);
	ShowMeeting := mod(floor(:ActTypes/10), 10);
	ShowOther := mod(:ActTypes, 10); 
	
	if :ShowTask = 1 then
		Task := 'T';
	end if;
	
	if :ShowPhoneCall = 1 then
		PhoneCall := 'C';
	end if;
	
	if :ShowMeeting = 1 then
		Meeting := 'M';
	end if;
	
	if :ShowOther = 1 then
		Other := 'N';
	end if;	
	
	DateStartPoint := ADD_DAYS(TO_DATE(:GotoDate), 1 - dayofmonth(:GotoDate));
	Diff := :FirstWeekDay - (weekday(:DateStartPoint) + 2);
	
	if :Diff > 0 then
		Diff := :Diff - 7;
	end if;
	
	DateStartPoint := Add_days(:DateStartPoint, :Diff);
	DateEndPoint := Add_days(:DateStartPoint, 35 - 1);
	
	delete from "CRSPCalMGA_Res";
	delete from "CRSPCalMGA_Seq";
	delete from "CRSPCalMGA_Clg";
	delete from "CRSPCalMGA_EmployeesAbsEdu";
	delete from "CRSPCalMGA_ServiceCalls";
	
	while :Counter < :RdsLmt + 23 do
		insert into "CRSPCalMGA_Seq" values(:Counter + 1, 0, 0, 0, '', null, null, 0);	
		Counter := :Counter + 1;
	end while;
	
	--- insert activity recurring instances
	recLen := length(:RecurringInstances);
	Counter := 1;
	
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
                      
        insert into "CRSPCalMGA_Clg" 
		select :tmpClgCode, :tmpRecontact, :tmpEndDate, 0, 0 from DUMMY;
        Counter := :Counter + 1;

        RecurringInstances := substr_after(RecurringInstances, ' ');
    End While;
		
	Update "CRSPCalMGA_Clg" T0 
	Set T0."BeginTime" = (select T1."BeginTime" from OCLG T1 where T0."ClgCode" = T1."ClgCode"),
		T0."EndTime" = (select T1."ENDTime" from OCLG T1 where T0."ClgCode" = T1."ClgCode"); 
		
	--- insert activity normal instances into temporary table
	tmpStr := ' insert into "CRSPCalMGA_Clg"' || 
	' Select "ClgCode", "Recontact", "endDate", "BeginTime", "ENDTime" ' || 
	' From OCLG T0 ' || 
	' Where "personal" IN (''N'', ''' || :Personal || ''') And "Action" IN (''' || :PhoneCall || ''','''|| :Meeting ||''','''|| :Other ||''', '''|| :Task ||''') And ' || 
		' ("AttendUser" IN (' || :UserIDList || ') Or "AttendEmpl" IN ('|| :EmployeeIDList ||')) And "inactive" = ''N'' And "IsRemoved" = ''N''' ||
		' And ((T0."Recontact" >= ''' || :DateStartPoint || ''' And T0."Recontact" <= ''' || :DateEndPoint || ''') Or' ||
	 	' (T0."endDate" >= ''' || :DateStartPoint || ''' And T0."endDate" <= ''' || :DateEndPoint || ''') Or' ||
	 	' (T0."Recontact" < ''' || :DateStartPoint || ''' And T0."endDate" > ''' || :DateEndPoint || ''' ))' ||
		' And "ClgCode" NOT IN (Select "SeriesNum" From OCLG Where "SeriesNum" IS NOT NULL)'  ||
		' And Not Exists (Select 1 From "CRSPCalMGA_Clg" T1 Where T0."ClgCode" = T1."ClgCode"' || 
		' And T0."Recontact" = T1."ReContact" And T0."endDate" = T1."EndDate")';
		
	exec(:tmpStr);

	-- Store Employee Absence and Education to the temporary table
	if :ShowEmployeeEdu = 1 then
		tmpStr := 'insert Into "CRSPCalMGA_EmployeesAbsEdu"("UserId", "EmployeeId", "StartDate", "EndDate", "Details")' ||	
			-- Absences
			' select T1."userId", T0."empID", T0."fromDate", T0."toDate", T0."reason" From HEM1 T0' || 
			' inner Join OHEM T1 On T1."empID" = T0."empID"' ||
			' where ((T0."fromDate" >= ''' || :DateStartPoint || ''' And T0."fromDate" <= ''' || :DateEndPoint || ''') Or' ||
	 		' (T0."toDate" >= ''' || :DateStartPoint || ''' And T0."toDate" <= ''' || :DateEndPoint || ''') Or' ||
	 		' (T0."fromDate" < ''' || :DateStartPoint || ''' And T0."toDate" > ''' || :DateEndPoint || ''' )) And' ||
			' (T1."userId" in ' || :UserIDList || ' Or T1."empID" in ' || EmployeeIDList || ') ' ||
			' Union All ' ||
			-- Education
			' select T1."userId", T0."empID", T0."fromDate", T0."toDate", T2."name"' || 
			' from HEM2 T0 ' || 
			' inner Join OHEM T1 On T1."empID" = T0."empID" ' ||
			' inner Join OHED T2 On T2."edType" = T0."type" ' ||
			' where ((T0."fromDate" >= ''' || :DateStartPoint || ''' And T0."fromDate" <= '''|| :DateEndPoint || ''') Or ' ||
	 		' (T0."toDate" >= ''' || :DateStartPoint || ''' And T0."toDate" <= ''' || :DateEndPoint || ''') Or ' ||
	 		' (T0."fromDate" < ''' || :DateStartPoint || ''' And T0."toDate" > ''' || :DateEndPoint || ''' )) And ' ||
			' (T1."userId" in ' || :UserIDList || ' Or T1."empID" in ' || :EmployeeIDList || ') ' ||
			' Order By T0."fromDate", T0."toDate" Desc';
			
		exec(:tmpStr);
	end if;
	
	-- Store Service calls to the temporary table
	if :ShowServiceCalls = 1 then
		tmpStr := 'insert into "CRSPCalMGA_ServiceCalls"("CallID", "StartDate", "EndDate", "BeginTime", "EndTime",' || 
			' "Details", "AssigneeUID", "QueueManagerUID", "TechUID", "TechEID", "Instance")' ||
			' select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee",' || 
			' T2."manager", T1."userId", T0."technician", ''A''' ||
			' From OSCL T0' ||
			' Left Outer Join OHEM T1 On T1."empID" = T0."technician" ' ||
			' Left Outer Join OQUE T2 On T2."queueID" = T0."Queue" ' ||
			' Where T0."DisplInCal" = ''Y'' And ' ||
			' ((T0."StartDate" >= ''' || :DateStartPoint || ''' And T0."StartDate" <= ''' || :DateEndPoint || ''') Or' ||
	 		' (T0."EndDate" >= ''' || :DateStartPoint || ''' And T0."EndDate" <= ''' || :DateEndPoint || ''') Or' ||
	 		' (T0."StartDate" < ''' || :DateStartPoint || ''' And T0."EndDate" > ''' || :DateEndPoint || ''' )) And' ||
    		' (T0."assignee" in (' || :UserIDList || ') Or T2."manager" in (' || :EmployeeIDList || '))' || 
			' Union All' ||
			' Select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee",' ||
			' T2."manager", T1."userId", T0."technician", ''T''' ||
			' From OSCL T0 ' ||
			' Left Outer Join OHEM T1 On T1."empID" = T0."technician"' ||
			' Left Outer Join OQUE T2 On T2."queueID" = T0."Queue"' ||
			' Where T0."DisplInCal" = ''Y'' And' ||
			' ((T0."StartDate" >= ''' || :DateStartPoint || ''' And T0."StartDate" <= ''' || :DateEndPoint || ''') Or' ||
	 		' (T0."EndDate" >= ''' || :DateStartPoint || ''' And T0."EndDate" <= ''' || :DateEndPoint || ''') Or' ||
	 		' (T0."StartDate" < ''' || :DateStartPoint || ''' And T0."EndDate" > ''' || :DateEndPoint || ''' )) And' ||
    		' (T1."userId" in (' || :UserIDList || ') Or T0."technician" in (' || :EmployeeIDList || '))' || 
			' order By T0."StartDate", T0."EndDate" Desc, T0."StartTime", T0."EndTime" Desc';
			
			exec(:tmpStr);
	end if;    
	
	-- Initialize #TmpRes table (all calendar item rows per day for whole month)
	DateFrom := :DateStartPoint;
	Iter := 0;
	
	while :DateFrom <= :DateEndPoint do
  		insert into "CRSPCalMGA_Res" Select ID, 0, 0, 0, '', :DateFrom, NULL, NULL, CAST(floor(:Iter/7) As Int) from "CRSPCalMGA_Seq";
  		Iter := :Iter + 1;
  		DateFrom := ADD_DAYS(:DateFrom, 1);
	end while;
	
	--- Insert Holidays
	open CursorHld (:DateStartPoint, :DateEndPoint);
	fetch CursorHld into StartDate, EndDate;
	
	while not CursorHld::NOTFOUND do
		
		update "CRSPCalMGA_Res" 
		set "ClgCode" = DAYS_BETWEEN(:EndDate, :StartDate) * 10 + DAYS_BETWEEN(:StartDate, :DateStartPoint) - 1,
			"StartDate" = :StartDate, "EndDate" = :EndDate
		where "CalendarDay" >= :StartDate and "CalendarDay" <= :EndDate and "ClgCode" = 0 and "AbsEduSeq" = 0 and "CallID" = 0 
			and "ID" = (select Min("LineID") from 
			(select "ID" As "LineID", COUNT("ID") as "CountIDs", 
  			DAYS_BETWEEN(case when :StartDate > :DateStartPoint then :StartDate else :DateStartPoint end,
  						case when :EndDate <= :DateEndPoint then :EndDate else :DateEndPoint end) + 1 as "ItemLen" 
  		from "CRSPCalMGA_Res" where "CalendarDay" >= :StartDate and "CalendarDay" <= :EndDate and "StartDate" is null and "EndDate" is null group by "ID") 
  		"VirtT" where "CountIDs" = "ItemLen");  	
		
		fetch CursorHld into StartDate, EndDate;
	end while;
	
	close CursorHld;

	-- Cursor for Employee absence and education	
	open CursorAbsEdu;
	fetch CursorAbsEdu into AbsEduSeq, StartDate, EndDate;
	
	while not CursorAbsEdu::NOTFOUND do
		
		update "CRSPCalMGA_Res" 
		set "AbsEduSeq" = :AbsEduSeq, "StartDate" = :StartDate, "EndDate" = :EndDate
		where "CalendarDay" >= :StartDate and "CalendarDay" <= :EndDate and "ClgCode" = 0 and "AbsEduSeq" = 0 and "CallID" = 0 
			and "ID" = (select Min("LineID") from 
			(select "ID" As "LineID", COUNT("ID") as "CountIDs", 
  			DAYS_BETWEEN(case when :StartDate > :DateStartPoint then :StartDate else :DateStartPoint end,
  						case when :EndDate <= :DateEndPoint then :EndDate else :DateEndPoint end) + 1 as "ItemLen" 
  		from "CRSPCalMGA_Res" where "CalendarDay" >= :StartDate and "CalendarDay" <= :EndDate and "StartDate" is null and "EndDate" is null group by "ID") 
  		"VirtT" where "CountIDs" = "ItemLen");
		
		fetch CursorAbsEdu into AbsEduSeq, StartDate, EndDate;
	end while;

	close CursorAbsEdu;

	-- Cursor for Service calls	
	open CursorScl;
	fetch CursorScl into CallID, SCInstance, StartDate, EndDate;
	
	while not CursorScl::NOTFOUND do
	
		update "CRSPCalMGA_Res" 
		set "CallID" = :CallID, "SCInstance" = :SCInstance, "StartDate" = :StartDate, "EndDate" = :EndDate
		where "CalendarDay" >= :StartDate and "CalendarDay" <= :EndDate and "ClgCode" = 0 and "AbsEduSeq" = 0 and "CallID" = 0 
			and "ID" = (select Min("LineID") from 
			(select "ID" As "LineID", COUNT("ID") as "CountIDs", 
  			DAYS_BETWEEN(case when :StartDate > :DateStartPoint then :StartDate else :DateStartPoint end,
  						case when :EndDate <= :DateEndPoint then :EndDate else :DateEndPoint end) + 1 as "ItemLen" 
  		from "CRSPCalMGA_Res" where "CalendarDay" >= :StartDate and "CalendarDay" <= :EndDate and "StartDate" is null and "EndDate" is null group by "ID") 
  		"VirtT" where "CountIDs" = "ItemLen");
		
		fetch CursorScl into CallID, SCInstance, StartDate, EndDate;
	end while;

	close CursorScl;

	-- Cursor for Activities	
	open CursorClg;
	fetch CursorClg into ClgCode, StartDate, EndDate;
	
	while not CursorClg::NOTFOUND do
	
		update "CRSPCalMGA_Res" 
		set "ClgCode" = :ClgCode, "StartDate" = :StartDate, "EndDate" = :EndDate
		where "CalendarDay" >= :StartDate and "CalendarDay" <= :EndDate and "ClgCode" = 0 and "AbsEduSeq" = 0 and "CallID" = 0 
			and "ID" = (select Min("LineID") from 
			(select "ID" As "LineID", COUNT("ID") as "CountIDs", 
  			DAYS_BETWEEN(case when :StartDate > :DateStartPoint then :StartDate else :DateStartPoint end,
  						case when :EndDate <= :DateEndPoint then :EndDate else :DateEndPoint end) + 1 as "ItemLen" 
  		from "CRSPCalMGA_Res" where "CalendarDay" >= :StartDate and "CalendarDay" <= :EndDate and "StartDate" is null and "EndDate" is null group by "ID") 
  		"VirtT" where "CountIDs" = "ItemLen");
		
		fetch CursorClg into ClgCode, StartDate, EndDate;
	end while;

	close CursorClg;
	
	Select MAX("ClgCode") - MIN("ClgCode"), MAX("AbsEduSeq") - MIN("AbsEduSeq"), MAX("CallID") - MIN(CallID)
		into IsClg, IsAbsEdu, IsScl from "CRSPCalMGA_Res";
		
	if :IsClg = 0 And :IsAbsEdu = 0 And :IsScl = 0 then
   		update "CRSPCalMGA_Res" Set "ID" = DAYOFMONTH("CalendarDay");
  		select * From "CRSPCalMGA_Res";
	
	else
  		select * From "CRSPCalMGA_Res" Where "ID" < :RdsLmt
  		union all
  		select :RdsLmt, MAX("ClgCode"), MAX("AbsEduSeq"), MAX("CallID"),
		"SCInstance", "CalendarDay", null, null, "WeekNum" From "CRSPCalMGA_Res" 
  		where "ID" >= :RdsLmt group by "WeekNum", "CalendarDay", "SCInstance";
	end if;
	
	delete from "CRSPCalMGA_Res";
	delete from "CRSPCalMGA_Seq";
	delete from "CRSPCalMGA_Clg";
	delete from "CRSPCalMGA_EmployeesAbsEdu";
	delete from "CRSPCalMGA_ServiceCalls";
end;











