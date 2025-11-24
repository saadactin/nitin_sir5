-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

Create Procedure CRSP_Calendar_Daily_Group_Across_Days_Items(
In DisplayDateIn NVarChar(8),
In HolidaysCodeIn NVarChar(20),
In EmployeeIDIn Integer,
In UserIDIn Integer,
In MinutesPerRowIn SmallInt,
In RecurringInstancesIn NVarChar(1000),
In ShowEmployeeAbsEduIn TinyInt,
In ShowPhoneCallIn TinyInt,
In ShowMeetingIn TinyInt,
In ShowOtherIn TinyInt,
In ShowTaskIn TinyInt,
In ShowPersonalIn TinyInt,
In ShowServiceCallsIn TinyInt)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
DisplayDate DateTime;
HolidaysCode NVarChar(20);
ShowPhoneCall tinyint;
ShowMeeting tinyint;
ShowOther tinyint;
ShowTask tinyint;
ShowPersonal tinyint;
ShowEmployeeAbsEdu tinyint;
ShowServiceCalls tinyint;
GapPerRow int;
RecurringInstances NVarChar(1000);
time int;
span int;
entry int;
curr int;
prev int;
seq int;
Code_Sep int;
Instance NVarChar(50);
sqlStr NCLOB;
UserId Integer;
EmployeeID Integer;
tmpClgCode int;
tmpRecontact DateTime;
tmpEndDate DateTime;
tmpUserId int;
tempEmpId int;
fromDate DateTime;
toDate DateTime;
reason NVarChar(120);

cursor hem_c (DisplayDate Datetime, UserId int, EmployeeID int) for Select T1."userId", T0."empID", T0."fromDate", T0."toDate", T0."reason"
	From HEM1 T0 
	Inner Join OHEM T1 On T1."empID" = T0."empID"
	Where 
		((T0."fromDate" >= :DisplayDate And T0."fromDate" <= :DisplayDate) Or
		(T0."toDate" >= :DisplayDate And T0."toDate" <= :DisplayDate) Or
		(T0."fromDate" < :DisplayDate And T0."toDate" > :DisplayDate)) And
		(T1."userId" = :UserId Or T1."empID" = :EmployeeID)
	Union All
	-- Education
	Select T1."userId", T0."empID", T0."fromDate", T0."toDate", T2."name"
	From HEM2 T0 
	Inner Join OHEM T1 On T1."empID" = T0."empID"
	Inner Join OHED T2 On T2."edType" = T0."type"
	Where 
		((T0."fromDate" >= :DisplayDate And T0."fromDate" <= :DisplayDate) Or
		(T0."toDate" >= :DisplayDate And T0."toDate" <= :DisplayDate) Or
		(T0."fromDate" < :DisplayDate And T0."toDate" > :DisplayDate)) And
		(T1."userId" =:UserID Or T1."empID" =:EmployeeID)
	Order By T0."fromDate", T0."toDate" Desc;
Begin
---------------------------------------------------------------------

call _TmSp_ValidateSpParam(DisplayDateIn);
call _TmSp_ValidateSpParam(HolidaysCodeIn);
call _TmSp_ValidateSpParam(RecurringInstancesIn);

HolidaysCode := :HolidaysCodeIn;
DisplayDate := :DisplayDateIn;
ShowPhoneCall := :ShowPhoneCallIn;
ShowMeeting := :ShowMeetingIn;
ShowOther := :ShowOtherIn;
ShowTask := :ShowTaskIn;
ShowPersonal := :ShowPersonalIn;
ShowEmployeeAbsEdu := :ShowEmployeeAbsEduIn;
ShowServiceCalls := :ShowServiceCallsIn;
GapPerRow := :MinutesPerRowIn;
RecurringInstances := :RecurringInstancesIn;
UserId := :UserIDIn;
EmployeeID := :EmployeeIDIn;
---------------------------------------------------------------------

delete from "CRSPCalendarDailyGroupAcrossDaysItemsInstances"; 
delete from "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD";


entry := 0;
span := :GapPerRow;
time := 0;

-- Insert the Holidays
If :HolidaysCode <> '' Then
	Insert into "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD" (HldCode, Recontact, EndDate, Details)
	Select "HldCode", "StrDate",  "EndDate", "Rmrks" From HLD1 where "HldCode" = :HolidaysCode
	And 
	( 
		("StrDate" = :DisplayDate Or "EndDate" = :DisplayDate)
		Or
		("StrDate" < :DisplayDate And "EndDate" > :DisplayDate)
	);
End If;


seq := 1;
-- Store Employee Absence and Education to the temporary table
If :ShowEmployeeAbsEdu = 1 Then

	open hem_c (:DisplayDate, :UserId, :EmployeeID);
	
	fetch hem_c into tmpUserId, tempEmpId, fromDate, toDate, reason;
	
	while not hem_c::NOTFOUND do
		insert Into "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD" (AbsEduSeq, UserId, EmployeeId, Recontact, EndDate, Details)
		values (:seq, :tmpUserId, :tempEmpId, :fromDate, :toDate, :reason);
	
		seq := :seq +1;
		fetch hem_c into tmpUserId, tempEmpId, fromDate, toDate, reason;
	end while;
	
	close hem_c;
	
	-- Insert Employee Absence and Education to "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD"
End If;

-- Store Service calls to the temporary table
If :ShowServiceCalls = 1 Then
		
	Insert Into "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD"(CallID, Recontact, EndDate, BeginTime, EndTime, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID, SCInstance)
	
	
	Select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee", T2."manager", T1."userId", T0."technician", 'A'
	From OSCL T0
	Left Outer Join OHEM T1 On T1."empID" = T0."technician"
	Left Outer Join OQUE T2 On T2."queueID" = T0."Queue"
	Where 
		T0."DisplInCal" = 'Y' And
		((T0."StartDate" >= :DisplayDate And T0."StartDate" <= :DisplayDate) Or
		(T0."EndDate" >= :DisplayDate And T0."EndDate" <= :DisplayDate) Or
		(T0."StartDate" < :DisplayDate And T0."EndDate" > :DisplayDate)) And
		T0."StartDate" <> T0."EndDate" And
		(T0."assignee" =:UserID Or T2."manager" =:EmployeeID) 
	Union All
	Select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee", T2."manager", T1."userId", T0."technician", 'T'
	From OSCL T0
	Left Outer Join OHEM T1 On T1."empID" = T0."technician"
	Left Outer Join OQUE T2 On T2."queueID" = T0."Queue"
	Where 
		T0."DisplInCal" = 'Y' And
		((T0."StartDate" >= :DisplayDate And T0."StartDate" <= :DisplayDate) Or
		(T0."EndDate" >= :DisplayDate And T0."EndDate" <= :DisplayDate) Or
		(T0."StartDate" < :DisplayDate And T0."EndDate" > :DisplayDate)) And
		T0."StartDate" <> T0."EndDate" And
		(T1."userId" =:UserID Or T0."technician" =:EmployeeID) 
	Order By T0."StartDate", T0."EndDate" Desc;
	
End IF;

-- Store the filtered activites to the temporary table
SqlStr := 'Insert Into "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD"(ClgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, UserId, EmployeeId)
	Select T0."ClgCode", T0."Recontact", T0."endDate", T0."BeginTime", T0."ENDTime", T0."Action", T0."personal", T0."Details", T0."AttendUser", T0."AttendEmpl"
	From OCLG T0 
	Where T0."AttendUser" =' || :UserId ||' And
	(
		(
			(T0."Recontact" >= ''' ||  :DisplayDate || ''' AND T0."Recontact" <= ''' || :DisplayDate || ''')  OR
			(T0."endDate" >= ''' || :DisplayDate || ''' AND T0."endDate" <= ''' || :DisplayDate || ''') 
		) OR
		( T0."Recontact" < ''' || :DisplayDate || ''' AND  T0."endDate" > ''' || :DisplayDate || ''' )
	) AND T0."endDate" <> T0."Recontact" And 
	(T0."RecurPat" = ''N'' Or T0."SeriesNum" IS NOT NULL) And (T0."personal" = ''N'' )';
	
If :ShowPersonal = 1 Then
	SqlStr := :SqlStr || 'OR (T0."personal" = ''Y'')';
End If;

SqlStr := :SqlStr || 'And (T0."Action" = ''_'' ';

If :ShowPhoneCall = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''C'' ';
End If;
If :ShowMeeting = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''M'' ';
End If;
If :ShowOther = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''N'' ';
End If;
If :ShowTask = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''T'' ';
End If;

SqlStr := :SqlStr || ') ';
SqlStr := :SqlStr || 'Union All
	Select T0."ClgCode", T0."Recontact", T0."endDate", T0."BeginTime", T0."ENDTime", T0."Action", T0."personal", T0."Details", T0."AttendUser", T0."AttendEmpl"
	From OCLG T0 
	Where T0."AttendEmpl" = ' || :EmployeeID || ' And
	(
		(
			(T0."Recontact" >= ''' || :DisplayDate || ''' AND T0."Recontact" <= ''' || :DisplayDate || ''')  OR
			(T0."endDate" >= ''' || :DisplayDate || ''' AND T0."endDate" <= ''' || :DisplayDate || ''') 
		) OR
		( T0."Recontact" < ''' || :DisplayDate || ''' AND  T0."endDate" > ''' || :DisplayDate || ''' )
	) AND T0."endDate" <> T0."Recontact" And 
	(T0."RecurPat" = ''N'' Or T0."SeriesNum" IS NOT NULL) And
	(T0."personal" = ''N'' ';
	
If :ShowPersonal = 1 Then
	SqlStr := :SqlStr || 'OR T0."personal" = ''Y''';
End IF;
SqlStr := :SqlStr || ') ';

SqlStr := :SqlStr || 'And (T0."Action" = ''_'' ';

If :ShowPhoneCall = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''C'' ';
End If;
If :ShowMeeting = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''M'' ';
End If;
If :ShowOther = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''N'' ';
End If;
If :ShowTask = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''T'' ';
End IF;
SqlStr := :SqlStr || ') ';
SqlStr := :SqlStr || 'Order By T0."Recontact", T0."BeginTime"';
exec (:SqlStr);

--insert into debug values (SqlStr);

-- Insert the recurring instances

curr := 1;
prev := 1;

	-- Store the recurring instances in the temporary table

While length(:RecurringInstances) > 0 DO
	curr := locate(:RecurringInstances, ' ');
	if :curr>0 then
			Instance := SUBSTRING(:RecurringInstances, 0, :curr);
	else
			Instance := :RecurringInstances;
	end if;
	tmpClgCode := substr(:Instance, 0, locate(:Instance, ',')-1);
	Instance := substr_after(:Instance, ',');
	tmpRecontact := substr(:Instance, 0, locate(:Instance, ',')-1);
	tmpEndDate := rtrim(substr_after(:Instance, ','));

	insert into "CRSPCalendarDailyGroupAcrossDaysItemsInstances" (Select :tmpClgCode, 0, 0, :tmpRecontact, :tmpEndDate, 0, 0, ' ', ' ', ' ' FROM DUMMY);

	RecurringInstances := substr_after(RecurringInstances, ' ');
End While;

	-- Get other information from OCLG
Update "CRSPCalendarDailyGroupAcrossDaysItemsInstances" T0
Set AttendUser = (SELECT "AttendUser" FROM OCLG T1 Where T0.ClgCode = T1."ClgCode"), 
AttendEmployee = (SELECT "AttendEmpl" FROM OCLG T1 Where T0.ClgCode = T1."ClgCode"), 
BeginTime = (SELECT "BeginTime" FROM OCLG T1 Where T0.ClgCode = T1."ClgCode"), 
EndTime = (SELECT "ENDTime" FROM OCLG T1 Where T0.ClgCode = T1."ClgCode"), 
Action = (SELECT "Action" FROM OCLG T1 Where T0.ClgCode = T1."ClgCode"), 
Personal = (SELECT "personal" FROM OCLG T1 Where T0.ClgCode = T1."ClgCode"), 
Details = (SELECT "Details" FROM OCLG T1 Where T0.ClgCode = T1."ClgCode");

	-- Delete the original recurring Activity, because the instances also include it
Delete FROM "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD"
Where clgCode in (Select Distinct clgCode From "CRSPCalendarDailyGroupAcrossDaysItemsInstances");

	-- Insert the instances filtered by users
Insert Into "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD"(ClgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, UserId, EmployeeId)
Select clgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, AttendUser, AttendEmployee 
From "CRSPCalendarDailyGroupAcrossDaysItemsInstances" T0 
Where T0.AttendUser = :UserID And T0.Recontact = :DisplayDate Or T0.endDate = :DisplayDate Or 
T0.Recontact < :DisplayDate And T0.EndDate > :Displaydate;

	-- Insert the instances filtered by employees
Insert Into "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD"(ClgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, UserId, EmployeeId)
Select clgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, AttendUser, AttendEmployee 
From "CRSPCalendarDailyGroupAcrossDaysItemsInstances" T0 
Where T0.AttendEmployee = :EmployeeID And T0.Recontact = :DisplayDate Or T0.endDate = :DisplayDate Or 
T0.Recontact < :DisplayDate And T0.EndDate > :Displaydate;

Select * From "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD"; -- Order By Recontact, BeginTime

delete from "CRSPCalendarDailyGroupAcrossDaysItemsInstances";
delete from "CRSPCalendarDailyGroupAcrossDaysItemsCalendarMD";

End;











