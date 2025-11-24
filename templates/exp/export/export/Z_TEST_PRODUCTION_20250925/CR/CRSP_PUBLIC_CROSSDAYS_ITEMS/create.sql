-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

Create Procedure CRSP_Public_CrossDays_Items (
In ActivityTypes Integer,
In EmployeeIDsIn NCLOB,
In UserIDsIn NCLOB,
In FirstWeekDayIn Integer,
In GotoDate NVarChar(8),
In HldCodeIn NVarChar(20),
In RecurringInstances_MD NVarChar(1000),
In ShowEmployeeAbsEduIn TinyInt,
In ShowPersonalIn TinyInt,
In ShowServiceCallsIn TinyInt)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
ActTypes int;
RecurringInstances NVarChar(1000);
ShowPersonal tinyint;
ShowEmployeeAbsEdu tinyint;
ShowServiceCalls tinyint;
ShowPhoneCall tinyint; 
ShowMeeting tinyint; 
ShowOther tinyint; 
ShowTask tinyint;
Temp int;
Seq Int; 
StartDate DateTime;
EndDate DateTime;
DayOffset int;
WeekBegin DateTime;
WeekEnd DateTime;
NumLimit int;
i int;
MaxPos int;	
Pos int;
DayVar DateTime;
AbsEduSeq int;
CallID int;
SCInstance nvarchar(1);
SqlStr NCLOB;
curr int;
prev int;
Code_Sep int;
Instance NVarChar(50);
ClgCode int;
Recontact DateTime;
Day1 Datetime;
Day2 Datetime;
Day3 Datetime; 
Day4 Datetime; 
Day5 Datetime;
Day6 Datetime;
Day7 Datetime;
DisplayDate DateTime;
FirstWeekDay int;
HldCode NVarChar(40);
DateFirst tinyint;
idx int;
Rmrks nvarchar(50);
StrDate Datetime;
UserId int;
EmployeeId int;
Details nvarchar(20);
EmployeeIDs NCLOB;
UserIDs NCLOB;
tmpClgCode int;
tmpRecontact DateTime;
tmpEndDate DateTime;
Cursor HldCur For Select Seq, StrDate, EndDate From "CRSPPublicCrossDaysItemsHolidays";
Cursor AbsEduCur For Select AbsEduSeq, StartDate, EndDate from "CRSPPublicCrossDaysItemsEmployeesAbsEdu";
Cursor ServiceCallCur For Select CallID, Instance, StartDate, EndDate from "CRSPPublicCrossDaysItemsServiceCalls" Order By StartDate, EndDate Desc, BeginTime, EndTime Desc, Instance;
Cursor ActCur For Select ClgCode, Recontact, EndDate from "CRSPPublicCrossDaysItemsActivities";
CURSOR Hld1Cur (WeekBegin DateTime, WeekEnd DateTime, HldCode NVarChar(20)) FOR Select "StrDate", "EndDate", "Rmrks" From HLD1 Where "HldCode" = :HldCode AND ("StrDate" >= :WeekBegin And "StrDate" <= :WeekEnd Or
		"EndDate" >= :WeekBegin And "EndDate" <= :WeekEnd  OR "StrDate" < :WeekBegin And "EndDate" > :WeekEnd);
CURSOR HemCur (WeekBegin DateTime, WeekEnd DateTime) FOR 
       Select T1."userId", T0."empID", T0."fromDate", T0."toDate", T0."reason"
       From HEM1 T0 
       Inner Join OHEM T1 On T1."empID" = T0."empID" 
       Where ((T0."fromDate" >= :WeekBegin And T0."fromDate" <= :WeekEnd) Or (T0."toDate" >= :WeekBegin And T0."toDate" <= :WeekEnd) 
                 Or (T0."fromDate" < :WeekBegin And T0."toDate" > :WeekEnd )) And (T1."userId" in (select uID from "CRSPPublicCrossDaysItemsUserIds") Or T1."empID" in (select eId from "CRSPPublicCrossDaysItemsEmployeeIds")) 
      Union All Select T1."userId", T0."empID", T0."fromDate", T0."toDate", T2."name" From HEM2 T0 
      Inner Join OHEM T1 On T1."empID" = T0."empID" Inner Join OHED T2 On T2."edType" = T0."type" 	
      Where ((T0."fromDate" >= :WeekBegin And T0."fromDate" <= :WeekEnd) Or (T0."toDate" >= :WeekBegin And T0."toDate" <= :WeekEnd) 
      Or (T0."fromDate" < :WeekBegin And T0."toDate" > :WeekEnd )) And (T1."userId" in (select uID from "CRSPPublicCrossDaysItemsUserIds") Or T1."empID" in (select eId from "CRSPPublicCrossDaysItemsEmployeeIds")) 
      Order By T0."fromDate", T0."toDate" Desc;
begin

call _TmSp_ValidateSpParam(EmployeeIDsIn);
call _TmSp_ValidateSpParam(UserIDsIn);
call _TmSp_ValidateSpParam(GotoDate);
call _TmSp_ValidateSpParam(HldCodeIn);
call _TmSp_ValidateSpParam(RecurringInstances_MD);

delete from "CRSPPublicCrossDaysItemsHolidays";
delete from "CRSPPublicCrossDaysItemsEmployeesAbsEdu";
delete from "CRSPPublicCrossDaysItemsServiceCalls";
delete from "CRSPPublicCrossDaysItemsActivities";
delete from "CRSPPublicCrossDaysItemsInstances";
delete from "CRSPPublicCrossDaysItemsCalendarMD";
delete from "CRSPPublicCrossDaysItemsCalendarMDPositions";
delete from "CRSPPublicCrossDaysItemsEmployeeIds";
delete from "CRSPPublicCrossDaysItemsUserIds";
delete from CRSPPublicCrossDaysItems_Users;
delete from CRSPPublicCrossDaysItems_Employees;

EmployeeIDs := :EmployeeIDsIn;
UserIDs := :UserIDsIn;

SqlStr := 'Insert Into "CRSPPublicCrossDaysItemsUserIds"
Select UserId
From OUSR
Where USERID in ' || :UserIDs;
exec (:SqlStr);

SqlStr := 'Insert Into "CRSPPublicCrossDaysItemsEmployeeIds"
Select "empID"
From OHEM
Where "empID" in ' || :EmployeeIDs;
exec (:SqlStr);

DisplayDate := :GotoDate;
FirstWeekDay := :FirstWeekDayIn;  --Sunday = 1, Monday = 2, ..
HldCode := :HldCodeIn;
ActTypes := :ActivityTypes;
RecurringInstances := :RecurringInstances_MD;

If :ShowPersonalIn = 1 Then
    ShowPersonal := 1;
Else
    ShowPersonal := 0;
End If;
    
If :ShowEmployeeAbsEduIn = 1 Then
   ShowEmployeeAbsEdu := 1;
Else
    ShowEmployeeAbsEdu := 0;
End If;
    
If :ShowServiceCallsIn = 1 Then
    ShowServiceCalls := 1;
Else
    ShowServiceCalls := 0;
End If;
    
-------------------------------------------------------------------------------

ShowTask := MOD((:ActTypes / 1000), 10);
ShowPhoneCall := MOD((:ActTypes / 100), 10);
ShowMeeting := MOD((:ActTypes / 10), 10);
ShowOther := MOD (:ActTypes, 10);

DayOffset := 6 - MOD(:FirstWeekDay + 4 - WEEKDAY (:DisplayDate), 7);

WeekBegin := ADD_DAYS(:DisplayDate, (-:DayOffset));
WeekEnd := ADD_DAYS(:WeekBegin, 6);

	--The max number of activities that across days

NumLimit := 100;

Insert Into CRSPPublicCrossDaysItems_Users
			Select UserId, USER_CODE, U_NAME
			From OUSR
			Where USERID in (select uId from "CRSPPublicCrossDaysItemsUserIds");

Insert Into CRSPPublicCrossDaysItems_Employees
Select "empID", "firstName", "middleName", "lastName"
From OHEM
Where "empID" in (select eid from "CRSPPublicCrossDaysItemsEmployeeIds");

-- The main table that stores the position of the holidays and activities
	-- WeekDate : The day of the week 
	-- ClgCode : Activity primary key
	-- HldSeq : The sequence order that the holiday in the day
	-- AbsEduSeq : The sequence order of employee absence/education
	-- SCInstance : Service call instance; it may be 'A' - assignee/queue manager, 'T' - technician
	-- CallID : Service Call primary key
	-- Pos: The postion that the calendar item occupy


idx := 1;
Open Hld1Cur(:WeekBegin, :WeekEnd, :HldCode);

FETCH Hld1Cur Into StrDate, EndDate, Rmrks;
WHILE NOT Hld1Cur::NOTFOUND DO
	INSERT INTO "CRSPPublicCrossDaysItemsHolidays" VALUES (:idx, :StrDate, :EndDate, :Rmrks);
	idx := :idx +1;
	FETCH Hld1Cur Into StrDate, EndDate, Rmrks;
END WHILE;
CLOSE Hld1Cur;

open HldCur;
Fetch HldCur Into Seq, StartDate, EndDate;
While NOT HldCur::NOTFOUND DO
	Temp := NULL;
	Pos := NULL;
	DayVar := :StartDate;

	-- Get the position of the holiday in the start day
	Select Pos into Pos
	From "CRSPPublicCrossDaysItemsCalendarMDPositions" 
	Where WeekDate = :DayVar And HldSeq = :Seq;
		
	If :Pos = NULL Then
		i := 1;
		Select MAX(Pos) into MaxPos From "CRSPPublicCrossDaysItemsCalendarMDPositions" Where WeekDate = :DayVar;
		
		If :MaxPos <> NULL Then
			While :i <= :NumLimit And :i <= :MaxPos Do
				Select Pos into Temp From "CRSPPublicCrossDaysItemsCalendarMDPositions" Where WeekDate = :DayVar And Pos = :i;
				If :Temp = NULL Then
					break;
				End If;
				
				i := :i + 1;
			End While;
		End if;
		Pos := :i;	
	Else
		Pos := :Pos + 1;
	End IF;
	-- Insert the holiday for the duration 
	While :DayVar <= :EndDate And :DayVar <= :WeekEnd Do
		INSERT Into "CRSPPublicCrossDaysItemsCalendarMDPositions" (WeekDate, HldSeq, Pos)
		Values(:DayVar, :Seq, :Pos)	;	

		DayVar := ADD_DAYS(:DayVar, 1);
	End While;
	Fetch HldCur Into Seq, StartDate, EndDate;
End WHILE;

Close HldCur;

-- Store Employee Absence and Education to the temporary table
If :ShowEmployeeAbsEdu = 1 Then
	idx := 0;
	open HemCur (:WeekBegin, :WeekEnd);
	fetch HemCur into UserId, EmployeeId, StartDate, EndDate, Details;
	while NOT HemCur::NOTFOUND DO
	
		Insert Into "CRSPPublicCrossDaysItemsEmployeesAbsEdu" (AbsEduSeq, UserId, EmployeeId, StartDate, EndDate, Details) 
		VALUES (:idx, :UserId, :EmployeeId, :StartDate, :EndDate, :Details);
		
		fetch HemCur into UserId, EmployeeId, StartDate, EndDate, Details;
		idx := :idx +1;
	END WHILE;
	-- Absence
End If;

-- Rollover employee absence/education and calculate their right positions

Open AbsEduCur;
Fetch AbsEduCur Into AbsEduSeq, StartDate, EndDate;

While NOT AbsEduCur::NOTFOUND DO
	Select MAX(Pos) into MaxPos
	From "CRSPPublicCrossDaysItemsCalendarMDPositions" 
	Where :StartDate <= WeekDate And WeekDate <= :EndDate And WeekDate <= :WeekEnd;

	If :MaxPos = NULL then
		Pos := 1;
	else
		i := 1;
		While :i <= :NumLimit And :i <= :MaxPos Do
			Temp := NULL;
			Select COUNT(Pos) into Temp From "CRSPPublicCrossDaysItemsCalendarMDPositions" Where :StartDate <= WeekDate And WeekDate <= :EndDate And WeekDate <= :WeekEnd And Pos = :i;
			If :Temp = 0 Then
				BREAK;
			End If;
			i := :i + 1;
		End While;
		Pos := :i;
	End If;

	DayVar := :StartDate;
	While :DayVar <= :EndDate And :DayVar <= :WeekEnd Do
		Insert into "CRSPPublicCrossDaysItemsCalendarMDPositions" (WeekDate, AbsEduSeq, Pos)
		Values(:DayVar, :AbsEduSeq, :Pos);
	
		DayVar := ADD_DAYS(:DayVar, 1);
	End While;
		
	Fetch AbsEduCur Into AbsEduSeq, StartDate, EndDate;
End While;

Close AbsEduCur;

-- Store Service calls to the temporary table
If :ShowServiceCalls = 1 Then
	Insert Into "CRSPPublicCrossDaysItemsServiceCalls"(CallID, StartDate, EndDate, BeginTime, EndTime, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID, Instance)
	Select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee", T2."manager", T1."userId", T0."technician", 'A'
	From OSCL T0
	Left Outer Join OHEM T1 On T1."empID" = T0."technician"
	Left Outer Join OQUE T2 On T2."queueID" = T0."Queue"
	Where 
		T0."DisplInCal" = 'Y' And
		((T0."StartDate" >= :WeekBegin And T0."StartDate" <= :WeekEnd) Or
		(T0."EndDate" >= :WeekBegin And T0."EndDate" <= :WeekEnd) Or
		(T0."StartDate" < :WeekBegin And T0."EndDate" > :WeekEnd )) And
		T0."StartDate" <> T0."EndDate" And
		(T0."assignee" in (select uId from "CRSPPublicCrossDaysItemsUserIds") Or T2."manager" in (select uId from "CRSPPublicCrossDaysItemsUserIds"))
	Union All
	Select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee", T2."manager", T1."userId", T0."technician", 'T'
	From OSCL T0
	Left Outer Join OHEM T1 On T1."empID" = T0."technician"
	Left Outer Join OQUE T2 On T2."queueID" = T0."Queue"
	Where 
		T0."DisplInCal" = 'Y' And
		((T0."StartDate" >= :WeekBegin And T0."StartDate" <= :WeekEnd) Or
		(T0."EndDate" >= :WeekBegin And T0."EndDate" <= :WeekEnd) Or
		(T0."StartDate" < :WeekBegin And T0."EndDate" > :WeekEnd )) And
		T0."StartDate" <> T0."EndDate" And
		(T1."userId" in (select uId from "CRSPPublicCrossDaysItemsUserIds") Or T0."technician" in (select eId from "CRSPPublicCrossDaysItemsEmployeeIds")) 
	Order By T0."StartDate", T0."EndDate" Desc, T0."StartTime", T0."EndTime" Desc;
End If; 

-- Rollover Service Calls and calculate their right positions

Open ServiceCallCur;
Fetch ServiceCallCur Into CallID, SCInstance, StartDate, EndDate;

While NOT ServiceCallCur::NOTFOUND DO
	Select MAX(Pos) into MaxPos
	From "CRSPPublicCrossDaysItemsCalendarMDPositions" 
	Where :StartDate <= WeekDate And WeekDate <= :EndDate And WeekDate <= :WeekEnd;
	
	If :MaxPos = NULL Then
		Pos := 1;
	else
		i := 1;
		While :i <= :NumLimit And :i <= :MaxPos Do
			Temp := NULL;
			Select COUNT(Pos) into Temp From "CRSPPublicCrossDaysItemsCalendarMDPositions" Where :StartDate <= WeekDate And WeekDate <= :EndDate And WeekDate <= :WeekEnd And Pos = :i;
			If :Temp = 0 Then
				break;
			End If;
				
			i := :i + 1;
		End While;
		Pos := :i;
	End If;
	
	DayVar := :StartDate;
	While :DayVar <= :EndDate And :DayVar <= :WeekEnd Do
		Insert into "CRSPPublicCrossDaysItemsCalendarMDPositions"(WeekDate, CallID, SCInstance, Pos)
		Values(:DayVar, :CallID, :SCInstance, :Pos);
		
		DayVar := ADD_DAYS(:DayVar, 1);
	End While;
		
	Fetch ServiceCallCur Into CallID, SCInstance, StartDate, EndDate;
End WHILE;

Close ServiceCallCur;

-- Store the filtered activites to the temporary table

SqlStr := 'Insert Into "CRSPPublicCrossDaysItemsActivities"
Select T0."ClgCode", T0."Recontact", T0."endDate", T0."BeginTime", T0."ENDTime", T0."Action", T0."personal", T0."Details", T0."AttendUser", T0."AttendEmpl"
From OCLG T0 
Join CRSPPublicCrossDaysItems_Users T1 On T0."AttendUser" = T1.UserId
Where
(
	(
		(T0."Recontact" >= ''' || :WeekBegin || ''' AND T0."Recontact" <= ''' || :WeekEnd || ''')  OR
		(T0."endDate" >= ''' || :WeekBegin || ''' AND T0."endDate" <= ''' || :WeekEnd || ''') 
	) OR
	( T0."Recontact" < ''' || :WeekBegin || ''' AND  T0."endDate" > ''' || :WeekEnd  || ''')
) AND T0."endDate" <> T0."Recontact" And 
(T0."RecurPat" = ''N'' Or T0."SeriesNum" IS NOT NULL) And
(T0."personal" = ''N'' ';
If :ShowPersonal = 1 Then
	SqlStr := :SqlStr || 'OR T0."personal" = ''Y''';
End if;
SqlStr := :SqlStr || ') ';
SqlStr := :SqlStr || 'And (T0."Action" = ''_'' ';
If :ShowPhoneCall = 1 then
	SqlStr := :SqlStr || 'Or T0."Action" = ''C'' ';
End if;
If :ShowMeeting = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''M'' ';
End if;
If :ShowOther = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''N'' ';
End if;
If :ShowTask = 1 Then
	SqlStr := :SqlStr || 'Or T0."Action" = ''T'' ';
End if;
SqlStr := :SqlStr || ') ';
SqlStr := :SqlStr || 'Union All
Select T0."ClgCode", T0."Recontact", T0."endDate", T0."BeginTime", T0."ENDTime", T0."Action", T0."personal", T0."Details", T0."AttendUser", T0."AttendEmpl"
From OCLG T0 
Join CRSPPublicCrossDaysItems_Employees T2 On T0."AttendEmpl" = T2.EmployeeID
Where
(
	(
		(T0."Recontact" >= ''' || :WeekBegin || ''' AND T0."Recontact" <= ''' || :WeekEnd || ''')  OR
		(T0."endDate" >= ''' || :WeekBegin || '''AND T0."endDate" <= ''' || :WeekEnd || ''') 
	) OR
	( T0."Recontact" < ''' || :WeekBegin || ''' AND  T0."endDate" > ''' || :WeekEnd || ''')
) AND T0."endDate" <> T0."Recontact" And 
(T0."RecurPat" = ''N'' Or T0."SeriesNum" IS NOT NULL) And
(T0."personal" = ''N'' ';
If :ShowPersonal = 1 Then
	SqlStr := :SqlStr || 'OR T0."personal" = ''Y''';
End If;
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
End IF;
If :ShowTask = 1 Then 
	SqlStr := :SqlStr || 'Or T0."Action" = ''T'' ';
End IF;
SqlStr := :SqlStr || ') ';
SqlStr := :SqlStr || 'Order By T0."Recontact", T0."BeginTime"';

Exec (:SqlStr);


--insert into debug values (:SqlStr);
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

	insert into "CRSPPublicCrossDaysItemsInstances" (Select :tmpClgCode, 0, 0, :tmpRecontact, :tmpEndDate, 0, 0, ' ', ' ', ' ' FROM DUMMY);

	RecurringInstances := substr_after(RecurringInstances, ' ');
End While;

-- Get other information from OCLG
Update "CRSPPublicCrossDaysItemsInstances" T0
Set AttendUser = (Select "AttendUser" from OCLG T1 where T0.ClgCode = T1."ClgCode"),
AttendEmployee = (Select "AttendEmpl" from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
BeginTime = (Select "BeginTime" from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
EndTime = (Select "ENDTime" from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
Action = (Select "Action" from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
Personal = (Select Personal from OCLG T1 where T0.ClgCode = T1."ClgCode"), 
Details = (Select "Details" from OCLG T1 where T0.ClgCode = T1."ClgCode");

-- Delete the original recurring Activity, because the instances also include it
Delete FROM "CRSPPublicCrossDaysItemsActivities"
Where clgCode in (Select Distinct clgCode From "CRSPPublicCrossDaysItemsInstances");

-- Insert the instances filtered by users
Insert Into "CRSPPublicCrossDaysItemsActivities" 
Select clgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, AttendUser, AttendEmployee 
From "CRSPPublicCrossDaysItemsInstances" T0 
Join CRSPPublicCrossDaysItems_Users T1 On T0.AttendUser = T1.UserID
Where  (T0.Recontact >= :WeekBegin And T0.Recontact <= :WeekEnd Or
T0.EndDate >= :WeekBegin And T0.EndDate <= :WeekEnd
) OR T0.Recontact < :WeekBegin And T0.EndDate > :WeekEnd;


-- Insert the instances filtered by employees
Insert Into "CRSPPublicCrossDaysItemsActivities" 
Select clgCode, Recontact, EndDate, BeginTime, EndTime, Action, Personal, Details, AttendUser, AttendEmployee 
From "CRSPPublicCrossDaysItemsInstances" T0 
Join CRSPPublicCrossDaysItems_Employees T2 On T0.AttendEmployee = T2.EmployeeID
Where  (T0.Recontact >= :WeekBegin And T0.Recontact <= :WeekEnd Or
T0.EndDate >= :WeekBegin And T0.EndDate <= :WeekEnd
) OR T0.Recontact < :WeekBegin And T0.EndDate > :WeekEnd;

-- Rollover the activities and calculate their right positions

Open ActCur;
Fetch ActCur Into ClgCode, Recontact, EndDate;

While NOT ActCur::NOTFOUND Do
	Select MAX(Pos) into MaxPos
	From "CRSPPublicCrossDaysItemsCalendarMDPositions" 
	Where :Recontact <= WeekDate And WeekDate <= :EndDate And WeekDate <= :WeekEnd;
	
	If :MaxPos = NULL Then
		Pos := 1;
	else
		i := 1;
		While :i <= :NumLimit And :i <= :MaxPos Do
			Temp := NULL;
			Select COUNT(Pos) into Temp From "CRSPPublicCrossDaysItemsCalendarMDPositions" Where :Recontact <= WeekDate And WeekDate <= :EndDate And WeekDate <= :WeekEnd And Pos = :i;
			If :Temp = 0 Then
				break;
			End If;
			i := :i + 1;
		End While; 
		Pos := :i;
	End If;

	DayVar := :Recontact;
	While :DayVar <= :EndDate And :DayVar <= :WeekEnd Do
		Insert into "CRSPPublicCrossDaysItemsCalendarMDPositions" (WeekDate, ClgCode, Pos)
		Values(:DayVar, :ClgCode, :Pos);
		
		DayVar := ADD_DAYS(:DayVar, 1);
	End While;
		
	Fetch ActCur Into ClgCode, Recontact, EndDate;
End While;
Close ActCur;

Day1 := :WeekBegin;
Day2 := ADD_DAYS(:Day1, 1);
Day3 := ADD_DAYS(:Day2, 1);
Day4 := ADD_DAYS(:Day3, 1);
Day5 := ADD_DAYS(:Day4, 1);
Day6 := ADD_DAYS(:Day5, 1);
Day7 := ADD_DAYS(:Day6, 1);

i := 1;
While :i <= 10 DO
	Insert into "CRSPPublicCrossDaysItemsCalendarMD" (Line) Values(:i);
	i := :i + 1;
End WHILE;

Update "CRSPPublicCrossDaysItemsCalendarMD" T0 
Set (Day1, Pos1, HldSeq1, AbsEduSeq1, CallID1, SCInstance1, ClgCode1) = 
(SELECT :Day1, Pos, HldSeq, AbsEduSeq, CallID, SCInstance, ClgCode FROM "CRSPPublicCrossDaysItemsCalendarMDPositions" T1 where T0.Line = T1.Pos and T1.WeekDate = :Day1);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Day2, Pos2, HldSeq2, AbsEduSeq2, CallID2, SCInstance2, ClgCode2)  = 
(select :Day2, Pos, HldSeq, AbsEduSeq, CallID, SCInstance, ClgCode from "CRSPPublicCrossDaysItemsCalendarMDPositions" T1 where T0.Line = T1.Pos and T1.WeekDate = :Day2);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Day3, Pos3, HldSeq3, AbsEduSeq3, CallID3, SCInstance3, ClgCode3)  = 
(select :Day3, Pos, HldSeq, AbsEduSeq, CallID, SCInstance, ClgCode from "CRSPPublicCrossDaysItemsCalendarMDPositions" T1 where T0.Line = T1.Pos and T1.WeekDate = :Day3);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Day4, Pos4, HldSeq4, AbsEduSeq4, CallID4, SCInstance4, ClgCode4)  = 
(select :Day4, Pos, HldSeq, AbsEduSeq, CallID, SCInstance, ClgCode from "CRSPPublicCrossDaysItemsCalendarMDPositions" T1 where T0.Line = T1.Pos and T1.WeekDate = :Day4);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Day5, Pos5, HldSeq5, AbsEduSeq5, CallID5, SCInstance5, ClgCode5)  = 
(select :Day5, Pos, HldSeq, AbsEduSeq, CallID, SCInstance, ClgCode from "CRSPPublicCrossDaysItemsCalendarMDPositions" T1 where T0.Line = T1.Pos and T1.WeekDate = :Day5);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Day6, Pos6, HldSeq6, AbsEduSeq6, CallID6, SCInstance6, ClgCode6)  = 
(select :Day6, Pos, HldSeq, AbsEduSeq, CallID, SCInstance, ClgCode from "CRSPPublicCrossDaysItemsCalendarMDPositions" T1 where T0.Line = T1.Pos and T1.WeekDate = :Day6);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Day7, Pos7, HldSeq7, AbsEduSeq7, CallID7, SCInstance7, ClgCode7)  = 
(select :Day7, Pos, HldSeq, AbsEduSeq, CallID, SCInstance, ClgCode from "CRSPPublicCrossDaysItemsCalendarMDPositions" T1 where T0.Line = T1.Pos and T1.WeekDate = :Day7);

-----------------------------------------------------------------------

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (BeginDate1, EndDate1, Remark1) = 
(select StrDate, EndDate, Rmrks from "CRSPPublicCrossDaysItemsHolidays" T1 where T0.HldSeq1 = T1.Seq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (BeginDate2, EndDate2, Remark2) = 
(select StrDate, EndDate, Rmrks from "CRSPPublicCrossDaysItemsHolidays" T1 where T0.HldSeq2 = T1.Seq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (BeginDate3, EndDate3, Remark3) = 
(select StrDate, EndDate, Rmrks from "CRSPPublicCrossDaysItemsHolidays" T1 where T0.HldSeq3 = T1.Seq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (BeginDate4, EndDate4, Remark4) = 
(select StrDate, EndDate, Rmrks from "CRSPPublicCrossDaysItemsHolidays" T1 where T0.HldSeq4 = T1.Seq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (BeginDate5, EndDate5, Remark5) = 
(select StrDate, EndDate, Rmrks from "CRSPPublicCrossDaysItemsHolidays" T1 where T0.HldSeq5 = T1.Seq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (BeginDate6, EndDate6, Remark6) = 
(select StrDate, EndDate, Rmrks from "CRSPPublicCrossDaysItemsHolidays" T1 where T0.HldSeq6 = T1.Seq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (BeginDate7, EndDate7, Remark7) = 
(select StrDate, EndDate, Rmrks from "CRSPPublicCrossDaysItemsHolidays" T1 where T0.HldSeq7 = T1.Seq);

-----------------------------------------------------------------------

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (User1, Employee1, BeginDate1, EndDate1, Remark1)
 = (select UserId, EmployeeId, StartDate, EndDate, Details from "CRSPPublicCrossDaysItemsEmployeesAbsEdu" T1 where T0.AbsEduSeq1 = T1.AbsEduSeq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (User2, Employee2, BeginDate2, EndDate2, Remark2)
 = (select UserId, EmployeeId, StartDate, EndDate, Details from "CRSPPublicCrossDaysItemsEmployeesAbsEdu" T1 where T0.AbsEduSeq2 = T1.AbsEduSeq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (User3, Employee3, BeginDate3, EndDate3, Remark3)
 = (select UserId, EmployeeId, StartDate, EndDate, Details from "CRSPPublicCrossDaysItemsEmployeesAbsEdu" T1 where T0.AbsEduSeq3 = T1.AbsEduSeq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (User4, Employee4, BeginDate4, EndDate4, Remark4)
 = (select UserId, EmployeeId, StartDate, EndDate, Details from "CRSPPublicCrossDaysItemsEmployeesAbsEdu" T1 where T0.AbsEduSeq4 = T1.AbsEduSeq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (User5, Employee5, BeginDate5, EndDate5, Remark5)
 = (select UserId, EmployeeId, StartDate, EndDate, Details from "CRSPPublicCrossDaysItemsEmployeesAbsEdu" T1 where T0.AbsEduSeq5 = T1.AbsEduSeq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (User6, Employee6, BeginDate6, EndDate6, Remark6)
 = (select UserId, EmployeeId, StartDate, EndDate, Details from "CRSPPublicCrossDaysItemsEmployeesAbsEdu" T1 where T0.AbsEduSeq6 = T1.AbsEduSeq);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (User7, Employee7, BeginDate7, EndDate7, Remark7)
 = (select UserId, EmployeeId, StartDate, EndDate, Details from "CRSPPublicCrossDaysItemsEmployeesAbsEdu" T1 where T0.AbsEduSeq7 = T1.AbsEduSeq);
---------------------------------------------------------------------

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (AssigneeUID1, QueueManagerUID1, TechUID1, TechEID1, BeginDate1, EndDate1, BeginTime1, EndTime1, Remark1)
 = (select AssigneeUID, QueueManagerUID, TechUID, TechEID, StartDate, EndDate, BeginTime, EndTime, Details from "CRSPPublicCrossDaysItemsServiceCalls" T1 where T0.CallID1 = T1.CallID And T0.SCInstance1 = T1.Instance); 

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (AssigneeUID2, QueueManagerUID2, TechUID2, TechEID2, BeginDate2, EndDate2, BeginTime2, EndTime2, Remark2)
 = (select AssigneeUID, QueueManagerUID, TechUID, TechEID, StartDate, EndDate, BeginTime, EndTime, Details from "CRSPPublicCrossDaysItemsServiceCalls" T1 where T0.CallID2 = T1.CallID And T0.SCInstance2 = T1.Instance); 

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (AssigneeUID3, QueueManagerUID3, TechUID3, TechEID3, BeginDate3, EndDate3, BeginTime3, EndTime3, Remark3)
 = (select AssigneeUID, QueueManagerUID, TechUID, TechEID, StartDate, EndDate, BeginTime, EndTime, Details from "CRSPPublicCrossDaysItemsServiceCalls" T1 where T0.CallID3 = T1.CallID And T0.SCInstance3 = T1.Instance); 

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (AssigneeUID4, QueueManagerUID4, TechUID4, TechEID4, BeginDate4, EndDate4, BeginTime4, EndTime4, Remark4)
 = (select AssigneeUID, QueueManagerUID, TechUID, TechEID, StartDate, EndDate, BeginTime, EndTime, Details from "CRSPPublicCrossDaysItemsServiceCalls" T1 where T0.CallID4 = T1.CallID And T0.SCInstance4 = T1.Instance); 

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (AssigneeUID5, QueueManagerUID5, TechUID5, TechEID5, BeginDate5, EndDate5, BeginTime5, EndTime5, Remark5)
 = (select AssigneeUID, QueueManagerUID, TechUID, TechEID, StartDate, EndDate, BeginTime, EndTime, Details from "CRSPPublicCrossDaysItemsServiceCalls" T1 where T0.CallID5 = T1.CallID And T0.SCInstance5 = T1.Instance); 

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (AssigneeUID6, QueueManagerUID6, TechUID6, TechEID6, BeginDate6, EndDate6, BeginTime6, EndTime6, Remark6)
 = (select AssigneeUID, QueueManagerUID, TechUID, TechEID, StartDate, EndDate, BeginTime, EndTime, Details from "CRSPPublicCrossDaysItemsServiceCalls" T1 where T0.CallID6 = T1.CallID And T0.SCInstance6 = T1.Instance); 

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (AssigneeUID7, QueueManagerUID7, TechUID7, TechEID7, BeginDate7, EndDate7, BeginTime7, EndTime7, Remark7)
 = (select AssigneeUID, QueueManagerUID, TechUID, TechEID, StartDate, EndDate, BeginTime, EndTime, Details from "CRSPPublicCrossDaysItemsServiceCalls" T1 where T0.CallID7 = T1.CallID And T0.SCInstance7 = T1.Instance); 

----------------------------------------------------------------------
Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Action1, Personal1, User1, Employee1, BeginDate1, EndDate1, BeginTime1, EndTime1, Remark1)
= (select Min(Action), MIN(Personal), MIN(UserId), MIN(EmployeeId), MIN(Recontact), MIN(EndDate), MIN(BeginTime), MIN(ENDTime), MIN(Details)  from "CRSPPublicCrossDaysItemsActivities" T1 where T0.ClgCode1 = T1.ClgCode);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Action2, Personal2, User2, Employee2, BeginDate2, EndDate2, BeginTime2, EndTime2, Remark2)
= (select Min(Action), MIN(Personal), MIN(UserId), MIN(EmployeeId), MIN(Recontact), MIN(EndDate), MIN(BeginTime), MIN(ENDTime), MIN(Details)  from "CRSPPublicCrossDaysItemsActivities" T1 where T0.ClgCode2 = T1.ClgCode);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Action3, Personal3, User3, Employee3, BeginDate3, EndDate3, BeginTime3, EndTime3, Remark3)
= (select Min(Action), MIN(Personal), MIN(UserId), MIN(EmployeeId), MIN(Recontact), MIN(EndDate), MIN(BeginTime), MIN(ENDTime), MIN(Details)  from "CRSPPublicCrossDaysItemsActivities" T1 where T0.ClgCode3 = T1.ClgCode);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Action4, Personal4, User4, Employee4, BeginDate4, EndDate4, BeginTime4, EndTime4, Remark4)
= (select Min(Action), MIN(Personal), MIN(UserId), MIN(EmployeeId), MIN(Recontact), MIN(EndDate), MIN(BeginTime), MIN(ENDTime), MIN(Details)  from "CRSPPublicCrossDaysItemsActivities" T1 where T0.ClgCode4 = T1.ClgCode);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Action5, Personal5, User5, Employee5, BeginDate5, EndDate5, BeginTime5, EndTime5, Remark5)
= (select Min(Action), MIN(Personal), MIN(UserId), MIN(EmployeeId), MIN(Recontact), MIN(EndDate), MIN(BeginTime), MIN(ENDTime), MIN(Details)  from "CRSPPublicCrossDaysItemsActivities" T1 where T0.ClgCode5 = T1.ClgCode);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Action6, Personal6, User6, Employee6, BeginDate6, EndDate6, BeginTime6, EndTime6, Remark6)
= (select Min(Action), MIN(Personal), MIN(UserId), MIN(EmployeeId), MIN(Recontact), MIN(EndDate), MIN(BeginTime), MIN(ENDTime), MIN(Details)  from "CRSPPublicCrossDaysItemsActivities" T1 where T0.ClgCode6 = T1.ClgCode);

Update "CRSPPublicCrossDaysItemsCalendarMD" T0
Set (Action7, Personal7, User7, Employee7, BeginDate7, EndDate7, BeginTime7, EndTime7, Remark7)
= (select Min(Action), MIN(Personal), MIN(UserId), MIN(EmployeeId), MIN(Recontact), MIN(EndDate), MIN(BeginTime), MIN(ENDTime), MIN(Details)  from "CRSPPublicCrossDaysItemsActivities" T1 where T0.ClgCode7 = T1.ClgCode);

Select * from "CRSPPublicCrossDaysItemsCalendarMD";

delete from "CRSPPublicCrossDaysItemsHolidays";
delete from "CRSPPublicCrossDaysItemsEmployeesAbsEdu";
delete from "CRSPPublicCrossDaysItemsServiceCalls";
delete from "CRSPPublicCrossDaysItemsActivities";
delete from "CRSPPublicCrossDaysItemsInstances";
delete from "CRSPPublicCrossDaysItemsCalendarMD";
delete from "CRSPPublicCrossDaysItemsCalendarMDPositions";
delete from "CRSPPublicCrossDaysItemsEmployeeIds";
delete from "CRSPPublicCrossDaysItemsUserIds";
delete from CRSPPublicCrossDaysItems_Users;
delete from CRSPPublicCrossDaysItems_Employees;

End;











