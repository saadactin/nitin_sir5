-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

create procedure CRSP_Calendar_Monthly_ActivityInLine (
	In Activity1 integer,
	In Activity2 integer,
	In Activity3 integer,
	In Activity4 integer,
	In Activity5 integer,
	In Activity6 integer,
	In Activity7 integer,
	In EmployeeAbs1 integer, 
	In EmployeeAbs2 integer, 
	In EmployeeAbs3 integer, 
	In EmployeeAbs4 integer, 
	In EmployeeAbs5 integer,
	In EmployeeAbs6 integer,
	In EmployeeAbs7 integer,
	In EmployeeIDList NClob,
	In FirstDayIn tinyint,
	In FirstWeekDay tinyint,
	In GotoDate NVarchar(30),
	In HldCode Nvarchar(40),
	In ServiceCall1 integer,
	In ServiceCall2 integer,
	In ServiceCall3 integer,
	In ServiceCall4 integer,
	In ServiceCall5 integer,
	In ServiceCall6 integer,
	In ServiceCall7 integer,
	In ServiceCallInstance1 NVarchar(1),
	In ServiceCallInstance2 NVarchar(1),
	In ServiceCallInstance3 NVarchar(1),
	In ServiceCallInstance4 NVarchar(1),
	In ServiceCallInstance5 NVarchar(1),
	In ServiceCallInstance6 NVarchar(1),
	In ServiceCallInstance7 NVarchar(1),
	In ShowEmployeeAbsEntry tinyint,
	In ShowServiceCall tinyint,
	In UserDList NClob)
  
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
as

	Diff int;
	DateStartPoint datetime;
	DateEndPoint datetime;
	FirstDay tinyint;
	ShowEmployeeAbsEdu tinyint;
	ShowServiceCalls tinyint;
	UserIDs NCLOB; 
	UserId int;
	EmployeeIDs NCLOB; 
	EmployeeId int;
	sqlStr NCLOB;
	
begin
	
	call _TmSp_ValidateSpParam(EmployeeIDList);
	call _TmSp_ValidateSpParam(UserDList);
	call _TmSp_ValidateSpParam(GotoDate);
	call _TmSp_ValidateSpParam(HldCode);
	call _TmSp_ValidateSpParam(ServiceCallInstance1);
	call _TmSp_ValidateSpParam(ServiceCallInstance2);
	call _TmSp_ValidateSpParam(ServiceCallInstance3);
	call _TmSp_ValidateSpParam(ServiceCallInstance4);
	call _TmSp_ValidateSpParam(ServiceCallInstance5);
	call _TmSp_ValidateSpParam(ServiceCallInstance6);
	call _TmSp_ValidateSpParam(ServiceCallInstance7);
	
	delete from "CRSPCalendarMonthlyActivityInLineCalendar";
	delete from "CRSPCalendarMonthlyActivityInLineEmployeeAbsEdu";
	delete from "CRSPCalendarMonthlyActivityInLineServiceCalls";
	delete from CRSPCalendarMonthlyActivityInLine_users;
	delete from CRSPCalendarMonthlyActivityInLine_employees;
	
	UserIDs := UserDList;
	EmployeeIDs := EmployeeIDList;
	
	if (:ShowEmployeeAbsEntry = 1) then
	   	ShowEmployeeAbsEdu := 1;
	else
    	ShowEmployeeAbsEdu := 0;
    end if;	
    
	if (:ShowServiceCall = 1) then
	    ShowServiceCalls := 1;
	else
	    ShowServiceCalls := 0;
	end if;
	
	sqlStr := 'Insert Into CRSPCalendarMonthlyActivityInLine_users
	Select UserId
	From OUSR
	Where USERID in ' || :UserIDs;
	exec (:sqlStr);

	sqlStr := 'Insert Into CRSPCalendarMonthlyActivityInLine_employees
	Select "empID"
	From OHEM
	Where "empID" in ' || :EmployeeIDs;
	exec (:sqlStr);
	
	FirstDay := :FirstDayIn;

--- get the real start date 
	DateStartPoint := ADD_DAYS(:GotoDate, - DAYOFMONTH(:GotoDate) + 1);
	
	Diff :=  :FirstWeekDay - (WEEKDAY(:DateStartPoint)+2);
	if (:Diff > 0) then
	  Diff := :Diff - 7;
	end if;
	DateStartPoint := ADD_DAYS(:DateStartPoint, :Diff);
	DateEndPoint := ADD_DAYS(:DateStartPoint, 34);
	
	insert into "CRSPCalendarMonthlyActivityInLineCalendar"(clgCode, HldCode, AbsEduSeq, CallID, Details, Action, Personal, AttendUser, AttendEmpl)
	(
		select T0."ClgCode", '', 0, 0, 'A' || '|' || Cast(T0."BeginTime" As Nvarchar(6)) || '|' || Cast(T0."ENDTime" As Nvarchar(6)) || '|' || IFNULL(T0."Details", ' ') As Description, T0."Action", T0."personal", T0."AttendUser", T0."AttendEmpl"
		from OCLG T0 
		where "ClgCode" In (:Activity1, :Activity2, :Activity3, :Activity4, :Activity5, :Activity6, :Activity7)
	);

	insert into "CRSPCalendarMonthlyActivityInLineCalendar"(clgCode, HldCode, AbsEduSeq, CallID, Details)
	(
		select DAYS_BETWEEN("EndDate", "StrDate")*10 + DAYS_BETWEEN("StrDate", :DateStartPoint) - 1, HldCode, 0, 0, 'H' || '|' || IFNull(T0."Rmrks", ' ')
		from HLD1 T0 
		where HldCode = :HldCode And DAYS_BETWEEN("EndDate", "StrDate")*10 + DAYS_BETWEEN("StrDate", :DateStartPoint) - 1 In (:Activity1, :Activity2, :Activity3, :Activity4, :Activity5, :Activity6, :Activity7)
	);

-- Store Employee Absence and Education to the temporary table
	if (:ShowEmployeeAbsEdu = 1) then
		insert into "CRSPCalendarMonthlyActivityInLineEmployeeAbsEdu"(UserId, EmployeeId, StartDate, EndDate, Details)
		(
-- Absence
			select T1."userId", T0."empID", T0."fromDate", T0."toDate", T0."reason"
			from HEM1 T0 
			inner join OHEM T1 On T1."empID" = T0."empID"
			where 
				((T0."fromDate" >= :DateStartPoint And T0."fromDate" <= :DateEndPoint) Or
				 (T0."toDate" >= :DateStartPoint And T0."toDate" <= :DateEndPoint) Or
				 (T0."fromDate" < :DateStartPoint And T0."toDate" > :DateEndPoint )) And
				(T1."userId" in (select uId from CRSPCalendarMonthlyActivityInLine_users) Or T1."empID" in (select empId from CRSPCalendarMonthlyActivityInLine_employees))
			union All
-- Education
			select T1."userId", T0."empID", T0."fromDate", T0."toDate", T2."name" 
			from HEM2 T0 
			inner join OHEM T1 on T1."empID" = T0."empID"
			inner join OHED T2 on T2."edType" = T0."type"
			where 
				((T0."fromDate" >= :DateStartPoint And T0."fromDate" <= :DateEndPoint) Or
				 (T0."toDate" >= :DateStartPoint And T0."toDate" <= :DateEndPoint) Or
				 (T0."fromDate" < :DateStartPoint And T0."toDate" > :DateEndPoint )) And
				(T1."userId" in (select uId from CRSPCalendarMonthlyActivityInLine_users) Or T1."empID" in (select empId from CRSPCalendarMonthlyActivityInLine_employees))
			order by T0."fromDate", T0."toDate" desc
		);
		
		
		insert into "CRSPCalendarMonthlyActivityInLineCalendar"(clgCode, HldCode, AbsEduSeq, CallID, Details, AttendUser, AttendEmpl)
		(
			select 0, '', AbsEduSeq, 0, 'E' || '|' || IFNull(T0.Details, ' ') As Description, T0.UserId, T0.EmployeeId
			from "CRSPCalendarMonthlyActivityInLineEmployeeAbsEdu" T0 
			where AbsEduSeq In (:EmployeeAbs1, :EmployeeAbs2, :EmployeeAbs3, :EmployeeAbs4, :EmployeeAbs5, :EmployeeAbs6, :EmployeeAbs7)
		);

	end if;

-- Store Service calls to the temporary table
	if (:ShowServiceCalls = 1) then

		insert into "CRSPCalendarMonthlyActivityInLineServiceCalls"(CallID, StartDate, EndDate, BeginTime, EndTime, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID, Instance)
		(
			select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee", T2."manager", T1."userId", T0."technician", 'A'
			from OSCL T0
			left outer join OHEM T1 On T1."empID" = T0."technician"
			left outer join OQUE T2 On T2."queueID" = T0."Queue"
			where 
				T0."DisplInCal" = 'Y' And
				((T0."StartDate" >= :DateStartPoint And T0."StartDate" <= :DateEndPoint) Or
				 (T0."EndDate" >= :DateStartPoint And T0."EndDate" <= :DateEndPoint) Or
				 (T0."StartDate" < :DateStartPoint And T0."EndDate" > :DateEndPoint )) And
			    (T0."assignee" in (select uId from CRSPCalendarMonthlyActivityInLine_users) Or T2."manager" in (select uId from CRSPCalendarMonthlyActivityInLine_users)) And
			    T0."callID" In (:ServiceCall1, :ServiceCall2, :ServiceCall3, :ServiceCall4, :ServiceCall5, :ServiceCall6, :ServiceCall7)
			union All
			select T0."callID", T0."StartDate", T0."EndDate", T0."StartTime", T0."EndTime", T0."subject", T0."assignee", T2."manager", T1."userId", T0."technician", 'T'
			from OSCL T0
			left outer join OHEM T1 On T1."empID" = T0."technician"
			left outer join OQUE T2 On T2."queueID" = T0."Queue"
			where 
				T0."DisplInCal" = 'Y' And
				((T0."StartDate" >= :DateStartPoint And T0."StartDate" <= :DateEndPoint) Or
				 (T0."EndDate" >= :DateStartPoint And T0."EndDate" <= :DateEndPoint) Or
				 (T0."StartDate" < :DateStartPoint And T0."EndDate" > :DateEndPoint )) And
			    (T1."userId" in (select uId from CRSPCalendarMonthlyActivityInLine_users) Or T0."technician" in (select empId from CRSPCalendarMonthlyActivityInLine_employees)) And
			    T0."callID" In (:ServiceCall1, :ServiceCall2, :ServiceCall3, :ServiceCall4, :ServiceCall5, :ServiceCall6, :ServiceCall7)
			order by T0."StartDate", T0."EndDate" Desc, T0."StartTime", T0."EndTime" desc
		);
		
		
		insert into "CRSPCalendarMonthlyActivityInLineCalendar"(clgCode, HldCode, AbsEduSeq, CallID, Details, AssigneeUID, QueueManagerUID, TechUID, TechEID, SCInstance)
		(
			select 0, '', 0, CallID, 'S' || '|' || Cast(T0.BeginTime As Nvarchar(6)) || '|' || Cast(T0.EndTime As Nvarchar(6)) || '|' || IFNull(T0.Details, ' ') As Description, T0.AssigneeUID, T0.QueueManagerUID, T0.TechUID, T0.TechEID, T0.Instance
			from "CRSPCalendarMonthlyActivityInLineServiceCalls" T0 
			where (CallID = :ServiceCall1 And Instance = :ServiceCallInstance1) Or
				  (CallID = :ServiceCall2 And Instance = :ServiceCallInstance2) Or
				  (CallID = :ServiceCall3 And Instance = :ServiceCallInstance3) Or
				  (CallID = :ServiceCall4 And Instance = :ServiceCallInstance4) Or
				  (CallID = :ServiceCall5 And Instance = :ServiceCallInstance5) Or
				  (CallID = :ServiceCall6 And Instance = :ServiceCallInstance6) Or
				  (CallID = :ServiceCall7 And Instance = :ServiceCallInstance7)
		);

	end if;   

select * from "CRSPCalendarMonthlyActivityInLineCalendar";

delete from "CRSPCalendarMonthlyActivityInLineCalendar";
delete from "CRSPCalendarMonthlyActivityInLineEmployeeAbsEdu";
delete from "CRSPCalendarMonthlyActivityInLineServiceCalls";
delete from CRSPCalendarMonthlyActivityInLine_users;
delete from CRSPCalendarMonthlyActivityInLine_employees;

end;











