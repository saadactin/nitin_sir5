-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

Create Procedure CRSP_Public_UserNames (
In UserIDs nclob, 
In EmployeeIDs nclob)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
	employeeID Int;
	firstName nvarchar(50);
	middleName nvarchar(50);
	lastName nvarchar(50); 
	name nvarchar(160);
cursor empCursor for select * From "CRSPPubUN_Employees";
Begin
		
	call _TmSp_ValidateSpParam(UserIDs);
	call _TmSp_ValidateSpParam(EmployeeIDs);
	
	delete from "CRSPPubUN_UsersEmployees";
	delete from "CRSPPubUN_Employees";
    
    exec ('insert into "CRSPPubUN_UsersEmployees" select "USERID", null, "U_NAME" from OUSR where "USERID" in ' || :UserIDs); 
 	exec ('insert into "CRSPPubUN_Employees" select "empID", ifnull("firstName", ''''), "middleName", "lastName" From OHEM Where "empID" in ' || :EmployeeIDs );
 	
 	open empCursor;
	fetch empCursor into employeeID, firstName, middleName, lastName;
	
	while not empCursor::NOTFOUND do
		
		name := ifnull(:lastName, '');
				
		if length(:firstName) > 0 then
			name := :name || ', ' || :firstName;
		end if;
				
		exec ('insert into "CRSPPubUN_UsersEmployees" values(null,' || :employeeID || ',''' || :name ||''')');
		
		fetch empCursor into employeeID, firstName, middleName, lastName;
	end while;
	
	close empCursor;
	select * from "CRSPPubUN_UsersEmployees";
	
	delete from "CRSPPubUN_UsersEmployees";
	delete from "CRSPPubUN_Employees";
End;











