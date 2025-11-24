-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

create procedure CRSP_GeneralLedger_OpenBalance (
	in AcctCodeList NCLOB,
	in PrintJVoucher nvarchar(1),
	in PrintAdjustTrans nvarchar(1),
	in PostDateFromIn nvarchar(30),
	in PostDateToIn nvarchar(30)
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
PrintAdjJE nvarchar(1);
acctList1  NCLOB;
SelectStr  nvarchar(4000);
FromStr   NCLOB;
JoinStr   nvarchar(4000);
whereStr  nvarchar(4000);
execSQL   NCLOB;
tableName nvarchar(5);
preTable nvarchar(5);
fatherName nvarchar(10);
loopCounter int;
ind int;
postDateFrom nvarchar(30);
postDateTo   nvarchar(30);
GetOpeningBal  nvarchar(1000); 
UpdateStr NCLOB;
CaseStr NCLOB;
BEGIN
--For HANA security issue call procedure "_TmSp_ValidateSpParam"
	call _TmSp_ValidateSpParam(:AcctCodeList);    
    call _TmSp_ValidateSpParam(:PrintJVoucher);    
	call _TmSp_ValidateSpParam(:PrintAdjustTrans);    
    call _TmSp_ValidateSpParam(:PostDateFromIn);    
	call _TmSp_ValidateSpParam(:PostDateToIn);    
	
--insert into debug values ('OpenBalance ' || :AcctCodeList);
--insert into debug values ('OpenBalance ' || :PrintJVoucher);
--insert into debug values ('OpenBalance ' || :PrintAdjustTrans);
--insert into debug values ('OpenBalance ' || :PostDateFromIn);
--insert into debug values ('OpenBalance ' || :PostDateToIn);

	delete from CRSPGeneralLedgerOpenBalance_tempLDGTable;

	loopCounter := 4;
	ind := 1;
	JoinStr := '';
	acctList1 := :AcctCodeList;
	postDateFrom := :PostDateFromIn;
	postDateTo := :PostDateToIn;
	PrintAdjJE := :PrintAdjustTrans;

	SelectStr := 'Select '' '' as GroupColumn, T0."AcctCode", T0."AcctName",  T0."FatherNum" as Father0 ';
	WHILE :loopCounter >= :ind Do
		tableName := 'T' || :ind;
		preTable := 'T' || (:ind-1);
		fatherName := 'Father' || :ind;
		SelectStr := :SelectStr || ',' || :tableName ||'."FatherNum" as ' || :fatherName;
		JoinStr := :JoinStr || ' Left outer Join (Select "AcctCode", "FatherNum" from OACT)' || tableName || ' on '|| :tableName ||'."AcctCode" = ' || :preTable ||'."FatherNum" ';
		ind := :ind + 1;
	End while;

	FromStr := ', T0."Postable", T0."Levels", T0."BalDirect", J0."Debit", J0."Credit", J0."TransId", J0."Line_ID", J0."RefDate", Year(J0."RefDate") as Year, Month(J0."RefDate") as Month, DAYOFYEAR(J0."RefDate") as Day, J0."LineMemo", J1."Number" From (Select * from OACT  where "AcctCode" in '|| :acctList1 || ') T0 Left Outer Join (Select * from JDT1 where  ';
	
	If length(:postDateFrom) = 0 Then
    	postDateFrom := '19700101';
	End if;

	FromStr := :FromStr || 'JDT1."RefDate" < ''' || :postDateFrom || ''' ';

	FromStr := :FromStr || ') J0 on T0."AcctCode" = J0."Account"  Inner Join OJDT J1  on J1."TransId" = J0."TransId" ';
	If :PrintAdjJE = 'N' Then
		FromStr := :FromStr || ' And J1."AdjTran" <> ''Y''';
	End If;

	execSQL := :SelectStr || :FromStr || :JoinStr;

	if :PrintJVoucher = 'Y' Then
		execSQL := :execSQL || 'Union all ';

		FromStr := ', T0."Postable", T0."Levels", T0."BalDirect", J0."Debit", J0."Credit", J0."TransId", J0."Line_ID", J0."RefDate", Year(J0."RefDate") as Year, Month(J0."RefDate") as Month, DAYOFYEAR(J0."RefDate") as Day,J0."LineMemo", J1."Number" From (Select * from OACT  where "AcctCode" in '||:acctList1 ||') T0 Left Outer Join (Select * from BTF1 where '; 
    	FromStr := :FromStr || 'BTF1."RefDate" <''' || :postDateFrom || ''' ';
		FromStr := :FromStr || ') J0 on T0."AcctCode" = J0."Account"  Inner Join OBTF J1  on J1."TransId" = J0."TransId" and J1."BatchNum" = J0."BatchNum" and J1."BtfStatus" <>''C'' ';
		
		If :PrintAdjJE = 'N' Then
			FromStr := :FromStr || ' And J1."AdjTran" <> ''Y''';
		End If;
    	execSQL := :execSQL || :SelectStr || :FromStr || :JoinStr;
	End If;

	execSQL := 'Insert into CRSPGeneralLedgerOpenBalance_tempLDGTable (' || :execSQL || ')';
	UpdateStr := ' Update CRSPGeneralLedgerOpenBalance_tempLDGTable Set GroupColumn = ( Case When (length(AcctCode) = 4 
	And ascii(substr(AcctCode, 0, 1)) > 47 And ascii(substr(AcctCode, 0, 1)) < 58
	And ascii(substr(AcctCode, 1, 1)) > 47 And ascii(substr(AcctCode, 1, 1)) < 58
	And ascii(substr(AcctCode, 2, 1)) > 47 And ascii(substr(AcctCode, 2, 1)) < 58
	And ascii(substr(AcctCode, 3, 1)) > 47 And ascii(substr(AcctCode, 3, 1)) < 58
	) Then Acctcode ';

--Begin to search GL Account , fill it into 'GroupColumn'
	ind := 0;

	While :loopCounter >= :ind Do
		fatherName := 'Father' || :ind;
		CaseStr := ' When (length(' || :fatherName || ') = 4) 
		And ascii(substr(' || :fatherName || ', 0, 1)) > 47 And ascii(substr(' || :fatherName || ', 0, 1)) < 58 
		And ascii(substr(' || :fatherName || ', 1, 1)) > 47 And ascii(substr(' || :fatherName || ', 1, 1)) < 58 
		And ascii(substr(' || :fatherName || ', 2, 1)) > 47 And ascii(substr(' || :fatherName || ', 2, 1)) < 58 
		And ascii(substr(' || :fatherName || ', 3, 1)) > 47 And ascii(substr(' || :fatherName || ', 3, 1)) < 58 Then ' || :fatherName;
		UpdateStr := :UpdateStr || :CaseStr;
		ind := :ind + 1;
	End while;

	UpdateStr := :UpdateStr || ' Else Acctcode End)';
--End Search

	Exec (:execSQL);
	Exec (:UpdateStr);
	
	Select  GroupColumn, Sum(Debit) as OBDebit, Sum(Credit) as OBCredit from CRSPGeneralLedgerOpenBalance_tempLDGTable T0 group by  T0.GroupColumn;

--	insert into debug values(:execSQL);
--	insert into debug values(:UpdateStr);

	delete from CRSPGeneralLedgerOpenBalance_tempLDGTable;
END;











