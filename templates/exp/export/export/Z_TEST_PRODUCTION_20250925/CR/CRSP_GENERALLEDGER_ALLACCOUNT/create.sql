-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

CREATE PROCEDURE CRSP_GeneralLedger_AllAccount
(In AcctCodeList NCLOB,
In PrintJVoucher NVarchar(1),
In PrintAdjustTrans NVarchar(1),
In PostDateFromIn NVarchar(30),
in PostDateToIn nvarchar(30)
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
PrintAdjJE nvarchar(1);
SelectStr  nvarchar(4000);
JoinStr   nvarchar(4000);
execSQL  NCLOB;
tableName nvarchar(5);
preTable nvarchar(5);
fatherName nvarchar(10);
loopCounter int;
ind int;
AcctList  NCLOB;
UpdateStr nvarchar(1000);
CaseStr nvarchar(200);
dateTableStr NCLOB;
mainSQL NCLOB;
iYearFrom char(4);
iMonthFrom char(2);
iYearTo char(4);
iMonthTo char(2);
PostDateFrom NVarchar(30);
PostDateTo nvarchar(30);
numDate int;
BEGIN
--For HANA security issue call procedure "_TmSp_ValidateSpParam"
    call _TmSp_ValidateSpParam(:AcctCodeList);    
	call _TmSp_ValidateSpParam(:PrintJVoucher);    
	call _TmSp_ValidateSpParam(:PrintAdjustTrans);    
    call _TmSp_ValidateSpParam(:PostDateFromIn);    
	call _TmSp_ValidateSpParam(:PostDateToIn);    
	
PrintAdjJE := :PrintAdjustTrans;
AcctList := :AcctCodeList;
PostDateFrom := :PostDateFromIn;
PostDateTo := :PostDateToIn;

--insert into debug values ('AllAccount ' || :AcctCodeList);
--insert into debug values ('AllAccount ' || :PrintJVoucher);
--insert into debug values ('AllAccount ' || :PrintAdjustTrans);
--insert into debug values ('AllAccount ' || :PostDateFromIn);
--insert into debug values ('AllAccount ' || :PostDateToIn);

delete from "CRSPGeneralLedgerAllAccountDateTable";
delete from "CRSPGeneralLedgerAllAccountAcctFatherSonResult";
delete from "CRSPGeneralLedgerAllAccountAcctFatherSon";
delete from "CRSPGeneralLedgerAllAccountAcctFatherSonTable";

If length(:PostDateFrom) = 0 Then
     PostDateFrom := '1970-03-17';
end if;

If length(:PostDateTo) = 0 Then
    PostDateTo := '2020-03-17';
end if;

execSQL := 'insert into "CRSPGeneralLedgerAllAccountAcctFatherSonTable" (Select '' '' as GroupCode, T0."AcctCode", T0."AcctName",  T0."FatherNum" as Father0 ,T1."FatherNum" as Father1, T2."FatherNum" as Father2, T3."FatherNum" as Father3, T4."FatherNum" as Father4 
From OACT T0   
Left outer Join (Select "AcctCode", "FatherNum" from OACT) T1 on T1."AcctCode" = T0."FatherNum"  
Left outer Join (Select "AcctCode", "FatherNum" from OACT) T2 on T2."AcctCode" = T1."FatherNum"  
Left outer Join (Select "AcctCode", "FatherNum" from OACT) T3 on T3."AcctCode" = T2."FatherNum"  
Left outer Join (Select "AcctCode", "FatherNum" from OACT) T4 on T4."AcctCode" = T3."FatherNum"  where T0."AcctCode" in ' || :AcctList || ')';

--insert into debug values (:execSQL);
exec (:execSQL);

Update "CRSPGeneralLedgerAllAccountAcctFatherSonTable" Set GroupCode = ( Case When (length(AcctCode) = 4 
	And ascii(substr(AcctCode, 0, 1)) > 47 And ascii(substr(AcctCode, 0, 1)) < 58
	And ascii(substr(AcctCode, 1, 1)) > 47 And ascii(substr(AcctCode, 1, 1)) < 58
	And ascii(substr(AcctCode, 2, 1)) > 47 And ascii(substr(AcctCode, 2, 1)) < 58
	And ascii(substr(AcctCode, 3, 1)) > 47 And ascii(substr(AcctCode, 3, 1)) < 58
	) Then Acctcode  When (length(Father0) = 4) 
		And ascii(substr(Father0, 0, 1)) > 47 And ascii(substr(Father0, 0, 1)) < 58 
		And ascii(substr(Father0, 1, 1)) > 47 And ascii(substr(Father0, 1, 1)) < 58 
		And ascii(substr(Father0, 2, 1)) > 47 And ascii(substr(Father0, 2, 1)) < 58 
		And ascii(substr(Father0, 3, 1)) > 47 And ascii(substr(Father0, 3, 1)) < 58 Then Father0 When (length(Father1) = 4) 
		And ascii(substr(Father1, 0, 1)) > 47 And ascii(substr(Father1, 0, 1)) < 58 
		And ascii(substr(Father1, 1, 1)) > 47 And ascii(substr(Father1, 1, 1)) < 58 
		And ascii(substr(Father1, 2, 1)) > 47 And ascii(substr(Father1, 2, 1)) < 58 
		And ascii(substr(Father1, 3, 1)) > 47 And ascii(substr(Father1, 3, 1)) < 58 Then Father1 When (length(Father2) = 4) 
		And ascii(substr(Father2, 0, 1)) > 47 And ascii(substr(Father2, 0, 1)) < 58 
		And ascii(substr(Father2, 1, 1)) > 47 And ascii(substr(Father2, 1, 1)) < 58 
		And ascii(substr(Father2, 2, 1)) > 47 And ascii(substr(Father2, 2, 1)) < 58 
		And ascii(substr(Father2, 3, 1)) > 47 And ascii(substr(Father2, 3, 1)) < 58 Then Father2 When (length(Father3) = 4) 
		And ascii(substr(Father3, 0, 1)) > 47 And ascii(substr(Father3, 0, 1)) < 58 
		And ascii(substr(Father3, 1, 1)) > 47 And ascii(substr(Father3, 1, 1)) < 58 
		And ascii(substr(Father3, 2, 1)) > 47 And ascii(substr(Father3, 2, 1)) < 58 
		And ascii(substr(Father3, 3, 1)) > 47 And ascii(substr(Father3, 3, 1)) < 58 Then Father3 When (length(Father4) = 4) 
		And ascii(substr(Father4, 0, 1)) > 47 And ascii(substr(Father4, 0, 1)) < 58 
		And ascii(substr(Father4, 1, 1)) > 47 And ascii(substr(Father4, 1, 1)) < 58 
		And ascii(substr(Father4, 2, 1)) > 47 And ascii(substr(Father4, 2, 1)) < 58 
		And ascii(substr(Father4, 3, 1)) > 47 And ascii(substr(Father4, 3, 1)) < 58 Then Father4 Else Acctcode End);

insert into "CRSPGeneralLedgerAllAccountAcctFatherSon" 
(select T0.GroupCode, T1."AcctName" as GroupName, T1."BalDirect" as GroupDirect, T0.AcctCode, T0.AcctName 
from "CRSPGeneralLedgerAllAccountAcctFatherSonTable" T0 
inner join OACT T1 on T0.GroupCode = T1."AcctCode"
);

insert into "CRSPGeneralLedgerAllAccountDateTable"
(Select Year(T0."RefDate") as AllYear, Month(T0."RefDate") as AllMonth from JDT1 T0 
group by Year(T0."RefDate"), Month(T0."RefDate")
);


If Year(:PostDateFrom) > 1970 then
	select Count(*) into numDate from "CRSPGeneralLedgerAllAccountDateTable" where AllYear = LTrim(Year(:PostDateFrom)) and AllMonth = LTrim(Month(:PostDateFrom));
Else
	select Count(*) into numDate from "CRSPGeneralLedgerAllAccountDateTable" where AllYear = LTrim(Year(:PostDateTo)) and AllMonth = LTrim(Month(:PostDateTo));
End if;

If :numDate = 0 Then
    If Year(:PostDateFrom) > 1970 then  
		Insert into "CRSPGeneralLedgerAllAccountDateTable" values (TO_NVARCHAR(Year(:PostDateFrom)),TO_NVARCHAR(Month(:PostDateFrom)));
	Else 
		Insert into "CRSPGeneralLedgerAllAccountDateTable" values (TO_NVARCHAR(Year(:PostDateTo)),TO_NVARCHAR(Month(:PostDateTo)));
	End If;
End If;


--Begin Create 'MainTable' for JE

SelectStr := 'Select J1."Account", J1."Debit", J1."Credit", J1."TransId", J1."Line_ID", J1."RefDate", Year(J1."RefDate") as Year, Month(J1."RefDate") as Month, DAYOFYEAR(J1."RefDate") as Day,J1."LineMemo", J0."Number" ';
mainSQL :=  :SelectStr || ' From JDT1 J1 inner join OJDT J0 on J1."TransId" = J0."TransId"  where J1."Account" in ' || :AcctList || ' And J1."RefDate" >=''' || :PostDateFrom || '''  And J1."RefDate" <=''' || :PostDateTo || '''';

If :PrintAdjJE = 'N' then
	mainSQL := :mainSQL || ' And  J0."AdjTran" <> ''Y''';
End if;

if :PrintJVoucher = 'Y' then
	mainSQL := :mainSQL || 'Union all ';
    mainSQL := :mainSQL || :SelectStr;
	mainSQL := :mainSQL || ' From BTF1 J1 inner join OBTF J0 on J1."TransId" = J0."TransId" and J1."BatchNum" = J0."BatchNum"  where  J0."BtfStatus" <>''C''  and  J1."Account" in ' || :AcctList || ' And J1."RefDate" >=''' || :PostDateFrom || '''  And J1."RefDate" <=''' || :PostDateTo || '''';
	If  :PrintAdjJE = 'N' then
		mainSQL := :mainSQL || ' And  J0."AdjTran" <> ''Y''';
	End if;
End if;

--End Create 'MainTable' for 
iYearFrom := LTrim(Year(:PostDateFrom));
iYearTo := LTrim(Year(:PostDateTo));
iMonthFrom :=  LTrim(Month(:PostDateFrom));
iMonthTo :=  LTrim(Month(:PostDateTo));

--Label :Add date filter here, use Year * 100 + Month to compare ( '1980-03' : 1980*100 + 3 = 19800

execSQL := ' Insert into "CRSPGeneralLedgerAllAccountAcctFatherSonResult" (Select * from "CRSPGeneralLedgerAllAccountAcctFatherSon" T0 join "CRSPGeneralLedgerAllAccountDateTable" T1 on
 ( T1.AllYear * 100 + T1.AllMonth >= ' || :iYearFrom || ' * 100 +' ||  :iMonthFrom || ' And T1.AllYear * 100 + T1.AllMonth <= ' || :iYearTo || ' * 100 + ' || :iMonthTo || ') '
	|| '  Left Outer Join  (' || :mainSQL || ') TMain on TMain."Account" = T0.AcctCode and TMain.Year = T1.AllYear and TMain.Month = T1.AllMonth  ';

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Some Account HAS JE in JDT1 in some month, but the RefDate is out of the Date Range, to keep the Account Info , Here it has to Use 'Union' ,haha!!
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

execSQL := :execSQL || ' Union all  Select * , NULL as Account, 0 as Debit, 0 as Credit , NULL as TransId, NULL as Line_id, NULL as RefDate, NULL as Year, NULL as Month, NULL as Day, NULL as LineMemo, NULL as Number  from "CRSPGeneralLedgerAllAccountAcctFatherSon" A0 Join "CRSPGeneralLedgerAllAccountDateTable" A1 on '
 ' ( A1.AllYear * 100 + A1.AllMonth >= ' || :iYearFrom || ' * 100 +' ||  :iMonthFrom ||  ' And A1.AllYear * 100 + A1.AllMonth <= ' || :iYearTo || ' * 100 + ' || :iMonthTo || ') ' ||
 ' order by T1.AllYear, T1.AllMonth, T0.AcctCode )';

	--print (@execSQL);

	exec (:execSQL);
--	insert into debug values (:CaseStr);
--	insert into debug values (:UpdateStr);
--	insert into debug values (:execSQL);

	select * from "CRSPGeneralLedgerAllAccountAcctFatherSonResult";
	
	delete from "CRSPGeneralLedgerAllAccountDateTable";
	delete from "CRSPGeneralLedgerAllAccountAcctFatherSonResult";
	delete from "CRSPGeneralLedgerAllAccountAcctFatherSon";
	delete from "CRSPGeneralLedgerAllAccountAcctFatherSonTable";
END;











