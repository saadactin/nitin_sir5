-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:SP:_TmSp_ValidateSpParam

Create Procedure CRSP_DetailLedger_OpenBalance (
 IsFromFilled TinyInt,
 PrintJVoucherIn NVarchar(1),
 PostDateFromIn NVarchar(30),
 PrintAdjustTransIn NVarchar(1),
 AccountCodeIn NVarchar(100)
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
execSQL nvarchar(4000);
whereStr nvarchar(1000);
selectStr nvarchar(1000);
printAdjTran NVarchar(1);
printJVoucher NVarchar(1);
accountCode nvarchar(100);
postDateFrom nvarchar(30);
Begin
--For HANA security issue call procedure "_TmSp_ValidateSpParam"
    call _TmSp_ValidateSpParam(:PrintJVoucherIn);    
	call _TmSp_ValidateSpParam(:PostDateFromIn);    
    call _TmSp_ValidateSpParam(:PrintAdjustTransIn);    
	call _TmSp_ValidateSpParam(:AccountCodeIn);    
	
--insert into debug values ('DetailLedger_OpenBalance ' || TO_CHAR(:IsFromFilled));
--insert into debug values ('DetailLedger_OpenBalance ' || :PrintJVoucherIn);
--insert into debug values ('DetailLedger_OpenBalance ' || :PostDateFromIn);
--insert into debug values ('DetailLedger_OpenBalance ' || :PrintAdjustTransIn);
--insert into debug values ('DetailLedger_OpenBalance ' || :AccountCodeIn);

delete from "CRSPDetailLedgerOpenBalanceResult";

postDateFrom := :PostDateFromIn;

If :IsFromFilled = 1 then
  postDateFrom := :PostDateFromIn;
Else
  postDateFrom := '';
End If;

accountCode := AccountCodeIn;

printAdjTran := 'N';
printJVoucher := 'N';

If :PrintAdjustTransIn = 'Y' then
  printAdjTran := 'Y';
End If;

If :PrintJVoucherIn = 'Y' then 
  printJVoucher := 'Y';
End If;

--If user input NO from , Set Opening Balance to ZERO
If length(:postDateFrom) = 0 then
	execSQL := 'insert into "CRSPDetailLedgerOpenBalanceResult" Select 0 as Debit, 0 as Credit from DUMMY';
Else
	If :printJVoucher = 'N' then
		selectStr := 'insert into "CRSPDetailLedgerOpenBalanceResult" select Sum("Debit") as Debit , Sum("Credit") as Credit From JDT1 T0 ';
		whereStr := '  Where  T0."Account" = ''' || :accountCode || ''' ';
		whereStr := :whereStr || '  And T0."RefDate" < ''' || :postDateFrom || ''' ';
		If :printAdjTran = 'N' Then
			whereStr := :whereStr || 'And T0."AdjTran" <>''Y'' ';
		End If;
		
		execSQL := :selectStr || :whereStr;
	Else
		selectStr :=  'insert into "CRSPDetailLedgerOpenBalanceResult" Select Sum("Debit") as Debit , Sum("Credit") as Credit From JDT1 T0 ';
		whereStr := '  Where  T0."Account" = ''' || :accountCode || ''' ';
		whereStr := :whereStr || '  And T0."RefDate" < ''' || :postDateFrom  || ''' ';
		If :printAdjTran = 'N' Then
			whereStr := :whereStr || 'And T0."AdjTran" <>''Y'' ';
		End If;
		
		execSQL := :selectStr || :whereStr;
		execSQL := :execSQL || ' union all ';
		selectStr := 'Select Sum("Debit") as Debit , Sum("Credit") as Credit From BTF1 T0 
				join OBTF T1 on T0."BatchNum" = T1."BatchNum" and T0."TransId" = T1."TransId" ';
		whereStr := '  Where  T0."Account" = ''' || :accountCode || ''' ';
		whereStr := :whereStr || ' And T1."BtfStatus" <> ''C'' And T0."RefDate" < ''' || :postDateFrom || ''' ';
		If :printAdjTran = 'N' Then
			whereStr := :whereStr || 'And T0."AdjTran" <>''Y'' ';
		End If;
		
		execSQL := :execSQL || :selectStr || :whereStr;
	End If;
End If;

--insert into debug values (:execSQL);



Exec (:execSQL);
select * from "CRSPDetailLedgerOpenBalanceResult";

delete from "CRSPDetailLedgerOpenBalanceResult";

End;











