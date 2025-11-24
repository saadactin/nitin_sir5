-- B1 DEPENDS: AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_DetailLedger_AllAccount
(In PrintJVoucherIn tinyint,
In PrintAdjustTransIn tinyint
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
execSQL nvarchar(4000);
whereStr nvarchar(1000);
selectStr nvarchar(1000);
groupStr nvarchar(200);
groupStr2 nvarchar(200);
orderStr nvarchar(200);
orderStr2 nvarchar(200);
printJVoucher tinyint;
printAdjTran tinyint;
postDateFrom nvarchar(30);
postDateTo nvarchar(30);
BEGIN
--insert into debug values ('DetailLedger_AllAccount ' || TO_CHAR(:PrintJVoucherIn));
--insert into debug values ('DetailLedger_AllAccount ' || TO_CHAR(:PrintAdjustTransIn));

delete from "CRSPDetailLedgerAllAccountResult";

printAdjTran := :PrintAdjustTransIn;
printJVoucher := :PrintJVoucherIn;
                  
selectStr := 'Select T0."AcctCode", 
		Max(T0."AcctName") as AcctName,
		T1."RefDate",
		Year(T1."RefDate") as RefYear, 
		Month(T1."RefDate") as RefMonth ,
		DAYOFYEAR(T1."RefDate")as RefDay,
	    Max(T2."Number") as Number, 
		Max(T1."LineMemo")as Memo, 
		Sum(T1."FCDebit") as FCDebit,
		Sum(T1."FCCredit")as FCCredit, 
		Max(T1."FCCurrency") as FCCurrency,
		Sum(T1."Debit") as Debit,
		Sum(T1."Credit") as Credit, 
		Max(T0."BalDirect") as BalDirect,
        Max(T1."Line_ID") as LineID,
        Max(T2."TransId") as TransID';
whereStr := '';
groupStr := 'Group By  Year(T1."RefDate"), Month(T1."RefDate"),T0."AcctCode", T2."TransId", T1."RefDate" ';
orderStr := 'Order By  T5."RefDate", T5."Number" ';
orderStr2 := 'Order By T5."RefDate", T5."Number"';
groupStr2 := 'Group By  Year(T1."RefDate"), Month(T1."RefDate"),T0."AcctCode",T2."TransId", T2."BatchNum", T1."Line_ID", T1."RefDate" ';

execSQL :=  :selectStr || '
		From OACT T0 inner join JDT1 T1 on T0."AcctCode" = T1."Account"
		inner join OJDT T2 on T2."TransId" = T1."TransId"  ';

If :printJVoucher = 0 Then
	if :printAdjTran = 0 Then
		whereStr := ' Where T2."AdjTran" <> ''Y'' ';
        execSQL := :execSQL || :whereStr;
	End	if;
	execSQL := execSQL || :groupStr;
	execSQL := 'Select  T5."AcctCode", T5.AcctName, T5."RefDate", T5.RefYear, T5.RefMonth , T5.RefDay, T5.Number, T5.Memo, 
						T5.FCDebit, T5.FCCredit, T5.FCCurrency, T5.Debit, T5.Credit, T5.BalDirect, T5.LineId, T5.TransId,
                        T4."CurOnRight", T4."DecSep", T4."SumDec", T4."ThousSep", T4."DateFormat", T4."CharMonth", T4."DateSep", T4."TimeFormat",
                        T6."LogoImage", T6."LogoFile", T6."MnhlNote", T4."CompnyName"  From ('  || :execSQL || ') T5 
						join (select *  from OADP ) T6 on 1=1 join  (Select *  from OADM) T4 on 1=1 ';
Else
	--Print Journal Voucher
	if :printAdjTran = 0 Then
		whereStr :=  ' Where T2."AdjTran" <> ''Y''  ';
        execSQL := :execSQL || :whereStr;
	End If;
		
	execSQL := :execSQL || :groupStr;	
	execSQL := :execSQL || ' union all ';
	execSQL := :execSQL || :selectStr;
	execSQL := :execSQL || '
			From OACT T0 inner join BTF1 T1 on T0."AcctCode" = T1."Account"
			inner join OBTF T2 on T2."TransId" = T1."TransId" and T1."BatchNum" = T2."BatchNum" and T2."BtfStatus" <>''C''  ';
	  	if :printAdjTran = 0 Then
			whereStr := ' where T2."AdjTran" <> ''Y''';
           execSQL := :execSQL || :whereStr;
		End If;
		execSQL :=  :execSQL || :groupStr2;
		execSQL := 'Select  T5."AcctCode", T5.AcctName, T5."RefDate", T5.RefYear, T5.RefMonth , T5.RefDay,
	                T5.Number, T5.Memo, T5.FCDebit, T5.FCCredit, T5.FCCurrency, T5.Debit, T5.Credit, 
					T5.BalDirect, T5.LineID, T5.TransId, T4."CurOnRight", T4."DecSep", T4."SumDec", T4."ThousSep",
                   T4."DateFormat", T4."CharMonth", T4."DateSep", T4."TimeFormat", T6."LogoImage", T6."LogoFile",
                    T6."MnhlNote", T4."CompnyName" From ('  || :execSQL || ')  T5 
					join (select *  from OADP ) T6 on 1=1
                    join (Select * from OADM) T4 on 1=1 ';
End If;

execSQL := 'Insert into "CRSPDetailLedgerAllAccountResult" (' || :execSQL || ' union all   
Select NULL as AcctCode, NULL as AcctName, NULL as RefDate, NULL as RefYear, NULL as   RefMonth,     NULL as RefDay,
   NULL as Number, NULL as Memo, NULL as FCDebit, NULL as FCCredit, NULL as FCCurrency, NULL as Debit, NULL as Credit,
   NULL as BalDirect, NULL as LineID, NULL as TransID,  T0."CurOnRight", T0."DecSep", T0."SumDec", T0."ThousSep", T0."DateFormat",
  T0."CharMonth", T0."DateSep", T0."TimeFormat", T1."LogoImage", T1."LogoFile", T1."MnhlNote", T0."CompnyName" from OADM T0 join OADP T1   on  1=1  Order by "RefDate", Number
)';
    
--    insert into debug values(:execSQL);

	Exec (:execSQL);
	
	select * from "CRSPDetailLedgerAllAccountResult";

delete from "CRSPDetailLedgerAllAccountResult";
	
END;











