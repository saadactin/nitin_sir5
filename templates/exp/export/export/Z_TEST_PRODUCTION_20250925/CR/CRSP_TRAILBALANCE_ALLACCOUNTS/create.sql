-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:SP:_TmSp_ValidateSpParam


CREATE PROCEDURE CRSP_TRAILBALANCE_ALLACCOUNTS (
      In AcctCodeList NClob,
      In AdjustFlag NVarchar(10),
      In ClosingPeriodFlag NVarchar(10),
      In DataType NVarchar(10),
      In MaxEndDate NVarchar(30),
      In PeriodBegDate NVarchar(30),
      In PeriodEndDate NVarchar(30), 
      In PostableFlag NVarchar(10),
      In FrgnCurrencySymbol1 NVarchar(3),
      In FrgnCurrencySymbol2 NVarchar(3),
      In FrgnCurrencySymbol3 NVarchar(3),
      In FrgnCurrencySymbol4 NVarchar(3),
      In FrgnCurrencySymbol5 NVarchar(3),
      In VoucherFlag NVarchar(2),
      In YearBegin NVarchar(30),
      In YearEnd NVarchar(30))
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
as 
queryStr NCLOB :='';
queryStr2 nvarchar(5000) :='';
whereStr  nvarchar(2000);
initCase nvarchar(1000);
periodCase nvarchar(1000);
yearCase nvarchar(1000);
sufCreditCase nvarchar(1000) :=' THEN T1."Credit" ELSE 0 END ';
sufDebitCase nvarchar(1000) := ' THEN T1."Debit" ELSE 0 END';
sufFCCreditCase nvarchar(1000) :=' THEN T1."FCCredit" ELSE 0 END ';
sufFCDebitCase nvarchar(1000) :=' THEN T1."FCDebit" ELSE 0 END ';
fcCurrencyStr nvarchar(1000) :='CASE WHEN T1."FCCurrency" =  ';
lcCurrencyStr nvarchar(1000) :='CASE WHEN T1."FCCurrency" IS NULL OR T1."FCCurrency" = ''''';
v_sql nvarchar(4000);
DO_WHILE_FLAG int;
TEMP_COUNT int;
V_ACCT_CODE nvarchar(15);
V_SCANED nvarchar(1);
SCANED_COUNT int;
i int:=1;
count_son int;
BEGIN
--For HANA security issue call procedure "_TmSp_ValidateSpParam"
    call _TmSp_ValidateSpParam(:AcctCodeList);    
    call _TmSp_ValidateSpParam(:AdjustFlag);    
	call _TmSp_ValidateSpParam(:ClosingPeriodFlag);   
	call _TmSp_ValidateSpParam(:DataType);   
	call _TmSp_ValidateSpParam(:MaxEndDate);   
	call _TmSp_ValidateSpParam(:PeriodBegDate);   
	call _TmSp_ValidateSpParam(:PeriodEndDate);  
	call _TmSp_ValidateSpParam(:PostableFlag);  	
	call _TmSp_ValidateSpParam(:FrgnCurrencySymbol1);  
	call _TmSp_ValidateSpParam(:FrgnCurrencySymbol2);  
	call _TmSp_ValidateSpParam(:FrgnCurrencySymbol3);  
	call _TmSp_ValidateSpParam(:FrgnCurrencySymbol4);  
	call _TmSp_ValidateSpParam(:FrgnCurrencySymbol5);  
	call _TmSp_ValidateSpParam(:VoucherFlag);  
	call _TmSp_ValidateSpParam(:YearBegin);  
	call _TmSp_ValidateSpParam(:YearEnd);  
	
IF :DataType  = 'Doc' THEN 
  whereStr := ' WHERE T1."RefDate" < '''||:MaxEndDate||'''';
  initCase := 'CASE WHEN T1."RefDate" < '''||:PeriodBegDate||''' OR (T1."RefDate" < '''||:PeriodEndDate||''' And T1."TransType" = -2) ';
  periodCase := 'CASE WHEN T1."RefDate" Between '''||:PeriodBegDate||'''  And '''||:PeriodEndDate||''' And T1."TransType" <> -2 ';
  yearCase := 'CASE WHEN T1."RefDate"  Between '''||:YearBegin||''' And '''||:YearEnd||'''  And T1."TransType" <> -2 ';
ELSEIF :DataType = 'Due' THEN
  whereStr := ' WHERE T1."DueDate" < '''||:MaxEndDate||'''';
  initCase := 'CASE WHEN T1."DueDate" < '''||:PeriodBegDate||''' OR (T1."RefDate" < '''||:PeriodEndDate||''' And T1."TransType" = -2) ';
periodCase := 'CASE WHEN T1."DueDate" Between ('''||:PeriodBegDate||''')  And  ('''||:PeriodEndDate||''') And T1."TransType" <> -2 ';
  yearCase := 'CASE WHEN T1."DueDate" Between ('''||:YearBegin||''') And ('''||:YearEnd||''')  And T1."TransType" <> -2 ';
ELSE
  whereStr := ' WHERE T1."TaxDate" < '''||:MaxEndDate||'''';
  initCase := 'CASE WHEN T1."TaxDate" < '''||:PeriodBegDate||''' OR (T1."RefDate" < '''||:PeriodEndDate||''' And T1."TransType" = -2) ';
  periodCase := 'CASE WHEN T1."TaxDate" Between ('''||:PeriodBegDate||''')  And  ('''||:PeriodEndDate||''') And T1."TransType" <> -2 ';
  yearCase := 'CASE WHEN T1."TaxDate"  Between ('''||:YearBegin||''') And ('''||:YearEnd||''')  And T1."TransType" <> -2 ';
END IF;

IF :ClosingPeriodFlag  = 'N' THEN
  whereStr := :whereStr || ' AND T1."TransType" <> ''-3''';
END IF;

IF :AdjustFlag  = 'N' THEN
  whereStr := :whereStr || ' AND T1."AdjTran" <> ''Y''';
END IF;

IF :PostableFlag = 'N' THEN 
      delete from "TMP_TrailBalance_AcctCode";
      v_sql:='CREATE SEQUENCE temp_code_sequence INCREMENT BY 1 START WITH 1';
      exec (:v_sql);
      --select max("Levels") into max_level from oact;
      v_sql:='insert into "TMP_TrailBalance_AcctCode" select "AcctCode","Levels",temp_code_sequence.nextval,null from oact a where a."AcctCode" in ' || :AcctCodeList;
      exec (:v_sql);
      do_while_flag:=1;
      
      while do_while_flag>0 do
            
        select count(*) into temp_count from "TMP_TrailBalance_AcctCode";
        for i in 1..temp_count do
        if :v_scaned is null then
                  update "TMP_TrailBalance_AcctCode" a set a.scaned = 'I';
                  
                  v_sql:='insert into "TMP_TrailBalance_AcctCode" (acct_code, acct_level) select DISTINCT(a."FatherNum"),a."Levels" from oact a INNER JOIN "TMP_TrailBalance_AcctCode" b on a."AcctCode" = b."ACCT_CODE" where b."SCANED" = ''I'' and a."FatherNum" NOT IN (select DISTINCT ("ACCT_CODE") from "TMP_TrailBalance_AcctCode")';
                  exec (:v_sql);

                  update "TMP_TrailBalance_AcctCode" a set a.scaned = 'Y';
                              
         end if;
            
            
        --select count(*) into temp_count from "TMP_TrailBalance_AcctCode";
        select count(*) into temp_count from "TMP_TrailBalance_AcctCode";
        end for;
        
		--select count(*) into temp_count from "TMP_TrailBalance_AcctCode";
        select count(*) into scaned_count from "TMP_TrailBalance_AcctCode" where scaned = 'Y';
        select count(*) into temp_count from "TMP_TrailBalance_AcctCode";
        if temp_count=scaned_count then 
            do_while_flag:=-1;
        end if;
        
      end while;
      
      delete from "TMP_TrailBalance_AcctLimited";

      insert into "TMP_TrailBalance_AcctLimited"
      SELECT T0."AcctCode", T0."AcctName", T0."AccntntCod", T0."FrgnName", T0."Levels", T0."FatherNum", T0."Postable", T0."GroupMask", T0."GrpLine", T0."BalDirect" FROM OACT T0, "TMP_TrailBalance_AcctCode" T1
      where T0."AcctCode"=T1.acct_code;
      v_sql:='drop sequence temp_code_sequence';
      exec (:v_sql);
      
END IF;
queryStr := :queryStr || ' SELECT T4.AcctCode, T4.AcctName, T4.AccntntCod, T4.FrgnName, T4.Levels,  T4.FatherNum,  T4.Postable, T4.GroupMask, T4.GrpLine, T4.BalDirect, 1 AS VirtualGroup, T3.InitCredit, T3.InitDebit, T3.CurrentCredit, T3.CurrentDebit, T3.YearCredit, T3.YearDebit,  T3.LCCredit,   T3.LCDebit, T3.FC1Credit, T3.FC1Debit, T3.FC2Credit, T3.FC2Debit, T3.FC3Credit, T3.FC3Debit, T3.FC4Credit, T3.FC4Debit,  T3.FC5Credit, T3.FC5Debit FROM ( ';

IF PostableFlag = 'N' THEN 
  queryStr := :queryStr || ' SELECT DISTINCT * FROM "TMP_TrailBalance_AcctLimited" ';
ELSE
  queryStr := :queryStr || ' SELECT T0."AcctCode" as ACCTCODE, T0."AcctName" AS ACCTNAME, T0."AccntntCod" AS ACCNTNTCOD, T0."FrgnName" AS FRGNNAME, T0."Levels" AS LEVELS, T0."FatherNum" AS FATHERNUM, T0."Postable" AS POSTABLE, T0."GroupMask" AS GROUPMASK, T0."GrpLine" AS GRPLINE, T0."BalDirect" AS BALDIRECT, 1 AS "VirtualGroup" FROM OACT AS T0 WHERE T0."AcctCode" IN '||:AcctCodeList;
END IF;
queryStr := :queryStr || ' ) AS T4 LEFT OUTER  JOIN ( ';

IF :VoucherFlag = 'Y' THEN 
   queryStr := :queryStr || ' SELECT "AcctCode", SUM(InitCredit) AS InitCredit, SUM(InitDebit) AS InitDebit, SUM(CurrentCredit) AS CurrentCredit, SUM(CurrentDebit) AS CurrentDebit, SUM(YearCredit) AS YearCredit, SUM(YearDebit) AS YearDebit,  SUM(LCCredit) AS LCCredit, SUM(LCDebit) AS LCDebit, SUM(FC1Credit) AS FC1Credit, SUM(FC1Debit) AS FC1Debit, SUM(FC2Credit) AS FC2Credit, SUM(FC2Debit) AS FC2Debit, SUM(FC3Credit) AS FC3Credit, SUM(FC3Debit) AS FC3Debit, SUM(FC4Credit) AS FC4Credit, SUM(FC4Debit) AS FC4Debit, SUM(FC5Credit) AS FC5Credit, SUM(FC5Debit) AS FC5Debit FROM ( ';
END IF;

queryStr := :queryStr 
                  ||' SELECT T0."AcctCode", SUM( ' || :initCase || :sufCreditCase || ') AS InitCredit, ' 
                  ||'SUM( ' || :initCase || :sufDebitCase || ') AS InitDebit, SUM( ' || :periodCase || :sufCreditCase || ') AS CurrentCredit, '
                  ||'SUM( ' || :periodCase || :sufDebitCase || ') AS CurrentDebit, SUM( ' || :yearCase || :sufCreditCase || ') AS YearCredit, '
                  ||'SUM( ' || :yearCase || :sufDebitCase || ') AS YearDebit, SUM( ' || :lcCurrencyStr || :sufCreditCase || ') AS LCCredit, '
                  ||'SUM( ' || :lcCurrencyStr || :sufDebitCase || ') AS LCDebit, SUM( ' || :fcCurrencyStr || ''''|| :FrgnCurrencySymbol1||'''' || :sufFCCreditCase || ') AS FC1Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol1||''''|| :sufFCDebitCase || ') AS FC1Debit, SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol2||'''' || :sufFCCreditCase || ') AS FC2Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol2||''''|| :sufFCDebitCase || ') AS FC2Debit, SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol3||'''' || :sufFCCreditCase || ') AS FC3Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol3||''''|| :sufFCDebitCase || ') AS FC3Debit, SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol4||'''' || :sufFCCreditCase || ') AS FC4Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol4||''''|| :sufFCDebitCase || ') AS FC4Debit, SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol5||'''' || :sufFCCreditCase || ') AS FC5Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol5||''''|| :sufFCDebitCase || ') AS FC5Debit ';
queryStr := :queryStr ||' FROM OACT AS T0 LEFT OUTER JOIN JDT1 T1 ON  T1."Account" = T0."AcctCode" LEFT OUTER  JOIN OJDT T2 ON  T2."TransId" = T1."TransId" ';
queryStr:= :queryStr|| :whereStr||' GROUP BY T0."AcctCode" ';
IF :VoucherFlag = 'Y' THEN 
queryStr2:= ' UNION ALL ';
queryStr2:= :queryStr2
                  ||' SELECT T0."AcctCode", SUM( ' || :initCase || :sufCreditCase || ') AS InitCredit, ' 
                  ||'SUM( ' || :initCase || :sufDebitCase || ') AS InitDebit, SUM( ' || :periodCase || :sufCreditCase || ') AS CurrentCredit, '
                  ||'SUM( ' || :periodCase || :sufDebitCase || ') AS CurrentDebit, SUM( ' || :yearCase || :sufCreditCase || ') AS YearCredit, '
                  ||'SUM( ' || :yearCase || :sufDebitCase || ') AS YearDebit, SUM( ' || :lcCurrencyStr || :sufCreditCase || ') AS LCCredit, '
                  ||'SUM( ' || :lcCurrencyStr || :sufDebitCase || ') AS LCDebit, SUM( ' || :fcCurrencyStr || ''''|| :FrgnCurrencySymbol1||'''' || :sufFCCreditCase || ') AS FC1Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol1||''''|| :sufFCDebitCase || ') AS FC1Debit, SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol2||'''' || :sufFCCreditCase || ') AS FC2Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol2||''''|| :sufFCDebitCase || ') AS FC2Debit, SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol3||'''' || :sufFCCreditCase || ') AS FC3Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol3||''''|| :sufFCDebitCase || ') AS FC3Debit, SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol4||'''' || :sufFCCreditCase || ') AS FC4Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol4||''''|| :sufFCDebitCase || ') AS FC4Debit, SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol5||'''' || :sufFCCreditCase || ') AS FC5Credit, '
                  ||'SUM( ' || :fcCurrencyStr || ''''||:FrgnCurrencySymbol5||''''|| :sufFCDebitCase || ') AS FC5Debit ';
queryStr2 := :queryStr2 ||' FROM OACT AS T0 LEFT OUTER JOIN BTF1 T1 ON T1."Account" = T0."AcctCode" LEFT OUTER  JOIN OBTF T2 ON  T2."TransId" = T1."TransId" ';
queryStr2:= :queryStr2|| :whereStr||' GROUP BY T0."AcctCode" ) AS Q0 GROUP BY Q0."AcctCode"';
END IF;

queryStr2 := queryStr2 || ' ) AS T3 ON T4.AcctCode = T3."AcctCode" ORDER BY T4.GroupMask, T4.GrpLine';
--EXECUTE sp_executesql @queryStr, @formatStr, '{?maxEnd}', '{?periodBegin}','{?periodEnd}','{?yearBegin}','{?yearEnd}'

delete from "TMP_TrailBalance_Output";
--select queryStr||queryStr2 from dummy;
EXEC ('insert into "TMP_TrailBalance_Output" select * from ('|| :queryStr || :queryStr2 || ')');

select * from "TMP_TrailBalance_Output";

delete from "TMP_TrailBalance_AcctCode";
delete from "TMP_TrailBalance_AcctLimited";
delete from "TMP_TrailBalance_Output";

END;











