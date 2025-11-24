-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:SP:_TmSp_ValidateSpParam

CREATE PROCEDURE "CRSP_TrailBalanceCN" (
	postFlagParam nvarchar(10),			--'{?postable}' 
	voucherFlagParam nvarchar(2), 		--'{?voucherFlag}'
	dateTypeParam nvarchar(10),			--'{?dateType}'
	closingFlagParam nvarchar(10),		--'{?closingFlag}'
	adjustFlagParam nvarchar(10),		--'{?adjustFlag}'
	acctCodeListParam nvarchar(15000),	--'{?acctCode}'	
	P1Param datetime,				--'{?maxEnd}'
	P2Param datetime,				--'{?periodBegin}'
	P3Param datetime,				--'{?periodEnd}'
	P4Param datetime,				--'{?yearBegin}'
	P5Param datetime,				--'{?yearEnd}'		
	symbol1	nvarchar(15),				--'{?symbol1}'
	symbol2	nvarchar(15),				--'{?symbol2}'
	symbol3	nvarchar(15),				--'{?symbol3}'
	symbol4	nvarchar(15),				--'{?symbol4}'
	symbol5	nvarchar(15)				--'{?symbol5}'
) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
	queryStr1 nvarchar(30000);
	queryStr2 nvarchar(5000);
	queryStr3 nvarchar(5000);
	queryStr4 nvarchar(5000);
	queryStr5 nvarchar(5000);
	queryStr6 nvarchar(5000);
	queryStr7 nvarchar(5000);	
	voucherFlag nvarchar(2);
	whereStr nvarchar(300);
	dateType nvarchar(10);
	initCase nvarchar(300);
	periodCase nvarchar(300);
	yearCase nvarchar(300);
	sufCreditCase nvarchar(100);
	sufDebitCase nvarchar(100);
	sufFCCreditCase nvarchar(100);
	sufFCDebitCase nvarchar(100);
	closingFlag nvarchar(10);
	adjustFlag nvarchar(10);
	fcCurrencyStr nvarchar(100);
	lcCurrencyStr nvarchar(100);
	postFlag nvarchar(10);
	initCondition nvarchar(300);
	periodCondition nvarchar(300);
	yearCondition nvarchar(300);
	P1 nvarchar(50);					
	P2 nvarchar(50);					
	P3 nvarchar(50);					
	P4 nvarchar(50);					
	P5 nvarchar(50);
	oldTempTabSize int;
	newTempTabSize int;	
	processBatchId smallInt;
BEGIN
--For HANA security issue call procedure "_TmSp_ValidateSpParam"
    call _TmSp_ValidateSpParam(:postFlagParam);    
    call _TmSp_ValidateSpParam(:voucherFlagParam);    
	call _TmSp_ValidateSpParam(:dateTypeParam);   
	call _TmSp_ValidateSpParam(:closingFlagParam);   
	call _TmSp_ValidateSpParam(:adjustFlagParam);   
	call _TmSp_ValidateSpParam(:acctCodeListParam);   
	call _TmSp_ValidateSpParam(:symbol1);  
	call _TmSp_ValidateSpParam(:symbol2);  	
	call _TmSp_ValidateSpParam(:symbol3);  
	call _TmSp_ValidateSpParam(:symbol4);  
	call _TmSp_ValidateSpParam(:symbol5);  
	
    queryStr1 := '';
    queryStr2 := '';
    queryStr3 := '';
    queryStr4 := '';
    queryStr5 := '';
    queryStr6 := '';
    queryStr7 := '';
       
    sufCreditCase := ' THEN T1."Credit" ELSE 0 END ';
    sufDebitCase := ' THEN T1."Debit" ELSE 0 END ';
    sufFCCreditCase := ' THEN T1."FCCredit" ELSE 0 END ';
    sufFCDebitCase := ' THEN T1."FCDebit" ELSE 0 END ';
    fcCurrencyStr := 'CASE WHEN T1."FCCurrency" = ';
    lcCurrencyStr := 'CASE WHEN (T1."FCCurrency" IS NULL OR T1."FCCurrency" = '''') ';
    postFlag := postFlagParam;
    voucherFlag := voucherFlagParam;
    dateType := dateTypeParam;
    closingFlag := closingFlagParam;
    adjustFlag := adjustFlagParam;
	P1 := '''' || :P1Param || '''';
	P2 := '''' || :P2Param || '''';
	P3 := '''' || :P3Param || '''';
	P4 := '''' || :P4Param || '''';
	P5 := '''' || :P5Param || '''';	
    
    IF :dateType = 'Doc' THEN 
        whereStr := ' WHERE T1."RefDate" < ' || :P1;
        initCase := 'CASE WHEN T1."RefDate" < ' || :P2 || ' OR (T1."RefDate" < ' || :P3 || ' And T1."TransType" = -2) ';
        periodCase := 'CASE WHEN T1."RefDate" Between (' || :P2 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
        yearCase := 'CASE WHEN T1."RefDate" Between (' || :P4 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
        initCondition := ' AND (T1."RefDate" < ' || :P2 || ' OR (T1."RefDate" < ' || :P3 || ' And T1."TransType" = -2))';
        periodCondition := ' AND T1."RefDate" Between (' || :P2 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
        yearCondition := ' AND T1."RefDate" Between (' || :P4 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
    ELSE 
        IF :dateType = 'Due' THEN 
            whereStr := ' WHERE T1."DueDate" < ' || :P1 ;
            initCase := 'CASE WHEN T1."DueDate" < ' || :P2 || ' OR (T1."RefDate" < ' || :P3 || ' And T1."TransType" = -2) ';
            periodCase := 'CASE WHEN T1."DueDate" Between (' || :P2 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
            yearCase := 'CASE WHEN T1."DueDate" Between (' || :P4 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
            initCondition := ' AND (T1."DueDate" < ' || :P2 || ' OR (T1."RefDate" < ' || :P3 || ' And T1."TransType" = -2)) ';
            periodCondition := ' AND T1."DueDate" Between (' || :P2 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
            yearCondition := ' AND T1."DueDate" Between (' || :P4 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
        ELSE 
            whereStr := ' WHERE T1."TaxDate" < ' || :P1 ;
            initCase := 'CASE WHEN T1."TaxDate" < ' || :P2 || ' OR (T1."RefDate" < ' || :P3 || ' And T1."TransType" = -2) ';
            periodCase := 'CASE WHEN T1."TaxDate" Between (' || :P2 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
            yearCase := 'CASE WHEN T1."TaxDate" Between (' || :P4 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
            initCondition := ' AND (T1."TaxDate" < ' || :P2 || ' OR (T1."RefDate" < ' || :P3 || ' And T1."TransType" = -2)) ';
            periodCondition := ' AND T1."TaxDate" Between (' || :P2 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
            yearCondition := ' AND T1."TaxDate" Between (' || :P4 || ') And (' || :P3 || ') And T1."TransType" <> -2 ';
        END IF;
    END IF;   
    IF :closingFlag = 'N' THEN 
        whereStr := :whereStr || ' AND T1."TransType" <> ''-3''';
    END IF;
    IF :adjustFlag = 'N' THEN 
        whereStr := :whereStr || ' AND T1."AdjTran" <> ''Y''';
    END IF;   
    IF :postFlag = 'N' THEN
		processBatchId := 1;
		oldTempTabSize := 0;
		delete from "CRSPTrailBalanceCNAcctLimitted";
		
		queryStr1 := 'insert into "CRSPTrailBalanceCNAcctLimitted" 
				select T0."AcctCode", T0."AcctName", T0."AccntntCod", T0."FrgnName", T0."Levels", T0."FatherNum", T0."Postable", T0."GroupMask", T0."GrpLine", T0."BalDirect", 1 
				FROM OACT AS T0 
				WHERE T0."AcctCode" IN (' || :acctCodeListParam || ')';				
		exec (:queryStr1);				
				
		select count(*) insert into newTempTabSize from "CRSPTrailBalanceCNAcctLimitted";		
		while :newTempTabSize > :oldTempTabSize do
			oldTempTabSize := :newTempTabSize;
			
			insert into	"CRSPTrailBalanceCNAcctLimitted" 
				select DISTINCT (T0."AcctCode"), T0."AcctName", T0."AccntntCod", T0."FrgnName", T0."Levels", T0."FatherNum", T0."Postable", T0."GroupMask", T0."GrpLine", T0."BalDirect", processBatchId + 1 
				FROM OACT AS T0, "CRSPTrailBalanceCNAcctLimitted" AS T1
				WHERE T0."AcctCode" = T1."FatherNum" And T1."ProcessBatch" = :processBatchId;
						
			processBatchId := :processBatchId + 1;
			select count(*) insert into newTempTabSize from "CRSPTrailBalanceCNAcctLimitted";
		end while;
		queryStr1 := '';	--this will be reused		
    END IF;

    queryStr1 := :queryStr1 || '     
		SELECT T4."AcctCode", T4."AcctName", T4."AccntntCod", T4."FrgnName", T4."Levels", T4."FatherNum", T4."Postable", T4."GroupMask", T4."GrpLine", T4."BalDirect", 1 AS VirtualGroup,
		T3.InitCredit, T3.InitDebit, T3.CurrentCredit, T3.CurrentDebit, T3.YearCredit, T3.YearDebit, T3.LCCredit, T3.LCDebit, 
		T3.FC1Credit, T3.FC1Debit, T3.FC2Credit, T3.FC2Debit, T3.FC3Credit, T3.FC3Debit, T3.FC4Credit, T3.FC4Debit, T3.FC5Credit, T3.FC5Debit, 
		T3.LCInitCredit, T3.LCInitDebit,T3.LCPeriodCredit, T3.LCPeriodDebit,T3.LCYearCredit, T3.LCYearDebit,
		T3.FC1InitCredit, T3.FC1InitDebit, T3.FC2InitCredit, T3.FC2InitDebit, T3.FC3InitCredit, T3.FC3InitDebit, T3.FC4InitCredit, T3.FC4InitDebit, T3.FC5InitCredit, T3.FC5InitDebit, 
		T3.FC1PeriodCredit, T3.FC1PeriodDebit, T3.FC2PeriodCredit, T3.FC2PeriodDebit, T3.FC3PeriodCredit, T3.FC3PeriodDebit, T3.FC4PeriodCredit, T3.FC4PeriodDebit, T3.FC5PeriodCredit, T3.FC5PeriodDebit, 
		T3.FC1YearCredit, T3.FC1YearDebit, T3.FC2YearCredit, T3.FC2YearDebit, T3.FC3YearCredit, T3.FC3YearDebit, T3.FC4YearCredit, T3.FC4YearDebit, T3.FC5YearCredit, T3.FC5YearDebit		
		FROM ( ';
    IF :postFlag = 'N' THEN 
        queryStr1 := :queryStr1 || ' SELECT DISTINCT T0."AcctCode", T0."AcctName", T0."AccntntCod", T0."FrgnName", T0."Levels", T0."FatherNum", T0."Postable", T0."GroupMask", T0."GrpLine", T0."BalDirect" 
        FROM "CRSPTrailBalanceCNAcctLimitted" As T0 ';
    ELSE 
        queryStr1 := :queryStr1 || '
		SELECT T0."AcctCode", T0."AcctName", T0."AccntntCod", T0."FrgnName", T0."Levels", T0."FatherNum", T0."Postable", T0."GroupMask", T0."GrpLine", T0."BalDirect", 1 AS VirtualGroup
		FROM OACT AS T0
		WHERE T0."AcctCode" IN ( ' || :acctCodeListParam || ' ) ';
    END IF;
    queryStr1 := :queryStr1 || ' ) AS T4 LEFT OUTER JOIN ( ';
    IF :voucherFlag = 'Y' THEN 
        queryStr1 := :queryStr1 || '        
		SELECT "AcctCode",SUM(InitCredit) AS InitCredit, SUM(InitDebit) AS InitDebit, SUM(CurrentCredit) AS CurrentCredit, SUM(CurrentDebit) AS CurrentDebit, SUM(YearCredit) AS YearCredit, SUM(YearDebit) AS YearDebit, SUM(LCCredit) AS LCCredit, SUM(LCDebit) AS LCDebit, 
		SUM(FC1Credit) AS FC1Credit, SUM(FC1Debit) AS FC1Debit, SUM(FC2Credit) AS FC2Credit, SUM(FC2Debit) AS FC2Debit, SUM(FC3Credit) AS FC3Credit, SUM(FC3Debit) AS FC3Debit, SUM(FC4Credit) AS FC4Credit, SUM(FC4Debit) AS FC4Debit, SUM(FC5Credit) AS FC5Credit, SUM(FC5Debit) AS FC5Debit,
		SUM(LCInitCredit) AS LCInitCredit, SUM(LCInitDebit) AS LCInitDebit,SUM(LCPeriodCredit) AS LCPeriodCredit, SUM(LCPeriodDebit) AS LCPeriodDebit,SUM(LCYearCredit) AS LCYearCredit, SUM(LCYearDebit) AS LCYearDebit,
		SUM(FC1InitCredit) AS FC1InitCredit, SUM(FC1InitDebit) AS FC1InitDebit, SUM(FC2InitCredit) AS FC2InitCredit, SUM(FC2InitDebit) AS FC2InitDebit, SUM(FC3InitCredit) AS FC3InitCredit, SUM(FC3InitDebit) AS FC3InitDebit, SUM(FC4InitCredit) AS FC4InitCredit, SUM(FC4InitDebit) AS FC4InitDebit, SUM(FC5InitCredit) AS FC5InitCredit, SUM(FC5InitDebit) AS FC5InitDebit, 
		SUM(FC1PeriodCredit) AS FC1PeriodCredit, SUM(FC1PeriodDebit) AS FC1PeriodDebit, SUM(FC2PeriodCredit) AS FC2PeriodCredit, SUM(FC2PeriodDebit) AS FC2PeriodDebit, SUM(FC3PeriodCredit) AS FC3PeriodCredit, SUM(FC3PeriodDebit) AS FC3PeriodDebit, SUM(FC4PeriodCredit) AS FC4PeriodCredit, SUM(FC4Debit) AS FC4PeriodDebit, SUM(FC5PeriodCredit) AS FC5PeriodCredit, SUM(FC5PeriodDebit) AS FC5PeriodDebit, 
		SUM(FC1YearCredit) AS FC1YearCredit, SUM(FC1YearDebit) AS FC1YearDebit, SUM(FC2YearCredit) AS FC2YearCredit, SUM(FC2YearDebit) AS FC2YearDebit, SUM(FC3YearCredit) AS FC3YearCredit, SUM(FC3YearDebit) AS FC3YearDebit, SUM(FC4YearCredit) AS FC4YearCredit, SUM(FC4YearDebit) AS FC4YearDebit, SUM(FC5YearCredit) AS FC5YearCredit, SUM(FC5YearDebit) AS FC5YearDebit
		FROM ( ';
    END IF;  

    queryStr2 := 'SELECT T0."AcctCode", SUM( ' || :initCase || :sufCreditCase || ') AS InitCredit, 
		SUM( ' || :initCase || :sufDebitCase || ') AS InitDebit, 
		SUM( ' || :periodCase || :sufCreditCase || ') AS CurrentCredit, 
		SUM( ' || :periodCase || :sufDebitCase || ') AS CurrentDebit, 
		SUM( ' || :yearCase || :sufCreditCase || ') AS YearCredit, 
		SUM( ' || :yearCase || :sufDebitCase || ') AS YearDebit, 
		SUM( ' || :lcCurrencyStr || :sufCreditCase || ') AS LCCredit, 
		SUM( ' || :lcCurrencyStr || :sufDebitCase || ') AS LCDebit,
		SUM( ' || :lcCurrencyStr || :initCondition || :sufCreditCase || ') AS LCInitCredit, 
		SUM( ' || :lcCurrencyStr || :initCondition || :sufDebitCase || ') AS LCInitDebit,
		SUM( ' || :lcCurrencyStr || :periodCondition || :sufCreditCase || ') AS LCPeriodCredit, 
		SUM( ' || :lcCurrencyStr || :periodCondition || :sufDebitCase || ') AS LCPeriodDebit,
		SUM( ' || :lcCurrencyStr || :yearCondition || :sufCreditCase || ') AS LCYearCredit, 
		SUM( ' || :lcCurrencyStr || :yearCondition || :sufDebitCase || ') AS LCYearDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :sufFCCreditCase || ') AS FC1Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :sufFCDebitCase || ') AS FC1Debit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :sufFCCreditCase || ') AS FC2Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :sufFCDebitCase || ') AS FC2Debit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :sufFCCreditCase || ') AS FC3Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :sufFCDebitCase || ') AS FC3Debit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :sufFCCreditCase || ') AS FC4Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :sufFCDebitCase || ') AS FC4Debit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :sufFCCreditCase || ') AS FC5Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :sufFCDebitCase || ') AS FC5Debit,';

	queryStr3 := 'SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :initCondition || :sufFCCreditCase || ') AS FC1InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :initCondition || :sufFCDebitCase || ') AS FC1InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :initCondition || :sufFCCreditCase || ') AS FC2InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :initCondition || :sufFCDebitCase || ') AS FC2InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :initCondition || :sufFCCreditCase || ') AS FC3InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :initCondition || :sufFCDebitCase || ') AS FC3InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :initCondition || :sufFCCreditCase || ') AS FC4InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :initCondition || :sufFCDebitCase || ') AS FC4InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :initCondition || :sufFCCreditCase || ') AS FC5InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :initCondition || :sufFCDebitCase || ') AS FC5InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC1PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC1PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC2PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC2PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC3PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC3PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC4PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC4PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC5PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC5PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC1YearCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC1YearDebit,';		
		 
	queryStr4 := 'SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC2YearCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC2YearDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC3YearCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC3YearDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC4YearCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC4YearDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC5YearCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC5YearDebit 
		FROM OACT AS T0 
		LEFT OUTER JOIN JDT1 T1 
		ON T1."Account" = T0."AcctCode" 
		LEFT OUTER JOIN OJDT T2 
		ON T2."TransId" = T1."TransId" ';				
    queryStr4 := :queryStr4 || :whereStr || ' GROUP BY T0."AcctCode" ';

    IF :voucherFlag = 'Y' THEN 
        queryStr5 := '
		UNION ALL
		SELECT T0."AcctCode", SUM( ' || :initCase || :sufCreditCase || ') AS InitCredit, 
		SUM( ' || :initCase || :sufDebitCase || ') AS InitDebit, SUM( ' || :periodCase || :sufCreditCase || ') AS CurrentCredit, 
		SUM( ' || :periodCase || :sufDebitCase || ') AS CurrentDebit, 
		SUM( ' || :yearCase || :sufCreditCase || ') AS YearCredit, 
		SUM( ' || :yearCase || :sufDebitCase || ') AS YearDebit, 
		SUM( ' || :lcCurrencyStr || :sufCreditCase || ') AS LCCredit, 
		SUM( ' || :lcCurrencyStr || :sufDebitCase || ') AS LCDebit, 
		SUM( ' || :lcCurrencyStr || :initCondition || :sufCreditCase || ') AS LCInitCredit, 
		SUM( ' || :lcCurrencyStr || :initCondition || :sufDebitCase || ') AS LCInitDebit,
		SUM( ' || :lcCurrencyStr || :periodCondition || :sufCreditCase || ') AS LCPeriodCredit, 
		SUM( ' || :lcCurrencyStr || :periodCondition || :sufDebitCase || ') AS LCPeriodDebit,
		SUM( ' || :lcCurrencyStr || :yearCondition || :sufCreditCase || ') AS LCYearCredit, 
		SUM( ' || :lcCurrencyStr || :yearCondition || :sufDebitCase || ') AS LCYearDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :sufFCCreditCase || ') AS FC1Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :sufFCDebitCase || ') AS FC1Debit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :sufFCCreditCase || ') AS FC2Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :sufFCDebitCase || ') AS FC2Debit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :sufFCCreditCase || ') AS FC3Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :sufFCDebitCase || ') AS FC3Debit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :sufFCCreditCase || ') AS FC4Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :sufFCDebitCase || ') AS FC4Debit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :sufFCCreditCase || ') AS FC5Credit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :sufFCDebitCase || ') AS FC5Debit,';
				
		queryStr6 := 'SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :initCondition || :sufFCCreditCase || ') AS FC1InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :initCondition || :sufFCDebitCase || ') AS FC1InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :initCondition || :sufFCCreditCase || ') AS FC2InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :initCondition || :sufFCDebitCase || ') AS FC2InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :initCondition || :sufFCCreditCase || ') AS FC3InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :initCondition || :sufFCDebitCase || ') AS FC3InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :initCondition || :sufFCCreditCase || ') AS FC4InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :initCondition || :sufFCDebitCase || ') AS FC4InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :initCondition || :sufFCCreditCase || ') AS FC5InitCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :initCondition || :sufFCDebitCase || ') AS FC5InitDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC1PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC1PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC2PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC2PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC3PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC3PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC4PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC4PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :periodCondition || :sufFCCreditCase || ') AS FC5PeriodCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :periodCondition || :sufFCDebitCase || ') AS FC5PeriodDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC1YearCredit,';
		 
		queryStr7 := 'SUM( ' || :fcCurrencyStr || '''' || :symbol1 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC1YearDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC2YearCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol2 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC2YearDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC3YearCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol3 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC3YearDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC4YearCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol4 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC4YearDebit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :yearCondition || :sufFCCreditCase || ') AS FC5YearCredit, 
		SUM( ' || :fcCurrencyStr || '''' || :symbol5 || '''' || :yearCondition || :sufFCDebitCase || ') AS FC5YearDebit 
		FROM OACT AS T0 
		LEFT OUTER JOIN BTF1 T1 
		ON T1."Account" = T0."AcctCode" 
		LEFT OUTER JOIN OBTF T2 
		ON T2."BatchNum" = T1."BatchNum" AND T2."TransId" = T1."TransId" ';
        queryStr7 := :queryStr7 || :whereStr || 'AND T2."BtfStatus" = ''O''' || ' GROUP BY T0."AcctCode") AS Q0 GROUP BY Q0."AcctCode"';
    END IF;
	
    queryStr7 := :queryStr7 || ') AS T3 ON T4."AcctCode" = T3."AcctCode" ORDER BY T4."GroupMask", T4."GrpLine"';
    
	execute immediate( :queryStr1 || :queryStr2 ||:queryStr3|| :queryStr4 || :queryStr5 || :queryStr6 || :queryStr7);
	--select :queryStr1 || :queryStr2 ||:queryStr3|| :queryStr4 || :queryStr5 || :queryStr6 || :queryStr7 from dummy; 
END











