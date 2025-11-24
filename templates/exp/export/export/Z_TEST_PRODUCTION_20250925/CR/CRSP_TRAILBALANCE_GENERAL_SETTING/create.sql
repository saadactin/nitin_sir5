-- B1 DEPENDS: AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_TrailBalance_General_Setting ()
--out beginDate datetime, out endDate datetime, out symbol nvarchar(10), out symbol1 nvarchar(10),out symbol2 nvarchar(10), out symbol3 nvarchar(10),
--out symbol4 nvarchar(10), out symbol5 nvarchar(10), out LogoFile NVARCHAR(200), out LogoImage BLOB, out DecSep NVARCHAR(1), out DateSep NVARCHAR(1),) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
as
loopParm int;
curFCCurrency nvarchar(3);
symbol nvarchar(10);
symbol1 nvarchar(10) :='1';
symbol2 nvarchar(10) :='2';
symbol3 nvarchar(10) :='3';
symbol4 nvarchar(10) :='4';
symbol5 nvarchar(10) :='5';
beginDate date;
endDate date;
CURSOR curTemp FOR SELECT DISTINCT "FCCurrency" FROM JDT1 WHERE "FCCurrency" <> '';
begin
	SELECT MIN("F_RefDate"),MAX("T_RefDate") into beginDate,endDate FROM OFPR;
	loopParm := 1;

	For cur_row as curTemp Do 
		symbol := cur_row."FCCurrency";

		IF :loopParm = 1 then 
		 	symbol1 := symbol;
		ELSEIF :loopParm = 2 then 
			symbol2 := symbol;
		ELSEIF :loopParm = 3 then 
			symbol3 := symbol;
		ELSEIF loopParm = 4 then 
			symbol4 := symbol;
		ELSE
		 	symbol5 := symbol;
		End IF;
		loopParm := loopParm + 1;
	End For;
	
	SELECT :beginDate AS BeginDate, :endDate AS EndDate, :symbol1 AS symbol1, :symbol2 AS symbol2, :symbol3 AS symbol3, :symbol4 AS symbol4, :symbol5 AS symbol5, 
	T0."LogoFile", T0."LogoImage",T1."DecSep", T1."DateSep", T1."TimeFormat", T1."DateFormat", T1."ThousSep", T1."CurOnRight", T1."CompnyName", T1."MainCurncy", T1."SumDec", T1."CharMonth" 
	FROM OADP T0, OADM T1;
END;











