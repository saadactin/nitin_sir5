CREATE VIEW "Z_TEST_PRODUCTION_20250925"."LAST12MONTHENDVIEW" ( "YEAR", "MONTH", "MONTHEND" ) AS Select Top 12 Year, Month, Max(Date_SQL) as MonthEnd
From _SYS_BI.M_TIME_DIMENSION 
Where Date_SAP >= Last_Day(Add_Months(Add_Days(CURRENT_DATE, 1-DayOfMonth(CURRENT_DATE)), -11))
and Date_Sql <= CURRENT_DATE 
Group By Year, month Order By Year Desc, Month Desc WITH READ ONLY