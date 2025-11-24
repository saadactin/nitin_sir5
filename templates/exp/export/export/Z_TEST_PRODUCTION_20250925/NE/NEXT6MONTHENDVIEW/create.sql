CREATE VIEW "Z_TEST_PRODUCTION_20250925"."NEXT6MONTHENDVIEW" ( "YEAR", "MONTH", "MONTHEND" ) AS Select Top 6 Year, Month, Max(Date_SQL) as MonthEnd
From _SYS_BI.M_TIME_DIMENSION 
Where Date_SAP <= Last_Day(Add_Months(Add_Days(CURRENT_DATE, 1-DayOfMonth(CURRENT_DATE)), 6))
and Date_Sql >Last_Day(CURRENT_DATE) 
Group By Year , Month Order By Year Desc, Month Desc WITH READ ONLY