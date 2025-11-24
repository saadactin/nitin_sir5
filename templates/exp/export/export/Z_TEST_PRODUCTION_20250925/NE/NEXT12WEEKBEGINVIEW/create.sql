CREATE VIEW "Z_TEST_PRODUCTION_20250925"."NEXT12WEEKBEGINVIEW" ( "WEEKBEGIN" ) AS Select Max(Date_Sql) as WeekBegin
From "_SYS_BI"."M_TIME_DIMENSION" 
Where Date_Sql >= CURRENT_DATE And Date_Sql <= Add_Days(CURRENT_DATE, 7*11 + 6 - WeekDay(CURRENT_DATE))
Group By Week_Year, Week Order By Week_Year, Max(Date_Sql) WITH READ ONLY