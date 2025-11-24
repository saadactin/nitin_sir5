CREATE VIEW "Z_TEST_PRODUCTION_20250925"."B1_EDFLatestLogsView" ( "AbsEntry", "LogNum", "LogMessage", "UserSign", "CreateDate", "UserSign2", "UpdateDate", "LogInstanc", "CreateTS", "UpdateTS", "LogType", "LogData", "LogOpDate", "LogOpTS", "RankByDT" ) AS SELECT
	 "TBLRANK"."AbsEntry" ,
	 "TBLRANK"."LogNum" ,
	 "TBLRANK"."LogMessage" ,
	 "TBLRANK"."UserSign" ,
	 "TBLRANK"."CreateDate" ,
	 "TBLRANK"."UserSign2" ,
	 "TBLRANK"."UpdateDate" ,
	 "TBLRANK"."LogInstanc" ,
	 "TBLRANK"."CreateTS" ,
	 "TBLRANK"."UpdateTS" ,
	 "TBLRANK"."LogType" ,
	 "TBLRANK"."LogData" ,
	 "TBLRANK"."LogOpDate" ,
	 "TBLRANK"."LogOpTS" ,
	 "TBLRANK"."RankByDT" 
FROM (SELECT
	 *,
	 CAST(RANK() OVER (PARTITION BY "AbsEntry",
	 "LogType" 
			ORDER BY "LogOpDate" DESC,
	 "LogOpTS" DESC,
	 "LogNum" DESC) AS Integer) AS "RankByDT" 
	FROM "ECM3" ) tblRank 
WHERE tblRank."RankByDT" = 1 ORDER BY "AbsEntry",
	 "LogType" WITH READ ONLY