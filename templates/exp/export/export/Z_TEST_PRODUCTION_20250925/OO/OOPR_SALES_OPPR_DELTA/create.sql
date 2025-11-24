create procedure OOPR_SALES_OPPR_DELTA() language SQLSCRIPT as
begin

		delete from "ETL_SALES_OPPR" where "OPPORTUNITY_ID" IN (SELECT "OpprId" FROM "OOPR_SALES_OPPR_DELTA_TABLE");
		INSERT INTO "ETL_SALES_OPPR" 
			SELECT T0."OpprId" AS "OPPORTUNITY_ID", 
				IFNULL(T0."Name", '-No Name-') AS "OPPORTUNITY_NAME",
				(CASE T0."Status" WHEN 'O' THEN 'Open' WHEN 'W' THEN 'Won' ELSE 'Lost' END) AS "STATUS", 
				T0."CardCode",
				T0."SlpCode",
				T0."PredDate",
				IFNULL(T0."Territory", -2) as "TerritoryID",
				T2."Step_Id",
				T0."OpenDate",
				T0."CloseDate",
				IFNULL(T0."Industry", -1),
				IFNULL(T0."IntRate", -1),
				1 AS "COUNT",
				T0."MaxSumLoc" AS "POTENTIAL_AMOUNT", 
				T0."MaxSumSys" AS "POTENTIAL_AMOUNT_SC",
				T0."WtSumLoc" AS "WEIGHTED_AMOUNT",
				T0."WtSumSys" AS "WEIGHTED_AMOUNT_SC",
				T0."SumProfL" AS "WEIGHTED_AMOUNT",
				T0."SumProfS" AS "WEIGHTED_AMOUNT_SC",
				IFNULL(T0."PrjCode",'-') AS "Project"
			FROM "OOPR" T0
				INNER JOIN (SELECT "OpprId" AS "OPPRID", MAX("Line") AS "MAXLINE" 
							FROM "OPR1" 
							GROUP BY "OpprId") T1 ON T0."OpprId" = T1."OPPRID"
				INNER JOIN "OPR1" T2 ON T0."OpprId" = T2."OpprId" AND T1."MAXLINE" = T2."Line"
				INNER JOIN "OOPR_SALES_OPPR_DELTA_TABLE" TEMP ON TEMP."OpprId" = T0."OpprId"
				WHERE TEMP."OPT" IN ('I', 'U');
end
