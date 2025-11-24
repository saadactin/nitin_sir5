CREATE PROCEDURE MDR1_COST_CENTER_DELTA()
AS
COUNTER  Integer := 0;
BEGIN
	DELETE FROM "ETL_COST_CENTER" T1 WHERE EXISTS (SELECT 1 FROM "MDR1_COST_CENTER_DELTA_TABLE" TT WHERE T1."OcrCode"=TT."OcrCode" AND T1."PrcCode"=TT."PrcCode");
	INSERT INTO "ETL_COST_CENTER" 
		SELECT T2."OcrCode", T2."PrcCode", T4."DimCode", T4."DimDesc", T2."OcrCode"||'('||IFNULL(T2."OcrName",'-')||')', 
			IFNULL(T3."GrpCode", '-No Sort Code-') AS "SORTCODE",
			(CASE T2."Direct" WHEN 'Y' THEN 'Direct Allocation' ELSE 'Indirect Allocation' END),
			T2."PrcCode"||'('||IFNULL(T3."PrcName",'-')||')', T2."ValidFrom"
		FROM (
			SELECT T0."PrcCode", T0."OcrCode", T1."OcrName", T1."DimCode", T1."Direct", TO_NVARCHAR(T0."ValidFrom", 'YYYYMMDD') AS "ValidFrom"
			FROM "MDR1" T0
			INNER JOIN "OMDR" T1 ON T0."OcrCode"=T1."OcrCode" 
			INNER JOIN "MDR1_COST_CENTER_DELTA_TABLE" TT ON T0."OcrCode"=TT."OcrCode" AND T0."PrcCode"=TT."PrcCode" AND T0."ValidFrom" = TT."ValidFrom" AND TT.OPT IN ('I','U')) T2
		INNER JOIN "OPRC" T3 ON T2."PrcCode"=T3."PrcCode"
		INNER JOIN "ODIM" T4 ON T2."DimCode"=T4."DimCode"
		WHERE T4."DimActive"='Y';
		
	--SELECT COUNT(*) INTO COUNTER FROM "ETL_BUDGET_COSTCENTER" T1, "MDR1_COST_CENTER_DELTA_TABLE" TT WHERE T1."CODE" = TT."PrcCode" AND TT.OPT='I';
	--IF COUNTER = 0 THEN
		DELETE FROM "ETL_BUDGET_COSTCENTER" WHERE "CODE" IN (SELECT DISTINCT "PrcCode" FROM MDR1_COST_CENTER_DELTA_TABLE);
		INSERT INTO "ETL_BUDGET_COSTCENTER"("CODE", "NAME", "COST_CENTER", "SORT_CODE", "DIM_CODE","DIMENSION")
		SELECT T0."PrcCode", T0."PrcName", IFNULL(T0."PrcName",'')||'('||T0."PrcCode"||')', 
				IFNULL(T0."GrpCode",'-'),T1."DimCode", IFNULL(T1."DimDesc",'')||'('||T1."DimName"||')' FROM OPRC T0
		INNER JOIN ODIM T1 ON T1."DimCode" = T0."DimCode"
		INNER JOIN "MDR1_COST_CENTER_DELTA_TABLE" TT ON T0."PrcCode" = TT."PrcCode";
	--END IF;
END
