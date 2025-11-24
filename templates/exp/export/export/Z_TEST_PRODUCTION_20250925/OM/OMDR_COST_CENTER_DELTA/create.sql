CREATE PROCEDURE OMDR_COST_CENTER_DELTA()
AS
BEGIN
	DELETE FROM "ETL_COST_CENTER" T1 WHERE EXISTS (SELECT 1 FROM "OMDR_COST_CENTER_DELTA_TABLE" TT WHERE T1."OcrCode"=TT."OcrCode" AND TT.OPT='U');
	INSERT INTO "ETL_COST_CENTER"
		SELECT T2."OcrCode", T2."PrcCode", T4."DimCode", T4."DimDesc", T2."OcrCode"||'('||IFNULL(T2."OcrName",'-')||')', 
			IFNULL(T3."GrpCode", '-No Sort Code-') AS "SORTCODE",
			(CASE T2."Direct" WHEN 'Y' THEN 'Direct Allocation' ELSE 'Indirect Allocation' END),
			T2."PrcCode"||'('||IFNULL(T3."PrcName",'-')||')', T2."ValidFrom"
		FROM (
			SELECT T0."PrcCode", T0."OcrCode", T1."OcrName", T1."DimCode", T1."Direct", TO_NVARCHAR(T0."ValidFrom", 'YYYYMMDD') AS "ValidFrom"
			FROM "MDR1" T0
			INNER JOIN "OMDR" T1 ON T0."OcrCode"=T1."OcrCode") T2
		INNER JOIN "OPRC" T3 ON T2."PrcCode"=T3."PrcCode"
		INNER JOIN "ODIM" T4 ON T2."DimCode"=T4."DimCode"
		INNER JOIN "OMDR_COST_CENTER_DELTA_TABLE" TT ON T2."OcrCode"=TT."OcrCode" AND TT.OPT='U'
		WHERE T4."DimActive"='Y';

END
