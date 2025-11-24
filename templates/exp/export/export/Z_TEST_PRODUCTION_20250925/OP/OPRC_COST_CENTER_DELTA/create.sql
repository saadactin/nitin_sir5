CREATE PROCEDURE OPRC_COST_CENTER_DELTA()
AS
BEGIN
		DELETE FROM "ETL_COST_CENTER" WHERE "PrcCode" IN (SELECT "PrcCode" FROM OPRC_COST_CENTER_DELTA_TABLE WHERE OPT = 'U');
		INSERT INTO "ETL_COST_CENTER"
			SELECT T2."OcrCode", T2."PrcCode", T4."DimCode", T4."DimDesc", T2."OcrCode"||'('||IFNULL(T2."OcrName",'-')||')', 
				IFNULL(T3."GrpCode", '-No Sort Code-') AS "SORTCODE",
				(CASE T2."Direct" WHEN 'Y' THEN 'Direct Allocation' ELSE 'Indirect Allocation' END),
				T2."PrcCode"||'('||IFNULL(T3."PrcName",'-')||')', T2."ValidFrom"
			FROM (
				SELECT T0."PrcCode", T0."OcrCode", T1."OcrName", T1."DimCode", T1."Direct",TO_NVARCHAR(T0."ValidFrom", 'YYYYMMDD') AS "ValidFrom"
				FROM "OCR1" T0
				INNER JOIN "OOCR" T1 ON T0."OcrCode"=T1."OcrCode" 
				UNION ALL
				SELECT T0."PrcCode", T0."OcrCode", T1."OcrName", T1."DimCode", T1."Direct",TO_NVARCHAR(T0."ValidFrom", 'YYYYMMDD') AS "ValidFrom"
				FROM "MDR1" T0
				INNER JOIN "OMDR" T1 ON T0."OcrCode"=T1."OcrCode") T2
			INNER JOIN "OPRC" T3 ON T2."PrcCode"=T3."PrcCode"
			INNER JOIN "ODIM" T4 ON T2."DimCode"=T4."DimCode"
			INNER JOIN OPRC_COST_CENTER_DELTA_TABLE TEMP ON TEMP."PrcCode" = T3."PrcCode"
			WHERE T4."DimActive"='Y' AND TEMP."OPT" = 'U';
			
		
		DELETE FROM "ETL_BUDGET_COSTCENTER" WHERE "CODE" IN (SELECT "PrcCode" FROM OPRC_COST_CENTER_DELTA_TABLE WHERE OPT = 'U');
		INSERT INTO "ETL_BUDGET_COSTCENTER"("CODE", "NAME", "COST_CENTER","SORT_CODE", "DIM_CODE", "DIMENSION")
		SELECT T0."PrcCode", T0."PrcName", IFNULL(T0."PrcName",'')||'('||T0."PrcCode"||')',IFNULL(T0."GrpCode",'-'),
		T0."DimCode", IFNULL(T1."DimDesc",'')||'('||T1."DimName"||')'
		FROM OPRC T0  INNER JOIN ODIM T1 ON T0."DimCode" = T1."DimCode"
		INNER JOIN OPRC_COST_CENTER_DELTA_TABLE TEMP ON TEMP."PrcCode" = T0."PrcCode"
		WHERE TEMP."OPT" = 'U';
END
