CREATE PROCEDURE OACT_GL_ACCOUNT_DELTA()
AS
ActSep CHAR(1);
EnbSgmnAct CHAR(1);
BEGIN
	
		DELETE FROM "ETL_GL_ACCOUNT" WHERE "ACCTCODE" IN (SELECT "AcctCode" FROM OACT_GL_ACCOUNT_DELTA_TABLE);
		SELECT "ActSep" INTO ActSep FROM OADM;
		SELECT "EnbSgmnAct" INTO EnbSgmnAct FROM CINF;
		INSERT INTO "ETL_GL_ACCOUNT"	
			SELECT
				T0."AcctCode" AS "ACCTCODE",
				CASE WHEN T1."AcctCode" IS NULL
					THEN NULL
				WHEN T1."FatherNum" IS NULL
					THEN T1."AcctCode"||'('||T1."AcctName"||')'
				WHEN :EnbSgmnAct = 'Y'
					THEN IFNULL(T1."Segment_0"||IFNULL(:ActSep||T1."Segment_1",'')
					||IFNULL(:ActSep||T1."Segment_2",'')||IFNULL(:ActSep||T1."Segment_3",'')
					||IFNULL(:ActSep||T1."Segment_4",'')||IFNULL(:ActSep||T1."Segment_5",'')
					||IFNULL(:ActSep||T1."Segment_6",'')||IFNULL(:ActSep||T1."Segment_7",'')
					||IFNULL(:ActSep||T1."Segment_8",'')||IFNULL(:ActSep||T1."Segment_9",''),T1."AcctCode")||'('||IFNULL(T1."AcctName",'')||')'
				ELSE T1."AcctCode"||'('||T1."AcctName"||')' 
				END AS "FATHERACCOUNT",
				CASE WHEN T0."FatherNum" IS NULL
					THEN T0."AcctCode"||'('||T0."AcctName"||')'
				WHEN :EnbSgmnAct = 'Y' AND T0."Postable" = 'Y'
					THEN IFNULL(T0."Segment_0"||IFNULL(:ActSep||T0."Segment_1",'')
					||IFNULL(:ActSep||T0."Segment_2",'')||IFNULL(:ActSep||T0."Segment_3",'')
					||IFNULL(:ActSep||T0."Segment_4",'')||IFNULL(:ActSep||T0."Segment_5",'')
					||IFNULL(:ActSep||T0."Segment_6",'')||IFNULL(:ActSep||T0."Segment_7",'')
					||IFNULL(:ActSep||T0."Segment_8",'')||IFNULL(:ActSep||T0."Segment_9",''),T0."AcctCode")||'('||IFNULL(T0."AcctName",'')||')'
				WHEN :EnbSgmnAct = 'Y' AND T0."Postable" = 'N'
					THEN T0."AcctCode"||'('||IFNULL(T0."AcctName",'')||')'
				ELSE T0."AcctCode"||'('||IFNULL(T0."AcctName",'')||')' 
				END AS "GL_ACCOUNT",
				IFNULL(T0."FormatCode", '-No FormatCode-') AS "FORMATCODE",
				IFNULL(T0."Segment_0", '-No Segment0-') AS "SEGMENT0",
				IFNULL(T0."Segment_1", '-No Segment1-') AS "SEGMENT1",
				IFNULL(T0."Segment_2", '-No Segment2-') AS "SEGMENT2",
				IFNULL(T0."Segment_3", '-No Segment3-') AS "SEGMENT3",
				IFNULL(T0."Segment_4", '-No Segment4-') AS "SEGMENT4",
				IFNULL(T0."Segment_5", '-No Segment5-') AS "SEGMENT5",
				IFNULL(T0."Segment_6", '-No Segment6-') AS "SEGMENT6",
				IFNULL(T0."Segment_7", '-No Segment7-') AS "SEGMENT7",
				IFNULL(T0."Segment_8", '-No Segment8-') AS "SEGMENT8",
				IFNULL(T0."Segment_9", '-No Segment9-') AS "SEGMENT9"
			FROM OACT T0
			INNER JOIN OACT_GL_ACCOUNT_DELTA_TABLE TEMP ON TEMP."AcctCode" = T0."AcctCode"
			LEFT OUTER JOIN OACT T1 ON T0."FatherNum"=T1."AcctCode"
			WHERE TEMP.OPT IN ('I', 'U');
		
END
