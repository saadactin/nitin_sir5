-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:PT:PROCESS_END AFTER:SP:_TmSp_ValidateSpParam

Create Procedure CRSP_OPEN_ITEM_LIST(
	In DateType nvarchar(1),
	In DateValue NVarChar(8),
	In ReconDateValue NVarChar(8),
	IN custs NCLOB,
	IN IncludeDPMRequestIn nvarchar(1) -- Bollean parameter from CR, '1' means True, '0' means False.
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
As
retCount integer;
BEGIN

DECLARE IsExists INTEGER;

call _TmSp_ValidateSpParam (:DateType);
call _TmSp_ValidateSpParam (:DateValue);
call _TmSp_ValidateSpParam (:ReconDateValue);
call _TmSp_ValidateSpParam (:IncludeDPMRequestIn);

SELECT COUNT(*) INTO IsExists 
  FROM M_TEMPORARY_TABLES
  WHERE TABLE_NAME = '#CRSP_TEMP_OCRD_COL' AND SCHEMA_NAME = CURRENT_SCHEMA and connection_id = current_connection;
  
IF :IsExists > 0 THEN
    DROP TABLE #CRSP_TEMP_OCRD_COL;
END IF;
  

create LOCAL TEMPORARY column table #CRSP_TEMP_OCRD_COL( 
    "CardCode" NVARCHAR(15) ,
    "CardName" NVARCHAR(100) ,
    "GroupCode" SMALLINT ,
    "Balance" DECIMAL(21,6)  
); 


exec ('INSERT INTO #CRSP_TEMP_OCRD_COL SELECT T0."CardCode", T0."CardName", T0."GroupCode", T0."Balance" FROM OCRD T0 WHERE ' || :custs );


JOURNALSOURCE= SELECT
     T1."CardCode" AS "WHERE_CARD",
     T0."DocEntry",
     T0."InstlmntID",
     T1."Installmnt",
     T0."DunDate",
     T0."ObjType",
     T1."CardCode",
     T1."CardName",
     T1."NumAtCard",
     T1."SlpCode",
     T0."InsTotal",
     T0."InsTotalFC",
     T0."InsTotalSy",
     T1."BlockDunn",
     T0."DunnLevel",
     N'I' AS "TransType",
     N'Y' AS "IsSales",
     N'Y' AS "IsDunnable" ,
     0 AS "BoeNum",
     N'' AS "BoeStatus" 
FROM "INV6" T0 
INNER JOIN "OINV" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN #CRSP_TEMP_OCRD_COL T2 On T1."CardCode" = T2."CardCode"
UNION ALL SELECT
     T1."CardCode" AS "WHERE_CARD",
     T0."DocEntry",
     T0."InstlmntID",
     T1."Installmnt",
     T0."DunDate",
     T0."ObjType",
     T1."CardCode",
     T1."CardName",
     T1."NumAtCard",
     T1."SlpCode",
     T0."InsTotal",
     T0."InsTotalFC",
     T0."InsTotalSy",
     T1."BlockDunn",
     T0."DunnLevel",
     N'I' AS "TransType",
     N'Y' AS "IsSales",
     N'N' AS "IsDunnable" ,
     0 AS "BoeNum",
     N'' AS "BoeStatus" 
FROM "RIN6" T0 
INNER JOIN "ORIN" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN #CRSP_TEMP_OCRD_COL T2 On T1."CardCode" = T2."CardCode" 
UNION ALL SELECT
     T1."CardCode" AS "WHERE_CARD",
     T0."DocEntry",
     T0."InstlmntID",
     T1."Installmnt",
     T0."DunDate",
     T0."ObjType",
     T1."CardCode",
     T1."CardName",
     T1."NumAtCard",
     T1."SlpCode",
     T0."InsTotal",
     T0."InsTotalFC",
     T0."InsTotalSy",
     T1."BlockDunn",
     T0."DunnLevel",
     N'I' AS "TransType",
     N'Y' AS "IsSales",
     N'Y' AS "IsDunnable" ,
     0 AS "BoeNum",
     N'' AS "BoeStatus" 
FROM "DPI6" T0 
INNER JOIN "ODPI" T1 ON T1."DocEntry" = T0."DocEntry" 
INNER JOIN #CRSP_TEMP_OCRD_COL T2 On T1."CardCode" = T2."CardCode"
UNION ALL SELECT
     '-1' AS "WHERE_CARD",
     T0."DocEntry",
     0 AS "InstlmntID",
     0 AS "Installmnt",
     NULL AS "DunDate",
    T0."ObjType",
     T0."CardCode",
     T0."CardName",
     N'' AS "NumAtCard",
     -1 AS "SlpCode",
     T0."DocTotal" AS "InsTotal",
     T0."DocTotalFC" AS "InsTotalFC",
     T0."DocTotalSy" AS "InsTotalSy",
     N'Y' AS "BlockDunn",
     0 AS "DunnLevel",
     N'P' AS "SrcType",
     N'' AS "IsSales",
     N'N' AS "IsDunnable",
     0 AS "BoeNum",
     N'' AS "BoeStatus" 
FROM "ORCT" T0 
UNION ALL SELECT
     '-1' AS "WHERE_CARD",
     T0."DeposId",
     0 AS "InstlmntID",
     0 AS "Installmnt",
     NULL AS "DunDate",
     T0."ObjType",
     N'' AS "CardCode",
     N'' AS "CardName",
     N'' AS "NumAtCard",
     -1 AS "SlpCode",
     T0."LocTotal" AS "InsTotal" ,
     T0."FcTotal" AS "InsTotalFC" ,
     T0."SysTotal" AS "InsTotalSy",
     N'Y' AS "BlockDunn",
     0 AS "DunnLevel",
     N'D' AS "SrcType",
     N'' AS "IsSales",
     N'N' AS "IsDunnable",
     T1."BoeNum",
     T1."BoeStatus" 
FROM "ODPS" T0 
LEFT OUTER JOIN "OBOE" T1 ON T1."BoeKey" = T0."DeposId"
;

IF :DateType = '1' 
THEN PostedTrans = SELECT
     T0."DocEntry",
     T0."TransId",
     T0."Line_ID",
     T0."InstlmntID",
     T0."ShortName",
     T0."Address",
     T0."Phone",
     T0."TransType",
     T0."CreatedBy",
     T0."BaseRef",
     T0."Debit",
     T0."Credit",
     T0."FCDebit",
     T0."FCCredit",
     T0."DebCred",
     T0."RefDate",
     T0."DueDate",
     T0."TaxDate",
     T0."BalDueCred",
     T0."BalFCCred",
     T0."LineMemo" ,
     T0."CardName",
     T0."CardCode",
     T0."DunnLevel",
     T0."BPCurrency",
     T0."JEFCCurrency",
     T0."JEFCCurrency" as "FCCurrency",
     T0."Installmnt",
     T0."DunDate",
     T0."InsTotal",
     T0."InsTotalFC" ,
     T0."InsTotalSy",
     T1."SrcObjTyp",
     T1."SrcObjAbs",
     T1."IsCredit",
     T2."ReconDate",
     T2."ReconCurr",
     T4."ReconBaseRef",
     T4."ReconRefDate",
     T4."ReconDocumentDate",
     T5."ReconJrnlMemo",
     T5."ReconDebCred",
     T5."ReconDebit",
     T5."ReconCredit",
     T5."ReconDebitFc",
     T5."ReconCreditFC" ,
     T5."ReconJEFCCurrency",
     CURRENT_DATE AS "ServerDate",
    T3."Name" AS "IndicatorName" ,
     (CASE WHEN T2."ReconDate" IS NULL 
    THEN 0 
    ELSE T1."ReconSum" 
    END ) AS "realReconSum" ,
     (CASE WHEN T2."ReconDate" IS NULL 
    THEN 0 
    ELSE T1."ReconSumFC" 
    END ) AS "realReconSumFC" ,
    'N' As "IsDPMRequest" 
FROM ( SELECT
     MAX(T5."DocEntry") AS "DocEntry",
    T0."TransId",
     T0."Line_ID",
     MAX("InstlmntID") AS "InstlmntID",
     MAX(T0."Account") AS "Account",
     MAX(T0."ShortName") AS "ShortName",
     MAX(T4."Address") AS "Address",
     MAX(T4."Phone1") AS "Phone",
     MAX(T0."TransType") AS "TransType",
     MAX(T0."CreatedBy") AS "CreatedBy",
     MAX(T0."BaseRef") AS "BaseRef",
     MAX(T0."SourceLine") AS "SourceLine",
     MAX(T0."Debit") AS "Debit",
     MAX(T0."Credit") AS "Credit",
     MAX(T0."FCDebit") AS "FCDebit",
     MAX(T0."FCCredit") AS "FCCredit",
    MAX(T0."DebCred") AS "DebCred",
     MAX(T0."RefDate") AS "RefDate" ,
     MAX(T0."DueDate") AS "DueDate",
     MAX(T0."TaxDate") AS "TaxDate",
     (MAX(T0."BalDueCred") + SUM(T1."ReconSum")) AS "BalDueCred",
     (MAX(T0."BalFcCred") + SUM(T1."ReconSumFC")) AS "BalFCCred",
    ( MAX(T0."BalScCred") + SUM(T1."ReconSumSC")) AS "BalScCred",
     MAX(T0."LineMemo") AS "LineMemo",
     MAX(T0."Indicator") AS "Indicator",
     MAX(T4."CardName") AS "CardName",
     MAX(T4."CardCode") AS "CardCode",
     MAX(T5."CardName") AS "CardName1",
     MAX(T4."Balance") AS "Balance",
     (SUM(T0."Debit") + SUM(T0."Credit")) AS "DocTotal",
     SUM(T0."FCDebit") + SUM(T0."FCCredit") AS "DocTotalFC",
     SUM(T0."SYSDeb") + SUM(T0."SYSCred") AS "DocTotalSC",
     MAX(T5."DunnLevel") AS "DunnLevel" ,
     MAX(T5."IsSales") AS "IsSales",
     MAX(T4."Currency") AS "BPCurrency",
     MAX(T0."FCCurrency") AS "JEFCCurrency" ,
     MAX(T5."Installmnt") AS "Installmnt",
     MAX(T5."DunDate") AS "DunDate",
     MAX(T5."InsTotal") AS "InsTotal",
     MAX(T5."InsTotalFC") AS "InsTotalFC",
     MAX(T5."InsTotalSy") AS "InsTotalSy" 
    FROM "JDT1" T0 
    INNER JOIN "ITR1" T1 ON T1."TransId" = T0."TransId" 
    AND T1."TransRowId" = T0."Line_ID" 
    INNER JOIN "OITR" T2 ON T2."ReconNum" = T1."ReconNum" 
    INNER JOIN "OJDT" T3 ON T3."TransId" = T0."TransId" 
    INNER JOIN "OCRD" T4 ON T4."CardCode" = T0."ShortName" 
    INNER JOIN #CRSP_TEMP_OCRD_COL T6 On T4."CardCode" = T6."CardCode"
       
    LEFT OUTER JOIN :JOURNALSOURCE T5 ON T5."ObjType" = T0."TransType" 
    AND T5."DocEntry" = T0."CreatedBy" 
    AND (T5."TransType" <> N'I' 
        OR T5."InstlmntID" = T0."SourceLine" ) 
    WHERE 
    --TO_CHAR(T0."RefDate", 
    --'YYYYMMDD') <= :DateValue
     T0."RefDate" <= to_timestamp(:DateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7')
    AND T4."CardType" = 'C' 
    --AND T4."Balance" <>0 
    --AND TO_CHAR(T2."ReconDate",    
    --'YYYYMMDD')>:ReconDateValue
    AND T2."ReconDate" > to_timestamp(:ReconDateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7') 
    AND T1."IsCredit" = 'C' 
    GROUP BY T0."TransId",
     T0."Line_ID" HAVING MAX(T0."BalFcCred") <>- SUM(T1."ReconSumFC") 
    OR MAX(T0."BalDueCred") <>- SUM(T1."ReconSum") 
    UNION ALL SELECT
     MAX(T5."DocEntry") AS "DocEntry",
    T0."TransId",
     T0."Line_ID",
     MAX("InstlmntID") AS "InstlmntID",
     MAX(T0."Account") AS "Account",
     MAX(T0."ShortName") AS "ShortName",
     MAX(T4."Address") AS "Address",
     MAX(T4."Phone1") AS "Phone",
    MAX(T0."TransType") AS "TransType",
     MAX(T0."CreatedBy") AS "CreatedBy",
     MAX(T0."BaseRef") AS "BaseRef",
     MAX(T0."SourceLine") AS "SourceLine",
     MAX(T0."Debit") AS "Debit",
     MAX(T0."Credit") AS "Credit",
     MAX(T0."FCDebit") AS "FCDebit",
     MAX(T0."FCCredit") AS "FCCredit",
    MAX(T0."DebCred") AS "DebCred",
     MAX(T0."RefDate") AS "RefDate",
     MAX(T0."DueDate") AS "DueDate",
     MAX(T0."TaxDate") AS "TaxDate",
     (- MAX(T0."BalDueDeb") - SUM(T1."ReconSum")) AS "BalDueCred",
     (- MAX(T0."BalFcDeb") - SUM(T1."ReconSumFC")) AS "BalFCCred",
     (- MAX(T0."BalScDeb") - SUM(T1."ReconSumSC")) AS "BalScCred",
     MAX(T0."LineMemo") AS "LineMemo",
     MAX(T0."Indicator") AS "Indicator",
     MAX(T4."CardName") AS "CardName",
     MAX(T4."CardCode") AS "CardCode",
     MAX(T5."CardName") AS "CardName1",
     MAX(T4."Balance") AS "Balance",
    (SUM(T0."Debit") + SUM(T0."Credit")) AS "DocTotal",
     (SUM(T0."FCDebit") + SUM(T0."FCCredit")) AS "DocTotalFC",
     (SUM(T0."SYSDeb") + SUM(T0."SYSCred")) AS "DocTotalSC",
     MAX(T5."DunnLevel") AS "DunnLevel",
     MAX(T5."IsSales") AS "IsSales",
     MAX(T4."Currency") AS "BPCurrency",
     MAX(T0."FCCurrency") AS "JEFCCurrency" ,
    MAX(T5."Installmnt") AS "Installmnt",
     MAX(T5."DunDate") AS "DunDate" ,
     MAX(T5."InsTotal") AS "InsTotal",
     MAX(T5."InsTotalFC") AS "InsTotalFC",
     MAX(T5."InsTotalSy") AS "InsTotalSy" 
    FROM "JDT1" T0 
    INNER JOIN "ITR1" T1 ON T1."TransId" = T0."TransId" 
    AND T1."TransRowId" = T0."Line_ID" 
    INNER JOIN "OITR" T2 ON T2."ReconNum" = T1."ReconNum" 
    INNER JOIN "OJDT" T3 ON T3."TransId" = T0."TransId" 
    INNER JOIN "OCRD" T4 ON T4."CardCode" = T0."ShortName" 
    INNER JOIN #CRSP_TEMP_OCRD_COL T6 On T4."CardCode" = T6."CardCode" 
    LEFT OUTER JOIN :JOURNALSOURCE T5 ON T5."ObjType" = T0."TransType" 
    AND T5."DocEntry" = T0."CreatedBy" 
    AND (T5."TransType" <> N'I' 
        OR T5."InstlmntID" = T0."SourceLine" ) 
    WHERE 
    --TO_CHAR(T0."RefDate",     'YYYYMMDD') <= :DateValue
    T0."RefDate" <= to_timestamp(:DateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7') 
    AND T4."CardType" = 'C' 
    --AND T4."Balance" <>0 
    --AND TO_CHAR(T2."ReconDate",    'YYYYMMDD') >:ReconDateValue
    AND T2."ReconDate" > to_timestamp(:ReconDateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7')
    AND T1."IsCredit" = 'D' 
    GROUP BY T0."TransId",
     T0."Line_ID" HAVING MAX(T0."BalFcDeb") <>- SUM(T1."ReconSumFC") 
    OR MAX(T0."BalDueDeb") <>- SUM(T1."ReconSum") 
    UNION ALL SELECT
     (CASE WHEN MAX(T3."DocEntry") IS NULL 
        THEN T0."TransId" 
        ELSE MAX(T3."DocEntry") 
        END ) AS "DocEntry",
     T0."TransId",
     T0."Line_ID",
     MAX("InstlmntID") AS "InstlmntID",
     MAX(T0."Account") AS "Account",
     MAX(T0."ShortName") AS "ShortName",
     MAX(T2."Address") AS "Address",
     MAX(T2."Phone1") AS "Phone",
    MAX(T0."TransType") AS "TransType",
     MAX(T0."CreatedBy") AS "CreatedBy",
     MAX(T0."BaseRef") AS "BaseRef",
     MAX(T0."SourceLine") AS "SourceLine",
    MAX(T0."Debit") AS "Debit",
     MAX(T0."Credit") AS "Credit",
     MAX(T0."FCDebit") AS "FCDebit",
     MAX(T0."FCCredit") AS "FCCredit",
    MAX(T0."DebCred") AS "DebCred",
     MAX(T0."RefDate") AS "RefDate",
     MAX(T0."DueDate") AS "DueDate",
     MAX(T0."TaxDate") AS "TaxDate",
     (MAX(T0."BalDueCred") - MAX(T0."BalDueDeb")) AS "BalDueCred",
     (MAX(T0."BalFcCred") - MAX(T0."BalFcDeb")) AS "BalFCCred",
     (MAX(T0."BalScCred") - MAX(T0."BalScDeb")) AS "BalSCCred",
     MAX(T0."LineMemo") AS "LineMemo" ,
    MAX(T0."Indicator") AS "Indicator",
     MAX(T2."CardName") AS "CardName",
     MAX(T2."CardCode") AS "CardCode",
     MAX(T3."CardName") AS "CardName1",
     MAX(T2."Balance") AS "Balance",
     (SUM(T0."Debit") + SUM(T0."Credit")) AS "DocTotal",
    ( SUM(T0."FCDebit") + SUM(T0."FCCredit")) AS "DocToalFC",
    ( SUM(T0."SYSDeb") + SUM(T0."SYSCred")) AS "DocTotalSC",
     MAX(T3."DunnLevel") AS "DunnLevel",
     MAX(T3."IsSales") AS "IsSales",
     MAX(T2."Currency") AS "BPCurrency",
     MAX(T0."FCCurrency") AS "JEFCCurrency",
     MAX(T3."Installmnt") AS "Installmnt",
     MAX(T3."DunDate") AS "DunDate",
     MAX(T3."InsTotal") AS "InsTotal",
     MAX(T3."InsTotalFC") AS "InsTotalFC",
     MAX(T3."InsTotalSy") AS "InsTotalSy" 
    FROM "JDT1" T0 
    INNER JOIN "OJDT" T1 ON T1."TransId" = T0."TransId" 
    INNER JOIN "OCRD" T2 ON T2."CardCode" = T0."ShortName" 
    INNER JOIN #CRSP_TEMP_OCRD_COL T6 On T2."CardCode" = T6."CardCode" 
    LEFT OUTER JOIN :JOURNALSOURCE T3 ON T3."ObjType" = T0."TransType" 
    AND T3."DocEntry" = T0."CreatedBy" 
    AND (T3."TransType" <> N'I' 
        OR T3."InstlmntID" = T0."SourceLine" ) 
    WHERE 
    --TO_CHAR(T0."RefDate",    'YYYYMMDD') <=:DateValue
    T0."RefDate" <= to_timestamp(:DateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7') 
    AND T2."CardType" ='C' 
    --AND T2."Balance" <> 0 
    AND (T0."BalDueCred" <> T0."BalDueDeb" 
        OR T0."BalFcCred" <> T0."BalFcDeb" ) 
    AND NOT EXISTS (SELECT
     U0."TransId",
     U0."TransRowId" 
        FROM "ITR1" U0 
        INNER JOIN "OITR" U1 ON U1."ReconNum" = U0."ReconNum" 
        WHERE T0."TransId" = U0."TransId" 
        AND T0."Line_ID" = U0."TransRowId" 
        --AND TO_CHAR(U1."ReconDate",    'YYYYMMDD') > :ReconDateValue
        AND U1."ReconDate" > to_timestamp(:ReconDateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7') 
        GROUP BY U0."TransId",
     U0."TransRowId") 
    GROUP BY T0."TransId",
     T0."Line_ID" ) T0 
LEFT OUTER JOIN "ITR1" T1 ON T1."ReconNum" IN (select
     U0."ReconNum" 
    from "ITR1" U0 
    where U0."TransId" = T0."TransId" 
    AND U0."TransRowId" = T0."Line_ID") 
and (T1."TransId"<> T0."TransId" 
    Or T1."TransRowId"<> T0."Line_ID") --LEFT OUTER JOIN "ITR1" U0 ON T1."ReconNum" =U0."ReconNum" AND U0."TransId" = T0."TransId" AND U0."TransRowId" = T0."Line_ID" 
LEFT OUTER JOIN "OITR" T2 ON T2."ReconNum" = T1."ReconNum" 
--AND TO_CHAR(T2."ReconDate",    'YYYYMMDD') <= :ReconDateValue
AND T2."ReconDate" <= to_timestamp(:ReconDateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7')
LEFT OUTER JOIN (SELECT
     "TransId" AS "ReconTransID",
     "BaseRef" AS "ReconBaseRef",
     "RefDate" AS "ReconRefDate" ,
     "TaxDate" AS "ReconDocumentDate" 
    From "OJDT") T4 ON T4."ReconTransID" = T1."TransId" 
LEFT OUTER JOIN (SELECT
     "TransId" AS "ReconTransID1",
     "Line_ID" AS "ReconJELine_ID",
     "LineMemo" AS "ReconJrnlMemo",
     "DebCred" AS "ReconDebCred",
     "Debit" AS "ReconDebit",
     "Credit" AS "ReconCredit",
     "FCDebit" AS "ReconDebitFc",
     "FCCredit" AS "ReconCreditFC" ,
     "FCCurrency" AS "ReconJEFCCurrency" 
    From "JDT1") T5 on T5."ReconTransID1" = T4."ReconTransID" 
AND T5."ReconJELine_ID" = T1."TransRowId" 
LEFT OUTER JOIN "OIDC" T3 ON T3."Code" = T0."Indicator" 
ORDER BY T0."RefDate",
     T0."BaseRef" ,
     T2."ReconDate" 
; 
 DPRequest = Select
     T0."DocEntry",
     -1 as "TransId",
     -1 as "Line_ID",
     -1 as InstlmntID,
     T4."CardCode" as ShortName,
     T4."Address",
     T4."Phone1",
     '203' as TransType,
     '-1' As CreatedBy,
     To_NVarChar(T0."DocNum") as BaseRef,
     0 as Debit,
     0 as Credit,
     0 as FCDebit,
     0 as FCCredit,
     '' as DebCred,
     T0."DocDate" as RefDate,
     T0."DocDueDate" as DueDate,
     T0."TaxDate",
     0 as BalDueCred,
     0 as BalFCCred,
     T0."JrnlMemo" as "LineMemo",
     T4."CardName",
     T4."CardCode",
     T3."DunnLevel",
     T4."Currency" as BPCurrency,
     '' as JEFCCurrency,
     '' as "FCCurrency",
     -1 as Installmnt,
     T3."DunDate",
     T3."InsTotal",
     T3."InsTotalFC",
     T3."InsTotalSy",
     '203' as SrcObjTyp,
     T0."DocEntry" as SrcObjAbs,
     '' as IsCredit,
     NULL as ReconDate,
     T0."CurSource" as ReconCurr,
     '' as ReconBaseRef,
     NULL as ReconRefDate,
     NULL as ReconDocumentDate,
     NULL as ReconJrnlMemo,
     '' as ReconDebCred,
     0 as ReconDebit,
     0 as ReconCredit,
     0 as ReconDebitFc,
     0 as ReconCreditFC ,
     T0."DocCur" as ReconJEFCCurrency,
     CURRENT_DATE as ServerDate,
    T5."Name" as IndicatorName ,
    0 as realReconSum ,
     0 as realRconSumFC,
     'Y' as IsDPMRequest 
From ODPI T0 
Inner Join DPI6 T3 On T0."CreateTran" ='N' 
and T0."DocEntry" = T3."DocEntry" 
and T0."DocEntry" in ( Select
     U0."DocEntry" 
    from odpi U0 
    Left Outer Join RCT2 T1 On U0."CreateTran" ='N' 
    and T1."InvType" = '203' 
    and T1."DocEntry" = U0."DocEntry" 
    Left Outer Join ORCT T2 on T2."DocEntry" = T1."DocNum" 
    Where TO_DATE(T2."DocDate") <= To_Date(:DateValue) 
    Or U0."DocStatus" = 'O' 
    Group By U0."DocEntry" Having Sum(T1."SumApplied") <> Max(U0."DocTotal") 
    Or Max(U0."DocStatus") = 'O' ) 
Inner Join OCRD T4 On T4."CardCode" = T0."CardCode" 
Inner Join #CRSP_TEMP_OCRD_COL T6 On T6."CardCode" = T4."CardCode"
Left Outer Join OIDC T5 On T5."Code" = T0."Indicator" 
Where :IncludeDPMRequestIn = '1'
;

ELSE PostedTrans = SELECT 
     T0."DocEntry",
     T0."TransId",
     T0."Line_ID",
     T0."InstlmntID",
     T0."ShortName",
     T0."Address",
     T0."Phone",
    T0."TransType",
     T0."CreatedBy",
     T0."BaseRef",
     T0."Debit",
     T0."Credit",
     T0."FCDebit",
     T0."FCCredit",
    T0."DebCred",
     T0."RefDate",
     T0."DueDate",
     T0."TaxDate",
     T0."BalDueCred",
     T0."BalFCCred",
     T0."LineMemo" ,
     T0."CardName",
     T0."CardCode",
     T0."DunnLevel",
     T0."BPCurrency",
     T0."JEFCCurrency",
     T0."JEFCCurrency" as "FCCurrency",
     T0."Installmnt",
     T0."DunDate",
     T0."InsTotal",
     T0."InsTotalFC" ,
     T0."InsTotalSy",
     T1."SrcObjTyp",
     T1."SrcObjAbs",
     T1."IsCredit",
     T2."ReconDate",
     T2."ReconCurr",
     T4."ReconBaseRef",
     T4."ReconRefDate",
     T4."ReconDocumentDate",
     T5."ReconJrnlMemo",
    T5."ReconDebCred",
     T5."ReconDebit",
     T5."ReconCredit",
     T5."ReconDebitFc",
     T5."ReconCreditFC" ,
     T5."ReconJEFCCurrency",
     CURRENT_DATE AS "ServerDate",
    T3."Name" AS "IndicatorName" ,
     (CASE WHEN T2."ReconDate" IS NULL 
    THEN 0 
    ELSE T1."ReconSum" 
    END ) AS "realReconSum" ,
     (CASE WHEN T2."ReconDate" IS NULL 
    THEN 0 
    ELSE T1."ReconSumFC" 
    END ) AS "realReconSumFC" ,
     'N' As "IsDPMRequest" 
FROM ( SELECT
     MAX(T5."DocEntry") AS "DocEntry",
    T0."TransId",
     T0."Line_ID",
     MAX("InstlmntID") AS "InstlmntID",
     MAX(T0."Account") AS "Account",
     MAX(T0."ShortName") AS "ShortName",
     MAX(T4."Address") AS "Address",
     MAX(T4."Phone1") AS "Phone",
     MAX(T0."TransType") AS "TransType",
     MAX(T0."CreatedBy") AS "CreatedBy",
     MAX(T0."BaseRef") AS "BaseRef",
     MAX(T0."SourceLine") AS "SourceLine",
     MAX(T0."Debit") AS "Debit",
     MAX(T0."Credit") AS "Credit",
     MAX(T0."FCDebit") AS "FCDebit",
     MAX(T0."FCCredit") AS "FCCredit",
    MAX(T0."DebCred") AS "DebCred",
     MAX(T0."RefDate") AS "RefDate" ,
     MAX(T0."DueDate") AS "DueDate",
     MAX(T0."TaxDate") AS "TaxDate",
     (MAX(T0."BalDueCred") + SUM(T1."ReconSum")) AS "BalDueCred",
     (MAX(T0."BalFcCred") + SUM(T1."ReconSumFC")) AS "BalFCCred",
    ( MAX(T0."BalScCred") + SUM(T1."ReconSumSC")) AS "BalScCred",
     MAX(T0."LineMemo") AS "LineMemo",
     MAX(T0."Indicator") AS "Indicator",
     MAX(T4."CardName") AS "CardName",
     MAX(T4."CardCode") AS "CardCode",
     MAX(T5."CardName") AS "CardName1",
     MAX(T4."Balance") AS "Balance",
     (SUM(T0."Debit") + SUM(T0."Credit")) AS "DocTotal",
     SUM(T0."FCDebit") + SUM(T0."FCCredit") AS "DocTotalFC",
     SUM(T0."SYSDeb") + SUM(T0."SYSCred") AS "DocTotalSC",
     MAX(T5."DunnLevel") AS "DunnLevel" ,
     MAX(T5."IsSales") AS "IsSales",
     MAX(T4."Currency") AS "BPCurrency",
     MAX(T0."FCCurrency") AS "JEFCCurrency" ,
     MAX(T5."Installmnt") AS "Installmnt",
     MAX(T5."DunDate") AS "DunDate",
     MAX(T5."InsTotal") AS "InsTotal",
     MAX(T5."InsTotalFC") AS "InsTotalFC",
     MAX(T5."InsTotalSy") AS "InsTotalSy" 
    FROM "JDT1" T0 
    INNER JOIN "ITR1" T1 ON T1."TransId" = T0."TransId" 
    AND T1."TransRowId" = T0."Line_ID" 
    INNER JOIN "OITR" T2 ON T2."ReconNum" = T1."ReconNum" 
    INNER JOIN "OJDT" T3 ON T3."TransId" = T0."TransId" 
    INNER JOIN "OCRD" T4 ON T4."CardCode" = T0."ShortName" 
    INNER JOIN #CRSP_TEMP_OCRD_COL T6 On T4."CardCode" = T6."CardCode" 
    LEFT OUTER JOIN :JOURNALSOURCE T5 ON T5."ObjType" = T0."TransType" 
    AND T5."DocEntry" = T0."CreatedBy" 
    AND (T5."TransType" <> N'I' 
        OR T5."InstlmntID" = T0."SourceLine" ) 
    WHERE 
    --TO_CHAR(T0."TaxDate",    'YYYYMMDD') <= :DateValue
    T0."TaxDate" <= to_timestamp(:DateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7') 
    AND T4."CardType" = 'C' 
    --AND T4."Balance" <>0 
    --AND TO_CHAR(T2."ReconDate",     'YYYYMMDD') >:ReconDateValue
    AND T2."ReconDate" > to_timestamp(:ReconDateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7') 
    AND T1."IsCredit" = 'C' 
    GROUP BY T0."TransId",
     T0."Line_ID" HAVING MAX(T0."BalFcCred") <>- SUM(T1."ReconSumFC") 
    OR MAX(T0."BalDueCred") <>- SUM(T1."ReconSum") 
    UNION ALL SELECT
     MAX(T5."DocEntry") AS "DocEntry",
    T0."TransId",
     T0."Line_ID",
     MAX("InstlmntID") AS "InstlmntID",
     MAX(T0."Account") AS "Account",
     MAX(T0."ShortName") AS "ShortName",
     MAX(T4."Address") AS "Address",
     MAX(T4."Phone1") AS "Phone",
    MAX(T0."TransType") AS "TransType",
     MAX(T0."CreatedBy") AS "CreatedBy",
     MAX(T0."BaseRef") AS "BaseRef",
     MAX(T0."SourceLine") AS "SourceLine",
     MAX(T0."Debit") AS "Debit",
     MAX(T0."Credit") AS "Credit",
     MAX(T0."FCDebit") AS "FCDebit",
     MAX(T0."FCCredit") AS "FCCredit",
    MAX(T0."DebCred") AS "DebCred",
     MAX(T0."RefDate") AS "RefDate",
     MAX(T0."DueDate") AS "DueDate",
     MAX(T0."TaxDate") AS "TaxDate",
     (- MAX(T0."BalDueDeb") - SUM(T1."ReconSum")) AS "BalDueCred",
     (- MAX(T0."BalFcDeb") - SUM(T1."ReconSumFC")) AS "BalFCCred",
     (- MAX(T0."BalScDeb") - SUM(T1."ReconSumSC")) AS "BalScCred",
     MAX(T0."LineMemo") AS "LineMemo",
     MAX(T0."Indicator") AS "Indicator",
     MAX(T4."CardName") AS "CardName",
     MAX(T4."CardCode") AS "CardCode",
     MAX(T5."CardName") AS "CardName1",
     MAX(T4."Balance") AS "Balance",
    (SUM(T0."Debit") + SUM(T0."Credit")) AS "DocTotal",
     (SUM(T0."FCDebit") + SUM(T0."FCCredit")) AS "DocTotalFC",
     (SUM(T0."SYSDeb") + SUM(T0."SYSCred")) AS "DocTotalSC",
     MAX(T5."DunnLevel") AS "DunnLevel",
     MAX(T5."IsSales") AS "IsSales",
     MAX(T4."Currency") AS "BPCurrency",
     MAX(T0."FCCurrency") AS "JEFCCurrency" ,
    MAX(T5."Installmnt") AS "Installmnt",
     MAX(T5."DunDate") AS "DunDate" ,
     MAX(T5."InsTotal") AS "InsTotal",
     MAX(T5."InsTotalFC") AS "InsTotalFC",
     MAX(T5."InsTotalSy") AS "InsTotalSy" 
    FROM "JDT1" T0 
    INNER JOIN "ITR1" T1 ON T1."TransId" = T0."TransId" 
    AND T1."TransRowId" = T0."Line_ID" 
    INNER JOIN "OITR" T2 ON T2."ReconNum" = T1."ReconNum" 
    INNER JOIN "OJDT" T3 ON T3."TransId" = T0."TransId" 
    INNER JOIN "OCRD" T4 ON T4."CardCode" = T0."ShortName" 
    INNER JOIN #CRSP_TEMP_OCRD_COL T6 On T4."CardCode" = T6."CardCode" 
    LEFT OUTER JOIN :JOURNALSOURCE T5 ON T5."ObjType" = T0."TransType" 
    AND T5."DocEntry" = T0."CreatedBy" 
    AND (T5."TransType" <> N'I' 
        OR T5."InstlmntID" = T0."SourceLine" ) 
    WHERE 
    --TO_CHAR(T0."TaxDate",     'YYYYMMDD') <=:DateValue
    T0."TaxDate" <= to_timestamp(:DateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7') 
    AND T4."CardType" = 'C' 
    --AND T4."Balance" <>0 
    --AND TO_CHAR(T2."ReconDate",    'YYYYMMDD') >:ReconDateValue
    AND T2."ReconDate" > to_timestamp(:ReconDateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7') 
    AND T1."IsCredit" = 'D' 
    GROUP BY T0."TransId",
     T0."Line_ID" HAVING MAX(T0."BalFcDeb") <>- SUM(T1."ReconSumFC") 
    OR MAX(T0."BalDueDeb") <>- SUM(T1."ReconSum") 
    UNION ALL SELECT
     (CASE WHEN MAX(T3."DocEntry") IS NULL 
        THEN T0."TransId" 
        ELSE MAX(T3."DocEntry") 
        END ) AS "DocEntry",
     T0."TransId",
     T0."Line_ID",
     MAX("InstlmntID") AS "InstlmntID",
     MAX(T0."Account") AS "Account",
     MAX(T0."ShortName") AS "ShortName",
     MAX(T2."Address") AS "Address",
     MAX(T2."Phone1") AS "Phone",
    MAX(T0."TransType") AS "TransType",
     MAX(T0."CreatedBy") AS "CreatedBy",
     MAX(T0."BaseRef") AS "BaseRef",
     MAX(T0."SourceLine") AS "SourceLine",
    MAX(T0."Debit") AS "Debit",
     MAX(T0."Credit") AS "Credit",
     MAX(T0."FCDebit") AS "FCDebit",
     MAX(T0."FCCredit") AS "FCCredit",
    MAX(T0."DebCred") AS "DebCred",
     MAX(T0."RefDate") AS "RefDate",
     MAX(T0."DueDate") AS "DueDate",
     MAX(T0."TaxDate") AS "TaxDate",
     (MAX(T0."BalDueCred") - MAX(T0."BalDueDeb")) AS "BalDueCred",
     (MAX(T0."BalFcCred") - MAX(T0."BalFcDeb")) AS "BalFCCred",
     (MAX(T0."BalScCred") - MAX(T0."BalScDeb")) AS "BalSCCred",
     MAX(T0."LineMemo") AS "LineMemo" ,
    MAX(T0."Indicator") AS "Indicator",
     MAX(T2."CardName") AS "CardName",
     MAX(T2."CardCode") AS "CardCode",
     MAX(T3."CardName") AS "CardName1",
     MAX(T2."Balance") AS "Balance",
     (SUM(T0."Debit") + SUM(T0."Credit")) AS "DocTotal",
    ( SUM(T0."FCDebit") + SUM(T0."FCCredit")) AS "DocToalFC",
    ( SUM(T0."SYSDeb") + SUM(T0."SYSCred")) AS "DocTotalSC",
     MAX(T3."DunnLevel") AS "DunnLevel",
     MAX(T3."IsSales") AS "IsSales",
     MAX(T2."Currency") AS "BPCurrency",
     MAX(T0."FCCurrency") AS "JEFCCurrency",
     MAX(T3."Installmnt") AS "Installmnt",
     MAX(T3."DunDate") AS "DunDate",
     MAX(T3."InsTotal") AS "InsTotal",
     MAX(T3."InsTotalFC") AS "InsTotalFC",
     MAX(T3."InsTotalSy") AS "InsTotalSy" 
    FROM "JDT1" T0 
    INNER JOIN "OJDT" T1 ON T1."TransId" = T0."TransId" 
    INNER JOIN "OCRD" T2 ON T2."CardCode" = T0."ShortName" 
    INNER JOIN #CRSP_TEMP_OCRD_COL T6 On T2."CardCode" = T6."CardCode" 
    LEFT OUTER JOIN :JOURNALSOURCE T3 ON T3."ObjType" = T0."TransType" 
    AND T3."DocEntry" = T0."CreatedBy" 
    AND (T3."TransType" <> N'I' 
        OR (T3."TransType" = N'I' 
            AND T3."InstlmntID" = T0."SourceLine" )) 
    WHERE 
    --TO_CHAR(T0."TaxDate",    'YYYYMMDD') <=:DateValue

    T0."TaxDate" <= to_timestamp(:DateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7') 
    AND T2."CardType" ='C' 
    --AND T2."Balance" <> 0 
    AND (T0."BalDueCred" <> T0."BalDueDeb" 
        OR T0."BalFcCred" <> T0."BalFcDeb" ) 
    AND NOT EXISTS (SELECT
     U0."TransId",
     U0."TransRowId" 
        FROM "ITR1" U0 
        INNER JOIN "OITR" U1 ON U1."ReconNum" = U0."ReconNum" 
        WHERE T0."TransId" = U0."TransId" 
        AND T0."Line_ID" = U0."TransRowId" 
        --AND TO_CHAR(U1."ReconDate",     'YYYYMMDD') > :ReconDateValue
        AND U1."ReconDate" > to_timestamp(:ReconDateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7')
        GROUP BY U0."TransId",
     U0."TransRowId") 
    GROUP BY T0."TransId",
     T0."Line_ID" ) T0 
LEFT OUTER JOIN "ITR1" T1 ON T1."ReconNum" IN (select
     U0."ReconNum" 
    from "ITR1" U0 
    where U0."TransId" = T0."TransId" 
    AND U0."TransRowId" = T0."Line_ID") 
and (T1."TransId"<> T0."TransId" 
    Or T1."TransRowId"<> T0."Line_ID") --LEFT OUTER JOIN "ITR1" U0 ON T1."ReconNum" =U0."ReconNum" AND U0."TransId" = T0."TransId" AND U0."TransRowId" = T0."Line_ID"
LEFT OUTER JOIN "OITR" T2 ON T2."ReconNum" = T1."ReconNum" 
--AND TO_CHAR(T2."ReconDate",    'YYYYMMDD') <= :ReconDateValue
AND T2."ReconDate" <= to_timestamp(:ReconDateValue || ' 23:59:59.9999999', 'yyyymmdd hh:mi:ss.ff7')
LEFT OUTER JOIN (SELECT
     "TransId" AS "ReconTransID",
     "BaseRef" AS "ReconBaseRef",
     "RefDate" AS "ReconRefDate" ,
     "TaxDate" AS "ReconDocumentDate" 
    From "OJDT") T4 ON T4."ReconTransID" = T1."TransId" 
LEFT OUTER JOIN (SELECT
     "TransId" AS "ReconTransID1",
     "Line_ID" AS "ReconJELine_ID",
     "LineMemo" AS "ReconJrnlMemo",
     "DebCred" AS "ReconDebCred",
     "Debit" AS "ReconDebit",
     "Credit" AS "ReconCredit",
     "FCDebit" AS "ReconDebitFc",
     "FCCredit" AS "ReconCreditFC" ,
     "FCCurrency" AS "ReconJEFCCurrency" 
    From "JDT1") T5 on T5."ReconTransID1" = T4."ReconTransID" 
AND T5."ReconJELine_ID" = T1."TransRowId" 
LEFT OUTER JOIN "OIDC" T3 ON T3."Code" = T0."Indicator" 
ORDER BY T0."RefDate",
     T0."BaseRef" ,
     T2."ReconDate" 
;
DPRequest = Select
     T0."DocEntry",
     -1 as "TransId",
     -1 as "Line_ID",
     -1 as InstlmntID,
     T4."CardCode" as ShortName,
     T4."Address",
     T4."Phone1",
     '203' as TransType,
     '-1' As CreatedBy,
     To_NVarChar(T0."DocNum") as BaseRef,
     0 as Debit,
     0 as Credit,
     0 as FCDebit,
     0 as FCCredit,
     '' as DebCred,
     T0."DocDate" as RefDate,
     T0."DocDueDate" as DueDate,
     T0."TaxDate",
     0 as BalDueCred,
     0 as BalFCCred,
     T0."JrnlMemo" as "LineMemo",
     T4."CardName",
     T4."CardCode",
     T3."DunnLevel",
     T4."Currency" as BPCurrency,
     '' as JEFCCurrency,
     '' as "FCCurrency",
     -1 as Installmnt,
     T3."DunDate",
     T3."InsTotal",
     T3."InsTotalFC",
     T3."InsTotalSy",
     '203' as SrcObjTyp,
     T0."DocEntry" as SrcObjAbs,
     '' as IsCredit,
     NULL as ReconDate,
     T0."CurSource" as ReconCurr,
     '' as ReconBaseRef,
     NULL as ReconRefDate,
     NULL as ReconDocumentDate,
     NULL as ReconJrnlMemo,
     '' as ReconDebCred,
     0 as ReconDebit,
     0 as ReconCredit,
     0 as ReconDebitFc,
     0 as ReconCreditFC ,
     T0."DocCur" as ReconJEFCCurrency,
     CURRENT_DATE as ServerDate,
    T5."Name" as IndicatorName ,
    0 as realReconSum ,
     0 as realRconSumFC,
     'Y' as IsDPMRequest 
From ODPI T0 
Inner Join DPI6 T3 On T0."CreateTran" ='N' 
and T0."DocEntry" = T3."DocEntry" 
and T0."DocEntry" in ( Select
     U0."DocEntry" 
    from odpi U0 
    Left Outer Join RCT2 T1 On U0."CreateTran" ='N' 
    and T1."InvType" = '203' 
    and T1."DocEntry" = U0."DocEntry" 
    Left Outer Join ORCT T2 on T2."DocEntry" = T1."DocNum" 
    Where TO_DATE(T2."DocDate") <= To_Date(:DateValue) 
    Or U0."DocStatus" = 'O' 
    Group By U0."DocEntry" Having Sum(T1."SumApplied") <> Max(U0."DocTotal") 
    Or Max(U0."DocStatus") = 'O' ) 
Inner Join OCRD T4 On T4."CardCode" = T0."CardCode" 
Inner Join #CRSP_TEMP_OCRD_COL T6 On T6."CardCode" = T4."CardCode"
Left Outer Join OIDC T5 On T5."Code" = T0."Indicator" 
Where :IncludeDPMRequestIn = '1'
;
END 
IF
;
Select
     * 
From :PostedTrans 
Union All Select
     * 
From :DPRequest 
Order By "RefDate",
     "BaseRef",
     "ReconDate"
;

--drop table #CRSP_TEMP_OCRD_COL; /* cause client of rev48 disconnected */

END











