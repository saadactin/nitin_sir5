-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:_TmSp_BootCreateGlobalTempTables2 AFTER:SP:_TmSp_ValidateSpParam

CREATE PROCEDURE CRSP_Customer_Open_Item_List
(
in DateTypeIn nvarchar(1),
in DateValueIn datetime,
in ReconDateValueIn datetime,
in CustomerIdsIn nclob,
in IncludeDPMRequestIn nvarchar(1)
)
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS
execSQL 			nclob;
dateType 			nvarchar(1);
dateValue 			datetime;
reconDateValue 		datetime;
acctList  			nclob;
customerIds 		nclob;
includeDPMRequest 	nvarchar(1);
tableName			nvarchar(132);
dateField			nvarchar(16);
BEGIN

call _TmSp_ValidateSpParam (:DateTypeIn);
call _TmSp_ValidateSpParam (:IncludeDPMRequestIn);

dateType := :DateTypeIn;
dateValue := :DateValueIn;
reconDateValue := :ReconDateValueIn;
customerIds := :CustomerIdsIn;
includeDPMRequest := :IncludeDPMRequestIn;

If :IncludeDPMRequestIn = '1' Then
	includeDPMRequest := 'Y';
END IF;

delete from "CRSP_Customer_Open_Item_List_TmpResult";
delete from "Customer_Open_Item_List_TempOpenDocumentViewByPosting";
delete from "Customer_Open_Item_List_TempOpenDocumentViewByDocDate";
delete from "Customer_Open_Item_List_TempJournalSource";
delete from "Customer_Open_Item_List_TempInstallmentsDocument";

execSQL:= 'insert into "Customer_Open_Item_List_TempInstallmentsDocument" 
(SELECT T0."DocEntry", T0."InstlmntID", 
T0."ObjType", T1."CardCode", T1."CardName", T1."NumAtCard", T1."SlpCode", 
T0."InsTotal", T0."InsTotalFC", T0."InsTotalSy", T1."BlockDunn", T0."DunnLevel", T1."Installmnt", T0."DunDate",
N''Y'' AS "IsSales", N''Y'' AS "IsDunnable" FROM  INV6 T0  INNER  JOIN OINV T1  
ON  T1."DocEntry" = T0."DocEntry" where T1."CardCode" in ' || :customerIds || ' 
Union All
SELECT T0."DocEntry", T0."InstlmntID", 
T0."ObjType", T1."CardCode", T1."CardName", T1."NumAtCard", T1."SlpCode", 
T0."InsTotal", T0."InsTotalFC", T0."InsTotalSy", T1."BlockDunn", T0."DunnLevel",  T1."Installmnt", T0."DunDate",
N''Y'' AS "IsSales", N''N'' AS "IsDunnable" FROM  RIN6 T0  INNER  JOIN ORIN T1  
ON  T1."DocEntry" = T0."DocEntry" where T1."CardCode" in ' || :customerIds || '
Union All
SELECT T0."DocEntry", T0."InstlmntID", 
T0."ObjType", T1."CardCode", T1."CardName", T1."NumAtCard", T1."SlpCode", 
T0."InsTotal", T0."InsTotalFC", T0."InsTotalSy", T1."BlockDunn", T0."DunnLevel",  T1."Installmnt", T0."DunDate",
N''Y'' AS "IsSales", N''Y'' AS "IsDunnable" FROM  DPI6 T0  INNER  JOIN ODPI T1  
ON  T1."DocEntry" = T0."DocEntry" where T1."CardCode" in ' || :customerIds || ')'
;

exec (:execSQL);

insert into  "Customer_Open_Item_List_TempJournalSource" (
SELECT T0."DocEntry", T0."InstlmntID", T0."Installmnt", T0."DunDate",T0."ObjType", 
T0."CardCode", T0."CardName", T0."NumAtCard", T0."SlpCode", 
T0."InsTotal" as "InsTotal", T0."InsTotalFC" as "InsTotalFC", T0."InsTotalSy" as "InsTotalSy", T0."BlockDunn", 
T0."DunnLevel", N'I' AS "TransType", T0."IsSales", T0."IsDunnable", 
0 AS "BoeNum", N'' AS "BoeStatus" FROM  "Customer_Open_Item_List_TempInstallmentsDocument"  T0  
UNION ALL SELECT T0."DocEntry", 0 AS "InstlmntID", 0 as "Installmnt", NULL as "DunDate",T0."ObjType", T0."CardCode", 
T0."CardName", N'' AS "NumAtCard", -1 AS "SlpCode", T0."DocTotal" as InsTotal, T0."DocTotalFC" as "InsTotalFC", 
T0."DocTotalSy" as "InsTotalSy", N'Y' AS "BlockDunn", 0 AS "DunnLevel", N'P' AS "SrcType", 
N'' AS "IsSales", N'N' AS "IsDunnable", 0 AS "BoeNum", N'' AS "BoeStatus" FROM  ORCT T0  
 UNION ALL SELECT T0."DeposId", 0 AS "InstlmntID", 0 as "Installmnt", NULL as "DunDate",
T0."ObjType", N'' AS "CardCode", N'' AS "CardName", N'' AS "NumAtCard", -1 AS "SlpCode", 
T0."LocTotal" as "InsTotal" , T0."FcTotal" as "InsTotalFC" , T0."SysTotal" as "InsTotalSy", N'Y' AS "BlockDunn", 0 AS "DunnLevel", 
N'D' AS "SrcType", N'' AS "IsSales", N'N' AS "IsDunnable", T1."BoeNum", T1."BoeStatus" 
FROM  ODPS T0   LEFT OUTER  JOIN OBOE T1  ON  T1."BoeKey" = T0."DeposId" 
); 

if :dateType = '1' then
	execSQL:= ' 
	INSERT INTO "Customer_Open_Item_List_TempOpenDocumentViewByPosting" (
	SELECT Max(T5."DocEntry") as DocEntry,T0."TransId", T0."Line_ID", Max("InstlmntID") as InstlmntID, MAX(T0."Account") as Account, MAX(T0."ShortName") as ShortName, MAX(T4."Address") as Address, MAX(T4."Phone1") as Phone, MAX(T0."TransType") as TransType, MAX(T0."CreatedBy") as CreatedBy, MAX(T0."BaseRef") as BaseRef, MAX(T0."SourceLine") as SourceLine, MAX(T0."Debit") as Debit, Max(T0."Credit") as Credit, Max(T0."FCDebit") as FCDebit, Max(T0."FCCredit") as FCCredit,Max(T0."DebCred") as DebCred,
	MAX(T0."RefDate") as RefDate , MAX(T0."DueDate") as DueDate, MAX(T0."TaxDate") as TaxDate, (MAX(T0."BalDueCred") + SUM(T1."ReconSum")) as BalDueCred, (MAX(T0."BalFcCred") + SUM(T1."ReconSumFC")) as BalFCCred,( MAX(T0."BalScCred") + 
	SUM(T1."ReconSumSC")) as BalScCred, MAX(T0."LineMemo") as LineMemo, MAX(T0."Indicator") as Indicator, MAX(T4."CardName") as CardName, MAX(T5."CardCode") as CardCode, MAX(T5."CardName") as CardName1, 
	MAX(T4."Balance") as Balance, (SUM(T0."Debit") + SUM(T0."Credit")) as DocTotal, SUM(T0."FCDebit") + SUM(T0."FCCredit") as DocTotalFC, SUM(T0."SYSDeb") + 
	SUM(T0."SYSCred") as DocTotalSC, MAX(T5."DunnLevel") as DunnLevel , MAX(T5."IsSales") as IsSales, MAX(T4."Currency") as BPCurrency, MAX(T0."FCCurrency") as JEFCCurrency , 
	Max(T5."Installmnt") as Installmnt, Max(T5."DunDate") as DunDate, Max(T5."InsTotal") as InsTotal, Max(T5."InsTotalFC") as InsTotalFC, Max(T5."InsTotalSy") as InsTotalSy
	FROM  JDT1 T0  INNER  JOIN ITR1 T1  ON  T1."TransId" = T0."TransId"  AND  T1."TransRowId" = T0."Line_ID"   INNER  JOIN 
	OITR T2  ON  T2."ReconNum" = T1."ReconNum"   INNER  JOIN OJDT T3  ON  T3."TransId" = T0."TransId"   INNER  JOIN OCRD T4  ON  T4."CardCode" = T0."ShortName"    
	LEFT OUTER  JOIN "Customer_Open_Item_List_TempJournalSource" T5  ON  T5."ObjType" = T0."TransType"  AND  T5."DocEntry" = T0."CreatedBy"  AND  (T5."TransType" <> N''I''  OR  (T5."TransType" = N''I'' 
	AND  T5."InstlmntID" = T0."SourceLine" ))  WHERE T0."RefDate" <= ''' || :dateValue || ''' AND  T4."CardType" = ''C''  AND  T4."Balance" <>0  AND  T4."CardCode"  in ' || :customerIds || '  AND  T2."ReconDate" > ''' || :reconDateValue || ''' AND  T1."IsCredit" = ''C''  GROUP BY T0."TransId", T0."Line_ID" HAVING MAX(T0."BalFcCred") <>- 
	SUM(T1."ReconSumFC")  OR  MAX(T0."BalDueCred") <>- SUM(T1."ReconSum")  
	Union All
	SELECT Max(T5."DocEntry") as DocEntry,T0."TransId", T0."Line_ID", Max("InstlmntID") as InstlmntID, MAX(T0."Account") as Account, MAX(T0."ShortName") as ShortName, MAX(T4."Address") as Address, MAX(T4."Phone1") as Phone,MAX(T0."TransType") as TransType, MAX(T0."CreatedBy") as CreatedBy, MAX(T0."BaseRef") as BaseRef, MAX(T0."SourceLine") as SourceLine, MAX(T0."Debit") as Debit, Max(T0."Credit") as Credit, Max(T0."FCDebit") as FCDebit, Max(T0."FCCredit") as FCCredit,Max(T0."DebCred") as DebCred,
	MAX(T0."RefDate") as RefDate, MAX(T0."DueDate") as DueDate, MAX(T0."TaxDate") as TaxDate,  (- MAX(T0."BalDueDeb") - SUM(T1."ReconSum")) as BalDueCred,  (- MAX(T0."BalFcDeb") - SUM(T1."ReconSumFC")) as BalFCCred,  (- MAX(T0."BalScDeb") - SUM(T1."ReconSumSC")) as BalScCred, MAX(T0."LineMemo") as LineMeno, MAX(T0."Indicator") as Indicator, MAX(T4."CardName") as CardName, MAX(T5."CardCode") as CardCode, MAX(T5."CardName") as CardName1, MAX(T4."Balance") as Balance,(SUM(T0."Debit") + 
	SUM(T0."Credit")) as DocTotal, (SUM(T0."FCDebit") + SUM(T0."FCCredit")) as DocTotalFC, (SUM(T0."SYSDeb") + SUM(T0."SYSCred")) as  DocTotalSC, MAX(T5."DunnLevel") as DunnLevel,
	MAX(T5."IsSales") as IsSales, MAX(T4."Currency") as BPCurrency, MAX(T0."FCCurrency") as JEFCCurrency  ,Max(T5."Installmnt") as Installmnt, Max(T5."DunDate") as DunDate
	, Max(T5."InsTotal") as InsTotal, Max(T5."InsTotalFC") as InsTotalFC, Max(T5."InsTotalSy") as InsTotalSy
	FROM  JDT1 T0  INNER  JOIN ITR1 T1  ON  
	T1."TransId" = T0."TransId"  AND  T1."TransRowId" = T0."Line_ID"   INNER  JOIN OITR T2  ON  T2."ReconNum" = T1."ReconNum"  INNER  JOIN OJDT T3  ON  T3."TransId" = 
	T0."TransId"   INNER  JOIN OCRD T4  ON  T4."CardCode" = T0."ShortName"    LEFT OUTER  JOIN "Customer_Open_Item_List_TempJournalSource" T5  ON  T5."ObjType" = T0."TransType"  AND  
	T5."DocEntry" = T0."CreatedBy"  AND  (T5."TransType" <> N''I''  OR  (T5."TransType" = N''I''  AND  T5."InstlmntID" = T0."SourceLine" ))  WHERE T0."RefDate" <= 
	''' || :dateValue || ''' AND  T4."CardType" = ''C''  AND  T4."Balance" <>0  AND  T4."CardCode" in ' ||  customerIds || '  AND  T2."ReconDate" > ''' || :reconDateValue || '''  AND  T1."IsCredit" = 
	''D''   GROUP BY T0."TransId", T0."Line_ID" HAVING MAX(T0."BalFcDeb") <>- SUM(T1."ReconSumFC")  OR  MAX(T0."BalDueDeb") <>- SUM(T1."ReconSum")   
	UNION ALL 
	SELECT (Case IfNull(Max(T3."DocEntry"), -1) when -1  Then T0."TransId" Else Max(T3."DocEntry") end )  as DocEntry, T0."TransId", T0."Line_ID", Max("InstlmntID") as InstlmntID, MAX(T0."Account") as Account, MAX(T0."ShortName") as ShortName, MAX(T2."Address") as Address, MAX(T2."Phone1") as Phone,MAX(T0."TransType") as TransType, MAX(T0."CreatedBy") as CreatedBy, MAX(T0."BaseRef") as BaseRef, MAX(T0."SourceLine") as SourceLine,MAX(T0."Debit") as Debit, Max(T0."Credit") as Credit, Max(T0."FCDebit") as FCDebit, Max(T0."FCCredit") as FCCredit,Max(T0."DebCred") as DebCred,
	MAX(T0."RefDate")  as RefDate, MAX(T0."DueDate") as DueDate, MAX(T0."TaxDate") as TaxDate, (MAX(T0."BalDueCred") - MAX(T0."BalDueDeb")) as BalDueCred, (MAX(T0."BalFcCred") - MAX(T0."BalFcDeb")) as BalFCCred, (MAX(T0."BalScCred") - MAX(T0."BalScDeb")) as BalSCCred, MAX(T0."LineMemo") as LineMemo ,MAX(T0."Indicator") as Indicator, MAX(T2."CardName") as CardName, MAX(T3."CardCode") as CardCode, MAX(T3."CardName") as CardName1, MAX(T2."Balance") as Balance, 
	(SUM(T0."Debit") + SUM(T0."Credit")) as DocTotal,( SUM(T0."FCDebit") + SUM(T0."FCCredit")) as DocToalFC,( SUM(T0."SYSDeb") + SUM(T0."SYSCred")) as DocTotalSC, 
	MAX(T3."DunnLevel") as DunnLevel, MAX(T3."IsSales") as IsSales, MAX(T2."Currency") as BPCurrency, MAX(T0."FCCurrency") as JEFCCurrency,
	Max(T3."Installmnt") as Installmnt, Max(T3."DunDate") as DunDate, Max(T3."InsTotal") as InsTotal, Max(T3."InsTotalFC") as InsTotalFC, Max(T3."InsTotalSy") as InsTotalSy
	FROM  JDT1 T0  INNER  JOIN OJDT T1  ON  T1."TransId" = T0."TransId"  INNER  JOIN OCRD T2  ON  T2."CardCode" = T0."ShortName"    LEFT 
	OUTER  JOIN  "Customer_Open_Item_List_TempJournalSource" T3  ON  T3."ObjType" = T0."TransType"  AND  T3."DocEntry" = T0."CreatedBy"  AND  (T3."TransType" <> N''I''  OR  (T3."TransType" = N''I''  AND  
	T3."InstlmntID" = T0."SourceLine" ))  WHERE  T0."RefDate" <= ''' || :dateValue || ''' AND  T2."CardType" =''C''  AND  T2."Balance" <> 0  AND  T2."CardCode" in ' || :customerIds || ' AND  (T0."BalDueCred" <> T0."BalDueDeb"  OR  T0."BalFcCred" <> T0."BalFcDeb" ) AND   NOT EXISTS (SELECT U0."TransId", U0."TransRowId" FROM  
	ITR1 U0  INNER  JOIN OITR U1  ON  U1."ReconNum" = U0."ReconNum"   WHERE T0."TransId" = U0."TransId"  AND  T0."Line_ID" = U0."TransRowId"  AND  U1."ReconDate" > 
	''' || :reconDateValue || ''')   GROUP BY T0."TransId", T0."Line_ID")';
	--insert into debug values (:execSQL);
	exec (:execSQL);
Else 
	execSQL:= '
	Insert into "Customer_Open_Item_List_TempOpenDocumentViewByDocDate" (
	SELECT Max(T5."DocEntry") as DocEntry, T0."TransId", T0."Line_ID", Max("InstlmntID") as InstlmntID, MAX(T0."Account") as Account, MAX(T0."ShortName") as ShortName, MAX(T4."Address") as Address, MAX(T4."Phone1") as Phone, MAX(T0."TransType") as TransType, MAX(T0."CreatedBy") as CreatedBy, MAX(T0."BaseRef") as BaseRef, MAX(T0."SourceLine") as SourceLine, MAX(T0."Debit") as Debit, Max(T0."Credit") as Credit, Max(T0."FCDebit") as FCDebit, Max(T0."FCCredit") as FCCredit,Max(T0."DebCred") as DebCred,
	MAX(T0."RefDate") as RefDate , MAX(T0."DueDate") as DueDate, MAX(T0."TaxDate") as TaxDate, (MAX(T0."BalDueCred") + SUM(T1."ReconSum")) as BalDueCred, (MAX(T0."BalFcCred") + SUM(T1."ReconSumFC")) as BalFCCred,( MAX(T0."BalScCred") + 
	SUM(T1."ReconSumSC")) as BalScCred, MAX(T0."LineMemo") as LineMemo, MAX(T0."Indicator") as Indicator, MAX(T4."CardName") as CardName, MAX(T5."CardCode") as CardCode, MAX(T5."CardName") as CardName1, 
	MAX(T4."Balance") as Balance, (SUM(T0."Debit") + SUM(T0."Credit")) as DocTotal, SUM(T0."FCDebit") + SUM(T0."FCCredit") as DocTotalFC, SUM(T0."SYSDeb") + 
	SUM(T0."SYSCred") as DocTotalSC, MAX(T5."DunnLevel") as DunnLevel , MAX(T5."IsSales") as IsSales, MAX(T4."Currency") as BPCurrency, MAX(T0."FCCurrency") as JEFCCurrency , 
	Max(T5."Installmnt") as Installmnt, Max(T5."DunDate") as DunDate, Max(T5."InsTotal") as InsTotal, Max(T5."InsTotalFC") as InsTotalFC, Max(T5."InsTotalSy") as InsTotalSy
	FROM  JDT1 T0  INNER  JOIN ITR1 T1  ON  T1."TransId" = T0."TransId"  AND  T1."TransRowId" = T0."Line_ID"   INNER  JOIN 
	OITR T2  ON  T2."ReconNum" = T1."ReconNum"   INNER  JOIN OJDT T3  ON  T3."TransId" = T0."TransId"   INNER  JOIN OCRD T4  ON  T4."CardCode" = T0."ShortName"  
	LEFT OUTER  JOIN "Customer_Open_Item_List_TempJournalSource" T5  ON  T5."ObjType" = T0."TransType"  AND  T5."DocEntry" = T0."CreatedBy"  AND  (T5."TransType" <> N''I''  OR  (T5."TransType" = N''I'' 
	AND  T5."InstlmntID" = T0."SourceLine" ))  WHERE T0."TaxDate" <=  ''' || :dateValue || '''  AND  T4."CardType" = ''C''  AND  T4."Balance" <>0  AND  T4."CardCode"  in ' || :customerIds || '  AND  T2."ReconDate" > ''' || :reconDateValue || ''' AND  T1."IsCredit" = ''C''  GROUP BY T0."TransId", T0."Line_ID" HAVING MAX(T0."BalFcCred") <>-	SUM(T1."ReconSumFC")  OR  MAX(T0."BalDueCred") <>- SUM(T1."ReconSum")  
	Union All
	SELECT Max(T5."DocEntry") as DocEntry, T0."TransId", T0."Line_ID", Max("InstlmntID") as InstlmntID, MAX(T0."Account") as Account, MAX(T0."ShortName") as ShortName, MAX(T4."Address") as Address, MAX(T4."Phone1") as Phone,MAX(T0."TransType") as TransType, MAX(T0."CreatedBy") as CreatedBy, MAX(T0."BaseRef") as BaseRef, MAX(T0."SourceLine") as SourceLine, MAX(T0."Debit") as Debit, Max(T0."Credit") as Credit, Max(T0."FCDebit") as FCDebit, Max(T0."FCCredit") as FCCredit,Max(T0."DebCred") as DebCred,
	MAX(T0."RefDate") as RefDate, MAX(T0."DueDate") as DueDate, MAX(T0."TaxDate") as TaxDate,  (- MAX(T0."BalDueDeb") - SUM(T1."ReconSum")) as BalDueCred,  (- MAX(T0."BalFcDeb") - SUM(T1."ReconSumFC")) as BalFCCred,  (- MAX(T0."BalScDeb") - SUM(T1."ReconSumSC")) as BalScCred, MAX(T0."LineMemo") as LineMeno, MAX(T0."Indicator") as Indicator, MAX(T4."CardName") as CardName, MAX(T5."CardCode") as CardCode, MAX(T5."CardName") as CardName1, MAX(T4."Balance") as Balance,(SUM(T0."Debit") + 
	SUM(T0."Credit")) as DocTotal, (SUM(T0."FCDebit") + SUM(T0."FCCredit")) as DocTotalFC, (SUM(T0."SYSDeb") + SUM(T0."SYSCred")) as  DocTotalSC, MAX(T5."DunnLevel") as DunnLevel,
	MAX(T5."IsSales") as IsSales, MAX(T4."Currency") as BPCurrency, MAX(T0."FCCurrency") as JEFCCurrency, Max(T5."Installmnt") as Installmnt, Max(T5."DunDate") as DunDate
	, Max(T5."InsTotal") as InsTotal, Max(T5."InsTotalFC") as InsTotalFC, Max(T5."InsTotalSy") as InsTotalSy
	FROM  JDT1 T0  INNER  JOIN ITR1 T1  ON  
	T1."TransId" = T0."TransId"  AND  T1."TransRowId" = T0."Line_ID"   INNER  JOIN OITR T2  ON  T2."ReconNum" = T1."ReconNum"  INNER  JOIN OJDT T3  ON  T3."TransId" = 
	T0."TransId"   INNER  JOIN OCRD T4  ON  T4."CardCode" = T0."ShortName"    LEFT OUTER  JOIN "Customer_Open_Item_List_TempJournalSource" T5  ON  T5."ObjType" = T0."TransType"  AND  
	T5."DocEntry" = T0."CreatedBy"  AND  (T5."TransType" <> N''I''  OR  (T5."TransType" = N''I''  AND  T5."InstlmntID" = T0."SourceLine" ))  WHERE T0."TaxDate" <= ''' || :dateValue || ''' 
	AND  T4."CardType" = ''C''  AND  T4."Balance" <>0  AND  T4."CardCode" in ' || :customerIds || ' AND  T2."ReconDate" > ''' || :reconDateValue || ''' AND  T1."IsCredit" = ''D''   GROUP BY T0."TransId", T0."Line_ID" HAVING MAX(T0."BalFcDeb") <>- SUM(T1."ReconSumFC")  OR  MAX(T0."BalDueDeb") <>- SUM(T1."ReconSum")   
	UNION ALL 
	SELECT (Case IfNull(Max(T3."DocEntry"), -1) when -1  Then T0."TransId" Else Max(T3."DocEntry") end )  as "DocEntry", T0."TransId", T0."Line_ID", Max("InstlmntID") as "InstlmntID", MAX(T0."Account") as Account, MAX(T0."ShortName") as ShortName, MAX(T2."Address") as Address, MAX(T2."Phone1") as Phone,MAX(T0."TransType") as "TransType", MAX(T0."CreatedBy") as "CreatedBy", MAX(T0."BaseRef") as "BaseRef", MAX(T0."SourceLine") as "SourceLine" ,MAX(T0."Debit") as "Debit", Max(T0."Credit") as "Credit", Max(T0."FCDebit") as "FCDebit", Max(T0."FCCredit") as "FCCredit",Max(T0."DebCred") as "DebCred",
	MAX(T0."RefDate")  as "RefDate", MAX(T0."DueDate") as "DueDate", MAX(T0."TaxDate") as "TaxDate", (MAX(T0."BalDueCred") - MAX(T0."BalDueDeb")) as "BalDueCred", (MAX(T0."BalFcCred") - MAX(T0."BalFcDeb")) as "BalFCCred", (MAX(T0."BalScCred") - MAX(T0."BalScDeb")) as "BalSCCred", MAX(T0."LineMemo") as "LineMemo" ,MAX(T0."Indicator") as "Indicator", MAX(T2."CardName") as "CardName", MAX(T3."CardCode") as "CardCode", MAX(T3."CardName") as "CardName1", MAX(T2."Balance") as "Balance", 
	(SUM(T0."Debit") + SUM(T0."Credit")) as "DocTotal",( SUM(T0."FCDebit") + SUM(T0."FCCredit")) as "DocTotalFC",( SUM(T0."SYSDeb") + SUM(T0."SYSCred")) as "DocTotalSC", 
	MAX(T3."DunnLevel") as "DunnLevel", MAX(T3."IsSales") as "IsSales", MAX(T2."Currency") as "BPCurrency", MAX(T0."FCCurrency") as "JEFCCurrency",
	Max(T3."Installmnt") as "Installmnt", Max(T3."DunDate") as "DunDate", Max(T3."InsTotal") as "InsTotal", Max(T3."InsTotalFC") as "InsTotalFC", Max(T3."InsTotalSy") as "InsTotalSy"
	FROM JDT1 T0 INNER JOIN OJDT T1  ON  T1."TransId" = T0."TransId"   INNER  JOIN OCRD T2  ON  T2."CardCode" = T0."ShortName"    LEFT 
	OUTER  JOIN  "Customer_Open_Item_List_TempJournalSource" T3  ON  T3."ObjType" = T0."TransType"  AND  T3."DocEntry" = T0."CreatedBy"  AND  (T3."TransType" <> N''I''  OR  (T3."TransType" = N''I''  AND  
	T3."InstlmntID" = T0."SourceLine" ))  WHERE T0."TaxDate" <= ''' || :dateValue || ''' AND  T2."CardType" = ''C''  AND  T2."Balance" <> 0  AND  T2."CardCode" in ' || :customerIds || ' AND  (T0."BalDueCred" <> T0."BalDueDeb"  OR  T0."BalFcCred" <> T0."BalFcDeb" ) AND   NOT EXISTS (SELECT U0."TransId", U0."TransRowId" FROM  
	ITR1 U0  INNER  JOIN OITR U1  ON  U1."ReconNum" = U0."ReconNum"   WHERE T0."TransId" = U0."TransId"  AND  T0."Line_ID" = U0."TransRowId"  AND  U1."ReconDate" > 
	''' || :reconDateValue || ''')   GROUP BY T0."TransId", T0."Line_ID"
	)';
	--insert into debug values (:execSQL);
	exec (:execSQL);
End If;
	
If :dateType = '1' Then
	tableName := '"Customer_Open_Item_List_TempOpenDocumentViewByPosting"';
Else
	tableName := '"Customer_Open_Item_List_TempOpenDocumentViewByDocDate"';
End If;

execSQL := '	
	insert into "CRSP_Customer_Open_Item_List_TmpResult" Select
	T0."Address", T0."BalDueCred", T0."BalFCCred", T0."BaseRef", T0."BPCurrency", 
	T0."CardCode", T0."CardName", T0."Credit", T0."Debit", T0."DebCred", T0."DocEntry",
	T0."DunDate", T0."DueDate", T0."DunnLevel",T0."FCCredit", T0."FCDebit", 
	T0."Installmnt", T0."InstlmntID", T0."InsTotal", T0."InsTotalFC", T0."InsTotalSy",T1."IsCredit", 
	T0."FCCurrency",T0."Line_ID", T0."LineMemo",T0."Phone",T4."ReconBaseRef",T0."RefDate",
	T4."ReconTransId",T4."ReconRefDate",T4."ReconDocumentDate",T5."ReconJrnlMemo",T5."ReconDebCred", 
	T5."ReconDebit", T5."ReconCredit",T5."ReconDebitFc", T5."ReconCreditFC" , T5."ReconJEFCCurrency",
	T2."ReconDate", T2."ReconCurr", T0."ShortName", T1."SrcObjAbs", T1."SrcObjTyp", T0."TaxDate", T0."TransType",
	CURRENT_DATE as ServerDate,T3."Name" as IndicatorName , (Case IFNULL("ReconDate", ''19700101'') When ''19700101'' Then 0 Else "ReconSum" end ) as realReconSum ,
	(Case IFNull("ReconDate", ''19700101'') When ''19700101'' Then 0 Else "ReconSumFC" end ) as realReconSumFC, ''N'' as IsDPMRequest from ' || :tableName || ' T0 
	Left Outer  Join ITR1 T1 On T1."ReconNum" in (Select U0."ReconNum" from ITR1 U0 where U0."TransId" = T0."TransId" and U0."TransRowId" = T0."Line_ID") and (T1."TransId"<> T0."TransId" Or T1."TransRowId"<> T0."Line_ID")
	Left Outer Join OITR T2 On T2."ReconNum" = T1."ReconNum" and  T2."ReconDate" <= ''' || :reconDateValue || '''
	Left Outer Join (Select "TransId" as "ReconTransId", "BaseRef" as "ReconBaseRef", "RefDate" as "ReconRefDate", "TaxDate" as "ReconDocumentDate"  From OJDT) T4 On T4."ReconTransId" = T1."TransId" 
	Left Outer Join (Select "TransId" as "ReconTransId1", "Line_ID" as "ReconJELine_Id", "LineMemo" as "ReconJrnlMemo", "DebCred" as "ReconDebCred", "Debit" as "ReconDebit", "Credit" as "ReconCredit", "FCDebit" as "ReconDebitFc", "FCCredit" as "ReconCreditFC" , "FCCurrency" as "ReconJEFCCurrency"  From JDT1) T5 on T5."ReconTransId1" = T4."ReconTransId" and T5."ReconJELine_Id" = T1."TransRowId"
	Left Outer Join  OIDC  T3 On T3."Code" = T0."Indicator"';

if :includeDPMRequest = 'Y' Then
	 If :dateType = '1' Then
	 	dateField := '"DocDate"';
	 Else 
	 	dateField := '"TaxDate"';
	 End IF;

	 execSQL := :execSQL || ' Union All ';
 	 execSQL := :execSQL || ' Select T4."Address",0 as BalDueCred,0 as BalFCCred, T0."DocNum" as BaseRef, T4."Currency" as BPCurrency, 
	T4."CardCode", T4."CardName", 0 as Credit, 0 as Debit, '''' as DebCred, T0."DocEntry",
	T3."DunDate", T0."DocDueDate" as DueDate, T3."DunnLevel", 0 as FCCredit, 0 as FCDebit, 
	-1 as Installmnt, -1 as InstlmntID, T3."InsTotal", T3."InsTotalFC", T3."InsTotalSy",'''' as IsCredit, 
	'''' as JEFCCurrency, -1 as Line_ID, T0."JrnlMemo",T4."Phone1",'''' as ReconBaseRef,T0."DocDate" as RefDate,
	-1 as ReconTransID, NULL as ReconRefDate,NULL as ReconDocumentDate,NULL as ReconJrnlMemo, '''' as ReconDebCred, 
	0 as ReconDebit, 0 as ReconCredit, 0 as ReconDebitFc, 0 as ReconCreditFC , T0."DocCur" as ReconJEFCCurrency,
	NULL as ReconDate, T0."CurSource" as ReconCurr, T4."CardCode" as ShortName, T0."DocEntry" as SrcObjAbs, ''203'' as SrcObjTyp, T0."TaxDate", ''203'' as TransType,
	CURRENT_DATE as ServerDate,T5."Name" as IndicatorName ,0 as realReconSum , 0 as realRconSumFC, ''Y'' as IsDPMRequest
	From ODPI T0 Inner Join DPI6 T3 On T0."CreateTran" =''N'' and T0."DocEntry" = T3."DocEntry" and T0."DocEntry" in
	(
	Select U0."DocEntry" from odpi U0 Left Outer Join RCT2 T1 On U0."CreateTran" =''N'' and T1."InvType" = ''203'' and T1."DocEntry" = U0."DocEntry"
	Join ORCT T2 on T2."DocEntry" = T1."DocNum" Where TO_DATE(T2. ' || :dateField || ') <= ''' ||  :dateValue || ''' Or U0."DocStatus" = ''O'' 
	Group By U0."DocEntry" 
	Having Sum(T1."SumApplied") <> Max(U0."DocTotal") Or Max(U0."DocStatus") = ''O''
	) 
	Inner Join OCRD T4 On T4."CardCode" = T0."CardCode"
	Left Outer Join OIDC T5 On T5."Code" = T0."Indicator" ';

END IF;

--insert into debug values (:execSQL);
exec(:execSQL);

select * from "CRSP_Customer_Open_Item_List_TmpResult" order by "RefDate", "BaseRef", "ReconDate";
delete from "CRSP_Customer_Open_Item_List_TmpResult";

END;











