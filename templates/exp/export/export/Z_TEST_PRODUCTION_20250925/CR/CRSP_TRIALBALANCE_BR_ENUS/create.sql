-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CRSP_TrialBalanceByCostCenter_BR_enUS_cont
CREATE PROCEDURE CRSP_TrialBalance_BR_enUS
(
in TemplateID VARCHAR(20),
in in_Currency VARCHAR(10),
in in_IgnoreAdj VARCHAR(1),
in in_AddVoucher VARCHAR(1),
in in_DateType varchar(256),
in in_FromDate timestamp,
in in_ToDate timestamp,
in in_DimCode nvarchar(10)
) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
Plan varchar(20);
MinGroupMask integer;
PlanType varchar(1);
Currency varchar(10);
IgnoreAdj varchar(1);
AddVoucher varchar(1);
DateType varchar(256);
FromDate timestamp;
ToDate timestamp;
DimCode nvarchar(10);
BroughtForward varchar(1);
EndYear varchar(1);
MainCurr varchar(3);
SysCurr varchar(3);
FinancYear timestamp;
BEGIN /*
    
***** General Information *****
Name: JDT1_AccountCalc (including Preparation for account structure)
Description: Return Journal Entries distributed according to distribution rules to cost centers
with relevant filters.
This datasource includes branches for intercompany only supported from SAP B1 8.81 PL10 or higher
Creator: coresystems ag for SAP AG, muf@coresystems.ch
Create Date: 2011-06-22
*/
    Plan := :TemplateID;
    /*
     Here we define the type of the report (P = Profit and Loss) and the Minimum Group Mask for Profit and Loss accounts 
This can be different for different localizations a mapping should be included. 
Therefore it is not supported in the official Version. 
It is kept in the datasource to enable this option in case a customer needs it. Then the MinGroupMask can be adjusted. 
    */
    MinGroupMask := 3;
    PlanType := 'P';
    /* Select which currency to display in the report. Possible values are Loc and Sys.*/
    Currency := :in_Currency;
    /*
     Include Journal Entries with checkbox "Ignore Adjustments (13. Per)" selected (Y) or not (N). Param '[%IgnoreAdj]' 
    */
    IgnoreAdj := :in_IgnoreAdj;
    IF :IgnoreAdj = '0' THEN 
        IgnoreAdj := 'N';
    ELSE 
        IgnoreAdj := 'Y';
        /* Include Journal Vouchers (Y) or not (N). */
    END IF;
    AddVoucher := :in_AddVoucher;
    IF :AddVoucher = '0' THEN 
        AddVoucher := 'N';
    ELSE 
        AddVoucher := 'Y';
        /* Date Type to set the filter and sort by. Possible values are RefDate, TaxDate and DueDate. */
    END IF;
    DateType := :in_DateType;
    /* Date Filter From and To Dates. Format 'YYYYMMDD' */
    FromDate := :in_FromDate;
    ToDate := :in_ToDate;
    /* Dimension new since Version 8.81.*/
    DimCode := in_DimCode;
    /* Include Amount Brought Forward (Y) or not (N). */
    /* Hardcoded for this report */
    BroughtForward := 'Y';
    /* Include End Year Closing (Y) or not (N). */
    /* Hardcoded for this report */
    EndYear := 'Y';
    /* Internal Parameters */
    /* Local Currency */
    SELECT 
    (
    	SELECT TOP 1 T2."MainCurncy" FROM OADM T2
    ) INTO MainCurr FROM DUMMY;
    /* System Currency */
    SELECT
    (
    	SELECT TOP 1 T2."SysCurrncy" FROM OADM T2
    ) INTO SysCurr FROM DUMMY;
    /* The Finance Year is determined by the RefDate no matter how the parameter DateType is set */
    SELECT 
    (
    	SELECT T0."FinancYear" FROM OACP T0 
        INNER JOIN OFPR T1 ON T0."PeriodCat" = T1."Category" 
    	WHERE T1."F_RefDate" <= :ToDate AND T1."T_RefDate" >= :ToDate
    ) INTO FinancYear FROM DUMMY;
    /* Account structure preparation */
    insert into "CRSP_TemplateAccount" 
    (
    	SELECT CAST(T0."CatId" AS nvarchar(32)) AS "Code", CAST(T0."FatherNum" AS nvarchar(32)) AS "FatherNum", 
        T0."Name" AS "Description", T0."Levels" AS "Lvl", T0."VisOrder" * 10000 AS "Sort", T2."DocType", 
        'N' AS "Postable", CAST(T0."CatId" AS nvarchar(32)) AS "FormatCode", 
        CAST(T0."CatId" AS nvarchar(32)) AS "AcctCodeDisp", CAST(T0."CatId" AS nvarchar(32)) AS "Segment_0", 
        CAST(T0."CatId" AS nvarchar(32)) AS "Segment_1", CAST(T0."CatId" AS nvarchar(32)) AS "Segment_2", 
        CAST(T0."CatId" AS nvarchar(32)) AS "Segment_3", CAST(T0."CatId" AS nvarchar(32)) AS "Segment_4", 
        CAST(T0."CatId" AS nvarchar(32)) AS "Segment_5", CAST(T0."CatId" AS nvarchar(32)) AS "Segment_6", 
        CAST(T0."CatId" AS nvarchar(32)) AS "Segment_7", CAST(T0."CatId" AS nvarchar(32)) AS "Segment_8", 
        CAST(T0."CatId" AS nvarchar(32)) AS "Segment_9", '' AS "ActCurr", 0 AS "GroupMask", 
        CAST(T0."CatId" AS nvarchar(32)) AS "AccntntCod", T0."FrgnName" 
    	FROM OFRC T0 
        INNER JOIN OFRT T2 ON T0."TemplateId" = T2."AbsId" 
    	WHERE CAST(T0."TemplateId" AS nvarchar) = :Plan 
    	
    	UNION 
    	
    	SELECT CAST(LTRIM(RTRIM(IFNULL(T1."FormatCode", T1."AcctCode"))) AS nvarchar(32)) AS "Code", 
        CAST(T0."CatId" AS nvarchar(32)) AS "FatherNum", T1."AcctName" AS "Description", 
        (
        	SELECT MAX("Levels") FROM OFRC 
        	WHERE "CatId" = T0."CatId" AND CAST("TemplateId" AS nvarchar) = :Plan
        ) + 1 AS "Lvl", 
        (
        	SELECT MAX("VisOrder") * 10000 
        	FROM OFRC 
        	WHERE "CatId" = T0."CatId" AND CAST("TemplateId" AS nvarchar) = :Plan) + T0."VisOrder" AS "Sort", 
        	T2."DocType", 'Y' AS "Postable", LTRIM(RTRIM(IFNULL(T1."FormatCode", T1."AcctCode"))) AS "FormatCode", 
        	CASE 
           		WHEN T1."FormatCode" IS NULL THEN LTRIM(RTRIM(T1."AcctCode")) 
           		WHEN T1."FormatCode" = T1."AcctCode" THEN LTRIM(RTRIM(T1."AcctCode")) 
           		ELSE REPLACE(IFNULL(T1."Segment_0", 'x') || 
           		(SELECT TOP 1 "ActSep" FROM OADM) || IFNULL(T1."Segment_1", 'x') || 
           		(SELECT TOP 1 "ActSep" FROM OADM) || IFNULL(T1."Segment_2", 'x') || 
           		(SELECT TOP 1 "ActSep" FROM OADM) || IFNULL(T1."Segment_3", 'x') ||
           		(SELECT TOP 1 "ActSep" FROM OADM) || IFNULL(T1."Segment_4", 'x') || 
           		(SELECT TOP 1 "ActSep" FROM OADM) || IFNULL(T1."Segment_5", 'x') ||
           		(SELECT TOP 1 "ActSep" FROM OADM) || IFNULL(T1."Segment_6", 'x') ||
           		(SELECT TOP 1 "ActSep" FROM OADM) || IFNULL(T1."Segment_7", 'x') ||
           		(SELECT TOP 1 "ActSep" FROM OADM) || IFNULL(T1."Segment_8", 'x') ||
           		(SELECT TOP 1 "ActSep" FROM OADM) || IFNULL(T1."Segment_9", 'x'), 
           		(SELECT TOP 1 "ActSep" FROM OADM) || 'x', '') 
        	END AS "AcctCodeDisp", T1."Segment_0", T1."Segment_1", T1."Segment_2", T1."Segment_3", T1."Segment_4", 
        	T1."Segment_5", T1."Segment_6", T1."Segment_7", T1."Segment_8", T1."Segment_9", T1."ActCurr", T1."GroupMask", 
        	T1."AccntntCod", T1."FrgnName" 
    		FROM FRC1 T0 
 	    	INNER JOIN OACT T1 ON T0."AcctCode" = T1."AcctCode" 
    	   	INNER JOIN OFRT T2 ON T0."TemplateId" = T2."AbsId" 
    		WHERE CAST(T0."TemplateId" AS nvarchar) = :Plan
    );
    /* The Accounts (including Template) */
    insert into "CRSP_AcctLang"
    (
    	SELECT "AcctCode" AS "Code", "AcctName" AS "Description" 
    	FROM OACT 
    	WHERE :Plan = '@@Account' 
    	UNION 
    	SELECT "Code", "Description" 
    	FROM "CRSP_TemplateAccount" 
    	WHERE :Plan <> '@@Account'
    );

	insert into "CRSP_Plan0"
	(
    	/* OACT Structure */
    	SELECT T0."Levels" AS "Lvl", T0."AcctCode" AS "AcctCode", T0."AcctName" AS "Description", T0."Postable", 
        0.0 AS "Variator", '' AS "Liq", 
        CASE 
            WHEN T0."Levels" = 1 THEN LTRIM(RTRIM(T0."FormatCode")) 
            WHEN T0."Levels" = 2 THEN LTRIM(RTRIM(IFNULL(T1."FormatCode", T1."AcctCode"))) 
            WHEN T0."Levels" = 3 THEN LTRIM(RTRIM(IFNULL(T2."FormatCode", T2."AcctCode"))) 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(IFNULL(T3."FormatCode", T3."AcctCode"))) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(IFNULL(T4."FormatCode", T4."AcctCode"))) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(IFNULL(T5."FormatCode", T5."AcctCode"))) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(IFNULL(T6."FormatCode", T6."AcctCode"))) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(IFNULL(T7."FormatCode", T7."AcctCode"))) 
            ELSE '' 
        END AS "Level1", 
        CASE 
            WHEN T0."Levels" = 2 THEN LTRIM(RTRIM(T0."FormatCode")) 
            WHEN T0."Levels" = 3 THEN LTRIM(RTRIM(IFNULL(T1."FormatCode", T1."AcctCode"))) 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(IFNULL(T2."FormatCode", T2."AcctCode"))) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(IFNULL(T3."FormatCode", T3."AcctCode"))) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(IFNULL(T4."FormatCode", T4."AcctCode"))) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(IFNULL(T5."FormatCode", T5."AcctCode"))) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(IFNULL(T6."FormatCode", T6."AcctCode"))) 
            ELSE '' 
        END AS "Level2", 
        CASE 
            WHEN T0."Levels" = 3 THEN LTRIM(RTRIM(T0."FormatCode")) 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(IFNULL(T1."FormatCode", T1."AcctCode"))) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(IFNULL(T2."FormatCode", T2."AcctCode"))) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(IFNULL(T3."FormatCode", T3."AcctCode"))) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(IFNULL(T4."FormatCode", T4."AcctCode"))) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(IFNULL(T5."FormatCode", T5."AcctCode"))) 
            ELSE '' 
        END AS "Level3", 
        CASE 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(T0."FormatCode")) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(IFNULL(T1."FormatCode", T1."AcctCode"))) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(IFNULL(T2."FormatCode", T2."AcctCode"))) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(IFNULL(T3."FormatCode", T3."AcctCode"))) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(IFNULL(T4."FormatCode", T4."AcctCode"))) 
            ELSE '' 
        END AS "Level4", 
        CASE 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T0."FormatCode")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(IFNULL(T1."FormatCode", T1."AcctCode"))) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(IFNULL(T2."FormatCode", T2."AcctCode"))) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(IFNULL(T3."FormatCode", T3."AcctCode"))) 
            ELSE '' 
        END AS "Level5", 
        CASE 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T0."FormatCode")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(IFNULL(T1."FormatCode", T1."AcctCode"))) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(IFNULL(T2."FormatCode", T2."AcctCode"))) 
            ELSE '' 
        END AS "Level6", 
        CASE 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T0."FormatCode")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(IFNULL(T1."FormatCode", T1."AcctCode"))) 
            ELSE '' 
        END AS "Level7", 
        CASE 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T0."FormatCode")) 
            ELSE '' 
        END AS "Level8", 
        CASE 
            WHEN T0."Levels" = 1 THEN LTRIM(RTRIM(T0."AccntntCod")) 
            WHEN T0."Levels" = 2 THEN LTRIM(RTRIM(T1."AccntntCod")) 
            WHEN T0."Levels" = 3 THEN LTRIM(RTRIM(T2."AccntntCod")) 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(T3."AccntntCod")) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T4."AccntntCod")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T5."AccntntCod")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T6."AccntntCod")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T7."AccntntCod")) 
            ELSE '' 
        END AS "Level1AccntntCod", 
        CASE 
            WHEN T0."Levels" = 2 THEN LTRIM(RTRIM(T0."AccntntCod")) 
            WHEN T0."Levels" = 3 THEN LTRIM(RTRIM(T1."AccntntCod")) 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(T2."AccntntCod")) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T3."AccntntCod")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T4."AccntntCod")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T5."AccntntCod")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T6."AccntntCod")) 
            ELSE '' 
        END AS "Level2AccntntCod", 
        CASE 
            WHEN T0."Levels" = 3 THEN LTRIM(RTRIM(T0."AccntntCod")) 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(T1."AccntntCod")) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T2."AccntntCod")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T3."AccntntCod")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T4."AccntntCod")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T5."AccntntCod")) 
            ELSE '' 
        END AS "Level3AccntntCod", 
        CASE 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(T0."AccntntCod")) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T1."AccntntCod")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T2."AccntntCod")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T3."AccntntCod")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T4."AccntntCod")) 
            ELSE '' 
        END AS "Level4AccntntCod", 
        CASE 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T0."AccntntCod")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T1."AccntntCod")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T2."AccntntCod")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T3."AccntntCod")) 
            ELSE '' 
        END AS "Level5AccntntCod", 
        CASE 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T0."AccntntCod")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T1."AccntntCod")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T2."AccntntCod")) 
            ELSE '' 
        END AS "Level6AccntntCod", 
        CASE 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T0."AccntntCod")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T1."AccntntCod")) 
            ELSE '' 
        END AS "Level7AccntntCod", 
        CASE 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T0."AccntntCod")) 
            ELSE '' 
        END AS "Level8AccntntCod", 
        CASE 
            WHEN T0."Levels" = 1 THEN LTRIM(RTRIM(T0."FrgnName")) 
            WHEN T0."Levels" = 2 THEN LTRIM(RTRIM(T1."FrgnName")) 
            WHEN T0."Levels" = 3 THEN LTRIM(RTRIM(T2."FrgnName")) 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(T3."FrgnName")) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T4."FrgnName")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T5."FrgnName")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T6."FrgnName")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T7."FrgnName")) 
            ELSE '' 
        END AS "Level1FrgnName", 
        CASE 
            WHEN T0."Levels" = 2 THEN LTRIM(RTRIM(T0."FrgnName")) 
            WHEN T0."Levels" = 3 THEN LTRIM(RTRIM(T1."FrgnName")) 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(T2."FrgnName")) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T3."FrgnName")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T4."FrgnName")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T5."FrgnName")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T6."FrgnName")) 
            ELSE '' 
        END AS "Level2FrgnName", 
        CASE 
            WHEN T0."Levels" = 3 THEN LTRIM(RTRIM(T0."FrgnName")) 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(T1."FrgnName")) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T2."FrgnName")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T3."FrgnName")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T4."FrgnName")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T5."FrgnName")) 
            ELSE '' 
        END AS "Level3FrgnName", 
        CASE 
            WHEN T0."Levels" = 4 THEN LTRIM(RTRIM(T0."FrgnName")) 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T1."FrgnName")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T2."FrgnName")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T3."FrgnName")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T4."FrgnName")) 
            ELSE '' 
        END AS "Level4FrgnName", 
        CASE 
            WHEN T0."Levels" = 5 THEN LTRIM(RTRIM(T0."FrgnName")) 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T1."FrgnName")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T2."FrgnName")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T3."FrgnName")) 
            ELSE '' 
        END AS "Level5FrgnName", 
        CASE 
            WHEN T0."Levels" = 6 THEN LTRIM(RTRIM(T0."FrgnName")) 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T1."FrgnName")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T2."FrgnName")) 
            ELSE '' 
        END AS "Level6FrgnName", 
        CASE 
            WHEN T0."Levels" = 7 THEN LTRIM(RTRIM(T0."FrgnName")) 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T1."FrgnName")) 
            ELSE '' 
        END AS "Level7FrgnName", 
        CASE 
            WHEN T0."Levels" = 8 THEN LTRIM(RTRIM(T0."FrgnName")) 
            ELSE '' 
        END AS "Level8FrgnName", 
        T0."GroupMask" * 10000 + T0."GrpLine" AS "Sort", CAST(T0."FatherNum" AS nvarchar(32)) AS "FatherId", CAST(T0."AcctCode" AS nvarchar(32)) AS "Id", 
        LTRIM(RTRIM(IFNULL(T0."FormatCode", T0."AcctCode"))) AS "FormatCode", 
        CASE 
            WHEN T0."FormatCode" IS NULL THEN LTRIM(RTRIM(T0."AcctCode")) 
            WHEN T0."FormatCode" = T0."AcctCode" THEN LTRIM(RTRIM(T0."AcctCode")) 
            ELSE REPLACE(IFNULL(T0."Segment_0", 'x') || 
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || IFNULL(T0."Segment_1", 'x') || 
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || IFNULL(T0."Segment_2", 'x') || 
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || IFNULL(T0."Segment_3", 'x') ||
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || IFNULL(T0."Segment_4", 'x') ||
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || IFNULL(T0."Segment_5", 'x') ||
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || IFNULL(T0."Segment_6", 'x') ||
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || IFNULL(T0."Segment_7", 'x') ||
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || IFNULL(T0."Segment_8", 'x') ||
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || IFNULL(T0."Segment_9", 'x'), 
            (SELECT TOP 1 "ActSep" 
            FROM OADM) || 'x', '') 
        END AS "AcctCodeDisp", 
        T0."Segment_0", T0."Segment_1", T0."Segment_2", T0."Segment_3", T0."Segment_4", T0."Segment_5", T0."Segment_6", T0."Segment_7", T0."Segment_8", T0."Segment_9", 
        T0."ActCurr", T0."GroupMask", 
        CASE 
            WHEN T0."GroupMask" >= :MinGroupMask THEN 'P' 
            ELSE 'B' 
        END AS "DocType", T0."AccntntCod", T0."FrgnName"
    	
    	FROM OACT T0 
        LEFT OUTER JOIN OACT T1 ON T0."FatherNum" = T1."AcctCode" 
        LEFT OUTER JOIN OACT T2 ON T1."FatherNum" = T2."AcctCode" 
        LEFT OUTER JOIN OACT T3 ON T2."FatherNum" = T3."AcctCode" 
        LEFT OUTER JOIN OACT T4 ON T3."FatherNum" = T4."AcctCode" 
        LEFT OUTER JOIN OACT T5 ON T4."FatherNum" = T5."AcctCode" 
        LEFT OUTER JOIN OACT T6 ON T5."FatherNum" = T6."AcctCode" 
        LEFT OUTER JOIN OACT T7 ON T6."FatherNum" = T7."AcctCode" 
    	WHERE :Plan = '@@Account' AND ((:PlanType = 'P' AND 
        CASE 
            WHEN T0."GroupMask" >= :MinGroupMask THEN 'P' 
            ELSE 'B' 
        END = 'P') OR (:PlanType = 'B' AND 
        CASE 
            WHEN T0."GroupMask" >= :MinGroupMask THEN 'P' 
            ELSE 'B' 
        END = 'B') OR :PlanType = '') 
    	
    	UNION 
    	
    	SELECT T0."Lvl", T0."Code" AS "AcctCode", T0."Description", T0."Postable", 0.0 AS "Variator", '' AS "Liq", 
        CASE 
            WHEN T0."Lvl" = 1 THEN T0."Code" 
            WHEN T0."Lvl" = 2 THEN T1."Code" 
            WHEN T0."Lvl" = 3 THEN T2."Code" 
            WHEN T0."Lvl" = 4 THEN T3."Code" 
            WHEN T0."Lvl" = 5 THEN T4."Code" 
            WHEN T0."Lvl" = 6 THEN T5."Code" 
            WHEN T0."Lvl" = 7 THEN T6."Code" 
            WHEN T0."Lvl" = 8 THEN T7."Code" 
            ELSE '' 
        END AS "Level1", 
        CASE 
            WHEN T0."Lvl" = 2 THEN T0."Code" 
            WHEN T0."Lvl" = 3 THEN T1."Code" 
            WHEN T0."Lvl" = 4 THEN T2."Code" 
            WHEN T0."Lvl" = 5 THEN T3."Code" 
            WHEN T0."Lvl" = 6 THEN T4."Code" 
            WHEN T0."Lvl" = 7 THEN T5."Code" 
            WHEN T0."Lvl" = 8 THEN T6."Code" 
            ELSE '' 
        END AS "Level2", 
        CASE 
            WHEN T0."Lvl" = 3 THEN T0."Code" 
            WHEN T0."Lvl" = 4 THEN T1."Code" 
            WHEN T0."Lvl" = 5 THEN T2."Code" 
            WHEN T0."Lvl" = 6 THEN T3."Code" 
            WHEN T0."Lvl" = 7 THEN T4."Code" 
            WHEN T0."Lvl" = 8 THEN T5."Code" 
            ELSE '' 
        END AS "Level3", 
        CASE 
            WHEN T0."Lvl" = 4 THEN T0."Code" 
            WHEN T0."Lvl" = 5 THEN T1."Code" 
            WHEN T0."Lvl" = 6 THEN T2."Code" 
            WHEN T0."Lvl" = 7 THEN T3."Code" 
            WHEN T0."Lvl" = 8 THEN T4."Code" 
            ELSE '' 
        END AS "Level4", 
        CASE 
            WHEN T0."Lvl" = 5 THEN T0."Code" 
            WHEN T0."Lvl" = 6 THEN T1."Code" 
            WHEN T0."Lvl" = 7 THEN T2."Code" 
            WHEN T0."Lvl" = 8 THEN T3."Code" 
            ELSE '' 
        END AS "Level5", 
        CASE 
            WHEN T0."Lvl" = 6 THEN T0."Code" 
            WHEN T0."Lvl" = 7 THEN T1."Code" 
            WHEN T0."Lvl" = 8 THEN T2."Code" 
            ELSE '' 
        END AS "Level6", 
        CASE 
            WHEN T0."Lvl" = 7 THEN T0."Code" 
            WHEN T0."Lvl" = 8 THEN T1."Code" 
            ELSE '' 
        END AS "Level7", 
        CASE 
            WHEN T0."Lvl" = 8 THEN T0."Code" 
            WHEN T0."Lvl" = 8 THEN T0."Code" 
            ELSE '' 
        END AS "Level8", 
        CASE 
            WHEN T0."Lvl" = 1 THEN T0."Code" 
            WHEN T0."Lvl" = 2 THEN T1."Code" 
            WHEN T0."Lvl" = 3 THEN T2."Code" 
            WHEN T0."Lvl" = 4 THEN T3."Code" 
            WHEN T0."Lvl" = 5 THEN T4."Code" 
            WHEN T0."Lvl" = 6 THEN T5."Code" 
            WHEN T0."Lvl" = 7 THEN T6."Code" 
            WHEN T0."Lvl" = 8 THEN T7."Code" 
            ELSE '' 
        END AS "Level1AccntntCod", 
        CASE 
            WHEN T0."Lvl" = 2 THEN T0."Code" 
            WHEN T0."Lvl" = 3 THEN T1."Code" 
            WHEN T0."Lvl" = 4 THEN T2."Code" 
            WHEN T0."Lvl" = 5 THEN T3."Code" 
            WHEN T0."Lvl" = 6 THEN T4."Code" 
            WHEN T0."Lvl" = 7 THEN T5."Code" 
            WHEN T0."Lvl" = 8 THEN T6."Code" 
            ELSE '' 
        END AS "Level2AccntntCod", 
        CASE 
            WHEN T0."Lvl" = 3 THEN T0."Code" 
            WHEN T0."Lvl" = 4 THEN T1."Code" 
            WHEN T0."Lvl" = 5 THEN T2."Code" 
            WHEN T0."Lvl" = 6 THEN T3."Code" 
            WHEN T0."Lvl" = 7 THEN T4."Code" 
            WHEN T0."Lvl" = 8 THEN T5."Code" 
            ELSE '' 
        END AS "Level3AccntntCod", 
        CASE 
            WHEN T0."Lvl" = 4 THEN T0."Code" 
            WHEN T0."Lvl" = 5 THEN T1."Code" 
            WHEN T0."Lvl" = 6 THEN T2."Code" 
            WHEN T0."Lvl" = 7 THEN T3."Code" 
            WHEN T0."Lvl" = 8 THEN T4."Code" 
            ELSE '' 
        END AS "Level4AccntntCod", 
        CASE 
            WHEN T0."Lvl" = 5 THEN T0."Code" 
            WHEN T0."Lvl" = 6 THEN T1."Code" 
            WHEN T0."Lvl" = 7 THEN T2."Code" 
            WHEN T0."Lvl" = 8 THEN T3."Code" 
            ELSE '' 
        END AS "Level5AccntntCod", 
        CASE 
            WHEN T0."Lvl" = 6 THEN T0."Code" 
            WHEN T0."Lvl" = 7 THEN T1."Code" 
            WHEN T0."Lvl" = 8 THEN T2."Code" 
            ELSE '' 
        END AS "Level6AccntntCod", 
        CASE 
            WHEN T0."Lvl" = 7 THEN T0."Code" 
            WHEN T0."Lvl" = 8 THEN T1."Code" 
            ELSE '' 
        END AS "Level7", 
        CASE 
            WHEN T0."Lvl" = 8 THEN T0."Code" 
            WHEN T0."Lvl" = 8 THEN T0."Code" 
            ELSE '' 
        END AS "Level8AccntntCod", 
        CASE 
            WHEN T0."Lvl" = 1 THEN T0."FrgnName" 
            WHEN T0."Lvl" = 2 THEN T1."FrgnName" 
            WHEN T0."Lvl" = 3 THEN T2."FrgnName" 
            WHEN T0."Lvl" = 4 THEN T3."FrgnName" 
            WHEN T0."Lvl" = 5 THEN T4."FrgnName" 
            WHEN T0."Lvl" = 6 THEN T5."FrgnName" 
            WHEN T0."Lvl" = 7 THEN T6."FrgnName" 
            WHEN T0."Lvl" = 8 THEN T7."FrgnName" 
            ELSE '' 
        END AS "Level1FrgnName", 
        CASE 
            WHEN T0."Lvl" = 2 THEN T0."FrgnName" 
            WHEN T0."Lvl" = 3 THEN T1."FrgnName" 
            WHEN T0."Lvl" = 4 THEN T2."FrgnName" 
            WHEN T0."Lvl" = 5 THEN T3."FrgnName" 
            WHEN T0."Lvl" = 6 THEN T4."FrgnName" 
            WHEN T0."Lvl" = 7 THEN T5."FrgnName" 
            WHEN T0."Lvl" = 8 THEN T6."FrgnName" 
            ELSE '' 
        END AS "Level2FrgnName", 
        CASE 
            WHEN T0."Lvl" = 3 THEN T0."FrgnName" 
            WHEN T0."Lvl" = 4 THEN T1."FrgnName" 
            WHEN T0."Lvl" = 5 THEN T2."FrgnName" 
            WHEN T0."Lvl" = 6 THEN T3."FrgnName" 
            WHEN T0."Lvl" = 7 THEN T4."FrgnName" 
            WHEN T0."Lvl" = 8 THEN T5."FrgnName" 
            ELSE '' 
        END AS "Level3FrgnName", 
        CASE 
            WHEN T0."Lvl" = 4 THEN T0."FrgnName" 
            WHEN T0."Lvl" = 5 THEN T1."FrgnName" 
            WHEN T0."Lvl" = 6 THEN T2."FrgnName" 
            WHEN T0."Lvl" = 7 THEN T3."FrgnName" 
            WHEN T0."Lvl" = 8 THEN T4."FrgnName" 
            ELSE '' 
        END AS "Level4FrgnName", 
        CASE 
            WHEN T0."Lvl" = 5 THEN T0."FrgnName" 
            WHEN T0."Lvl" = 6 THEN T1."FrgnName" 
            WHEN T0."Lvl" = 7 THEN T2."FrgnName" 
            WHEN T0."Lvl" = 8 THEN T3."FrgnName" 
            ELSE '' 
        END AS "Level5FrgnName", 
        CASE 
            WHEN T0."Lvl" = 6 THEN T0."FrgnName" 
            WHEN T0."Lvl" = 7 THEN T1."FrgnName" 
            WHEN T0."Lvl" = 8 THEN T2."FrgnName" 
            ELSE '' 
        END AS "Level6FrgnName", 
        CASE 
            WHEN T0."Lvl" = 7 THEN T0."FrgnName" 
            WHEN T0."Lvl" = 8 THEN T1."FrgnName" 
            ELSE '' 
        END AS "Level7FrgnName", 
        CASE 
            WHEN T0."Lvl" = 8 THEN T0."FrgnName" 
            WHEN T0."Lvl" = 8 THEN T0."FrgnName" 
            ELSE '' 
        END AS "Level8FrgnName", T0."Sort", CAST(T0."FatherNum" AS nvarchar(32)) AS "FatherId", 
        CAST(T0."Code" AS nvarchar(32)) AS "Id", T0."FormatCode", T0."AcctCodeDisp", T0."Segment_0", T0."Segment_1", 
        T0."Segment_2", T0."Segment_3", T0."Segment_4", T0."Segment_5", T0."Segment_6", T0."Segment_7", 
        T0."Segment_8", T0."Segment_9", T0."ActCurr", T0."GroupMask", T0."DocType", T0."AccntntCod", T0."FrgnName" 
    	FROM "CRSP_TemplateAccount" T0 
        LEFT OUTER JOIN "CRSP_TemplateAccount" T1 ON T0."FatherNum" = T1."Code" 
        LEFT OUTER JOIN "CRSP_TemplateAccount" T2 ON T1."FatherNum" = T2."Code" 
        LEFT OUTER JOIN "CRSP_TemplateAccount" T3 ON T2."FatherNum" = T3."Code" 
        LEFT OUTER JOIN "CRSP_TemplateAccount" T4 ON T3."FatherNum" = T4."Code" 
        LEFT OUTER JOIN "CRSP_TemplateAccount" T5 ON T4."FatherNum" = T5."Code" 
        LEFT OUTER JOIN "CRSP_TemplateAccount" T6 ON T5."FatherNum" = T6."Code" 
        LEFT OUTER JOIN "CRSP_TemplateAccount" T7 ON T6."FatherNum" = T7."Code" 
    	WHERE :Plan <> '@@Account'
    );
    /* This is important to know which Group Masks belong to Balance and which to Profit and Loss 
 It can change for different localizations */
    /* Here the actual AccountCalc Query starts and the account preparation ends*/
    /*
     Distribution Rule (OOCR, OCR1) 
 * Temp. Table with distribution rules, created with ValidFrom and ValidTo
 * Desc: If only 1 Rule per Date oder the last one, the ValidTo Column ist ToDay+99Years 
 */
    insert into "CRSP_Plan" 
    (
    	SELECT T1.*, (SELECT MAX("Lvl") FROM "CRSP_Plan0") AS "MaxLvl", T21."Description" AS "Desc1", T22."Description" AS "Desc2", 
        T23."Description" AS "Desc3", T24."Description" AS "Desc4", T25."Description" AS "Desc5", 
        T26."Description" AS "Desc6", T27."Description" AS "Desc7", T28."Description" AS "Desc8", 
        T1."Description" AS "DescAll", 
        CASE 
            WHEN (select :Plan from dummy) = '@@Account' THEN T1."AcctCode" 
            ELSE T1."Id"
        END AS "Code" 
    	FROM "CRSP_Plan0" T1
        LEFT OUTER JOIN "CRSP_AcctLang" T21 ON T1."Level1" = T21."Code" 
        LEFT OUTER JOIN "CRSP_AcctLang" T22 ON T1."Level2" = T22."Code" 
        LEFT OUTER JOIN "CRSP_AcctLang" T23 ON T1."Level3" = T23."Code" 
        LEFT OUTER JOIN "CRSP_AcctLang" T24 ON T1."Level4" = T24."Code" 
        LEFT OUTER JOIN "CRSP_AcctLang" T25 ON T1."Level5" = T25."Code" 
        LEFT OUTER JOIN "CRSP_AcctLang" T26 ON T1."Level6" = T26."Code" 
        LEFT OUTER JOIN "CRSP_AcctLang" T27 ON T1."Level7" = T27."Code" 
        LEFT OUTER JOIN "CRSP_AcctLang" T28 ON T1."Level8" = T28."Code" 
    	ORDER BY T1."Sort"
    );

    /* Amount Brought Forward */
    
    INSERT INTO "CRSP_OCR1DistRule"
    (
    	SELECT T0."OcrCode", T0."PrcCode", 
        CASE 
            WHEN T0."OcrTotal" = 0 THEN 1 
            ELSE T0."OcrTotal" 
        END AS "OcrTotal", 
        CASE 
            WHEN T0."OcrTotal" = 0 THEN 1 
            ELSE T0."PrcAmount" 
        END AS "PrcAmount", 
        CASE 
            WHEN T0."OcrTotal" = 0 THEN 1 
            ELSE T0."PrcAmount" 
        END / 
        CASE 
            WHEN T0."OcrTotal" = 0 THEN 1 
            ELSE T0."OcrTotal" 
        END AS "OcrFactor", T0."Direct", T0."ValidFrom", IFNULL
        (
        	(
        	SELECT ADD_DAYS(T1."ValidTo", -1) FROM
        	(
        		SELECT U0."OcrCode", U0."ValidFrom", MIN(U1."ValidFrom") AS "ValidTo"
        		FROM OCR1 U0 INNER JOIN OCR1 U1 ON U1."OcrCode" = U0."OcrCode" AND U1."ValidFrom" > U0."ValidFrom"
        		GROUP BY U0."OcrCode", U0."ValidFrom"
        	) T1 
        	WHERE T1."OcrCode" = T0."OcrCode" AND T1."ValidTo" > T0."ValidFrom" 
        	), ADD_YEARS(NOW(), 99)
        ) AS "ValidTo" 
        FROM OCR1 T0 
        ORDER BY T0."OcrCode", T0."ValidFrom", T0."PrcCode"
	);

	call CRSP_TrialBalanceByCostCenter_BR_enUS_cont(:Currency, :IgnoreAdj, :AddVoucher, :DateType,
				:FromDate, :ToDate, :DimCode, :BroughtForward, :EndYear);
	
	SELECT IFNULL(T1."Typ", 'Acct') AS "Typ", IFNULL(T1."TransId", 0) AS "TransId", 
        IFNULL(T1."Line_ID", -1) AS "Line_ID", T0."Lvl", IFNULL(T0."AcctCode", '') AS "AcctCode", T0."Description", 
        T0."Postable", T0."Variator", T0."Liq", T0."Level1", T0."Level2", T0."Level3", T0."Level4", T0."Level5", 
        T0."Level6", T0."Level7", T0."Level8", T0."Level1AccntntCod", T0."Level2AccntntCod", T0."Level3AccntntCod", 
        T0."Level4AccntntCod", T0."Level5AccntntCod", T0."Level6AccntntCod", T0."Level7AccntntCod", 
        T0."Level8AccntntCod", T0."Level1FrgnName", T0."Level2FrgnName", T0."Level3FrgnName", T0."Level4FrgnName", 
        T0."Level5FrgnName", T0."Level6FrgnName", T0."Level7FrgnName", T0."Level8FrgnName", T0."Sort", 
        IFNULL(T0."FatherId", '') AS "FatherId", T0."Id", T0."MaxLvl", IFNULL(T0."Desc1", '') AS "Desc1", 
        IFNULL(T0."Desc2", '') AS "Desc2", IFNULL(T0."Desc3", '') AS "Desc3", IFNULL(T0."Desc4", '') AS "Desc4", 
        IFNULL(T0."Desc5", '') AS "Desc5", IFNULL(T0."Desc6", '') AS "Desc6", IFNULL(T0."Desc7", '') AS "Desc7", 
        IFNULL(T0."Desc8", '') AS "Desc8", IFNULL(T0."DescAll", '') AS "DescAll", T1."RefDate", 
        IFNULL(T1."FinancYear", :FinancYear) AS "FinancYear", IFNULL(T1."PrcCode", '') AS "PrcCode", 
        IFNULL(T1."OcrCode", '') AS "OcrCode", IFNULL(T1."PrjCode", '') AS "PrjCode", 
        IFNULL(T1."Credit", 0) AS "Credit", IFNULL(T1."CreditPreviousYear", 0) AS "CreditPreviousYear", 
        IFNULL(T1."Debit", 0) AS "Debit", IFNULL(T1."DebitPreviousYear", 0) AS "DebitPreviousYear", 
        IFNULL(T1."CreditBudget", 0) AS "CreditBudget", IFNULL(T1."DebitBudget", 0) AS "DebitBudget", 
        IFNULL(T1."CreditBudgetPrevYear", 0) AS "CreditBudgetPrevYear", 
        IFNULL(T1."DebitBudgetPrevYear", 0) AS "DebitBudgetPrevYear", 
        IFNULL(T1."CreditActualMonth", 0) AS "CreditActualMonth", 
        IFNULL(T1."CreditMonthPreviousYear", 0) AS "CreditMonthPreviousYear", 
        IFNULL(T1."DebitActualMonth", 0) AS "DebitActualMonth", 
        IFNULL(T1."DebitMonthPreviousYear", 0) AS "DebitMonthPreviousYear", 
        IFNULL(T1."CreditActualMonthBudget", 0) AS "CreditActualMonthBudget", 
        IFNULL(T1."DebitActualMonthBudget", 0) AS "DebitActualMonthBudget", 
        IFNULL(T1."CreditFinancYear", 0) AS "CreditFinancYear", IFNULL(T1."DebitFinancYear", 0) AS "DebitFinancYear", 
        IFNULL(T1."CreditFinancYearBudget", 0) AS "CreditFinancYearBudget", 
        IFNULL(T1."DebitFinancYearBudget", 0) AS "DebitFinancYearBudget", 
        IFNULL(T1."CreditPrevFinancYear", 0) AS "CreditPrevFinancYear", 
        IFNULL(T1."DebitPrevFinancYear", 0) AS "DebitPrevFinancYear", IFNULL(T1."BFDebit", 0) AS "BFDebit", 
        IFNULL(T1."BFCredit", 0) AS "BFCredit", IFNULL(T1."OBDebit", 0) AS "OBDebit", 
        IFNULL(T1."OBCredit", 0) AS "OBCredit", IFNULL(T1."BFOBDebit", 0) AS "BFOBDebit", 
        IFNULL(T1."BFOBCredit", 0) AS "BFOBCredit", IFNULL(T6."OcrName", '') AS "OcrName", 
        IFNULL(T7."OcrName", '') AS "PrcName", IFNULL(T8."PrjName", '') AS "PrjName", 
        IFNULL(T9."GrpCode", '') AS "PrcGrpCode", T3."AcctName", T3."CurrTotal", T3."Finanse", T3."Budget", 
        T3."Frozen", T3."Levels", IFNULL(T3."ExportCode", '') AS "ExportCode", T3."GrpLine", T0."DocType", 
        T3."ActType", IFNULL(T3."OverCode", '') AS "OverCode", IFNULL(T3."Project", '') AS "OACTProject", 
        IFNULL(T0."FormatCode", '') AS "FormatCode", IFNULL(T0."AcctCodeDisp", '') AS "AcctCodeDisp", 
        IFNULL(T0."ActCurr", '') AS "ActCurr", IFNULL(T0."GroupMask", 0) AS "GroupMask", T0."Segment_0", 
        T0."Segment_1", T0."Segment_2", T0."Segment_3", T0."Segment_4", T0."Segment_5", T0."Segment_6", 
        T0."Segment_7", T0."Segment_8", T0."Segment_9", IFNULL(T0."AccntntCod", '') AS "AccntntCod", 
        IFNULL(T0."FrgnName", '') AS "FrgnName" 
    FROM "CRSP_Plan" T0 
        LEFT OUTER JOIN CRSP_TEMPRES T1 ON T0."AcctCode" = T1."Account" 
        LEFT OUTER JOIN OACT T3 ON T0."AcctCode" = T3."AcctCode" 
        LEFT OUTER JOIN OOCR T6 ON T1."OcrCode" = T6."OcrCode" 
        LEFT OUTER JOIN OOCR T7 ON T1."PrcCode" = T7."OcrCode" 
        LEFT OUTER JOIN OPRJ T8 ON T1."PrjCode" = T8."PrjCode" 
        LEFT OUTER JOIN OPRC T9 ON T1."PrcCode" = T9."PrcCode" 
    ORDER BY T0."Sort";
	
    /* Clean up */
    /*LEFT OUTER JOIN OBPL T10 ON T1.BPLId = T10.BPLId*/
    /* External Code */
    /* Foreign Name */
    /*,IFNULL(T1.BPLId,'') As BPLId*/
    /*,IFNULL(T10.BPLName,'') As BPLName*/
    DELETE FROM "CRSP_TEMPJDT1";
    DELETE FROM "CRSP_TEMPRES";
    DELETE FROM "CRSP_OCR1DistRule";
    DELETE FROM "CRSP_Plan";
    DELETE FROM "CRSP_TemplateAccount";
    DELETE FROM "CRSP_Plan0";
    DELETE FROM "CRSP_AcctLang";
END;











