-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables AFTER:PT:PROCESS_END

CREATE PROCEDURE CRSP_ProfitAndLostByCC_BR_enUS3 (in BudgId integer) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
BEGIN /*
    
***** General Information *****
Name: OBGS_Budget
Description:
Return name and absid for selected Budget

Creator: coresystems ag for SAP AG, muf@coresystems.ch
Create Date: 2011-10-03

***** Updates *****
*/
    SELECT "AbsId" AS "AbsID", "Name", "FinancYear", 
        CAST(YEAR("FinancYear") AS varchar) || ' - ' || "Name" AS "BudgetYearName" 
    FROM OBGS 
    WHERE "AbsId" = :BudgId;
END











