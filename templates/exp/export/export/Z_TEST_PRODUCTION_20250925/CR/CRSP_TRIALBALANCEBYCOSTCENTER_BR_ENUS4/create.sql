-- B1 DEPENDS: AFTER:SP:_TmSp_BootCreateGlobalTempTables2
CREATE PROCEDURE CRSP_TrialBalanceByCostCenter_BR_enUS4
(
in TemplateID VARCHAR(200)
) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
Plan varchar(200);
BEGIN /*
    
***** General Information *****
Name: OFRT_FinancialTemplates
Description:
Return name and absid for selected financial template

Creator: coresystems ag for SAP AG, muf@coresystems.ch
Create Date: 2011-06-22

***** Updates *****
*/
    /* Financial Template ID, @@Account for Chart of Accounts. Param: '[%TemplateID]' */
    Plan := :TemplateID;
    IF :Plan = '@@Account' THEN 
        SELECT '@@Account' AS "AbsId", 'Chart of Accounts' AS "Name" 
        FROM DUMMY;
    ELSE 
        SELECT "AbsId", "Name" 
        FROM OFRT 
        WHERE "AbsId" = :Plan;
    END IF;
END;











