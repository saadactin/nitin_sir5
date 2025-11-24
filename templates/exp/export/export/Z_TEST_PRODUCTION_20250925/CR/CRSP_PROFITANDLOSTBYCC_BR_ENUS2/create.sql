CREATE PROCEDURE CRSP_ProfitAndLostByCC_BR_enUS2 (in plan nvarchar(20)) 
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
BEGIN /*
    
***** General Information *****
Name: OFRT_FinancialTemplates
Description:
Return name and absid for selected financial template

Creator: coresystems ag for SAP AG, muf@coresystems.ch
Create Date: 2011-06-22

HANA version converted by SAP.

***** Updates *****
*/
    /* Financial Template ID, @@Account for Chart of Accounts. Param: '[%TemplateID]' */
    
    IF :Plan = '@@Account' THEN 
        SELECT '@@Account' AS "AbsId", 'Chart of Accounts' AS "Name" FROM DUMMY;
    ELSE 
        SELECT "AbsId", "Name" FROM OFRT 
        WHERE "AbsId" = :Plan;
    END IF;
END;











