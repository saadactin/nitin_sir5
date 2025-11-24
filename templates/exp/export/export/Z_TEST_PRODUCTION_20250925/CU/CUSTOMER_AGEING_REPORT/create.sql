CREATE procedure Customer_Ageing_Report

(IN FromDate Timestamp
,IN ToDate Timestamp)   
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
--READ SQL DATA 
AS
 Begin
SELECT
      "Customer Code"
    , "Customer Name"
    , "BalanceDue"
    , "DueDays"
    
    , SUM(CASE 
            WHEN "DueDays" < 0 THEN "BalanceDue"
            ELSE 0.00
          END) AS "Future Remit"
    , SUM(CASE 
            WHEN "DueDays" >= 0 AND "DueDays" < 30 THEN "BalanceDue"
            ELSE 0.00
          END) AS "0-30 days"
    , SUM(CASE 
            WHEN "DueDays" >= 30 AND "DueDays" < 60 THEN "BalanceDue"
            ELSE 0.00
          END) AS "31 to 60 days" 
    , SUM(CASE 
            WHEN "DueDays" >= 60 AND "DueDays" < 90 THEN "BalanceDue"
            ELSE 0.00
          END) AS "61 to 90 days"
    , SUM(CASE 
            WHEN "DueDays" >= 90 AND "DueDays" < 120 THEN "BalanceDue"
            ELSE 0.00
          END) AS "91 to 120 days" 
    , SUM(CASE 
            WHEN "DueDays" >= 120 THEN "BalanceDue"
            ELSE 0.00
          END) AS "120+ days" 
FROM (SELECT 
          OCRD."CardCode" AS "Customer Code"
        , OCRD."CardName" AS "Customer Name"
        , DAYS_BETWEEN(JDT1."DueDate", CURRENT_DATE) as "DueDays"
        , Sum(CASE 
           WHEN JDT1."BalDueCred" <> 0 THEN JDT1."BalDueCred" * -1 
           ELSE JDT1."BalDueDeb" 
          END) AS "BalanceDue"
    FROM 
                   JDT1 
        INNER JOIN OCRD 
                ON JDT1."ShortName" = OCRD."CardCode" 
               AND OCRD."CardType" = 'C' 
    Where JDT1."RefDate" BETWEEN :FromDate AND :ToDate 
    GROUP BY OCRD."CardCode", OCRD."CardName",JDT1."DueDate",JDT1."BalDueCred",JDT1."BalDueDeb")
    --HAVING 
        --SUM(CASE WHEN "SYSCred" <> 0 THEN "SYSCred" * -1 
            --ELSE "SYSDeb" END) <> 0;
Where "BalanceDue" <> 0
    GROUP BY 
    "Customer Code" ,"Customer Name" , "BalanceDue", "DueDays";
 END

