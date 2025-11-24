CREATE PROCEDURE OCSI_STAGING_DELTA
(
)
AS
BEGIN
		DELETE FROM ETL_SALES_LINES WHERE DOCTYPE = 165 AND DOCENTRY IN (SELECT "DocEntry" FROM OCSI_STAGING_DELTA_TABLE);
		INSERT INTO ETL_SALES_LINES
			Select 165,				
			T0."DocEntry", 
			T6."LineNum",
			
			T0."CardCode" ,
			T0."SlpCode",
			IFNULL(T1."CountryB", 'XX'),
			
			T0."DocDate",
			T0."DocDueDate",
			T0."TaxDate",
			
			T6."ItemCode",
			T6."WhsCode",
			T6."Quantity" * T6."NumPerMsr" as Quantity,
			

			TO_DECIMAL(T6."LineTotal"),
			TO_DECIMAL(T6."TotalSumSy"),
			TO_DECIMAL(T6."GPTtlBasPr"),
			TO_DECIMAL(T6."GrssProfit"),
			TO_DECIMAL(T6."GrssProfSC"),
			T7."Code" AS "Period",
			(CASE WHEN T6."Project" IS NULL OR  LENGTH(T6."Project") = 0 then '-' ELSE T6."Project" END) AS "Project"
			From OCSI T0 
			LEFT JOIN "CSI12" T1 ON T0."DocEntry" = T1."DocEntry"
			INNER JOIN "OCSI_STAGING_DELTA_TABLE" TEMP ON TEMP."DocEntry" = T0."DocEntry"
			Join CSI1 T6 On T0."DocEntry" = T6."DocEntry"
			Left Outer Join 
				(
					Select Header."DocEntry", Header."WTSum", Header."WTSumSC"
					  From OCSI Header 
					  Join CSI5 WT On WT."AbsEntry" = Header."DocEntry"
								And WT."Category" = 'I'
					  Group By Header."DocEntry", Header."WTSum", Header."WTSumSC"
				) WTax On T0."DocEntry" = WTax."DocEntry"
			Left Outer Join OFPR T7
			ON  T0."DocDate" >= T7."F_RefDate" AND T0."DocDate" <= T7."T_RefDate"
			Where T0."Instance" = 0;
END
