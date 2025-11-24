CREATE PROCEDURE ORIN_STAGING_DELTA
(

)
AS
BEGIN
		DELETE FROM ETL_SALES_LINES Where DOCTYPE = 14 And DOCENTRY IN (SELECT "DocEntry" FROM ORIN_STAGING_DELTA_TABLE);
		INSERT INTO ETL_SALES_LINES
			Select 14,
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
			-T6."Quantity" * T6."NumPerMsr" as Quantity,
			

			Case T6."TaxOnly" 
				When 'Y' Then 0
				Else (1 -
						(Case T0."DocTotal" - T0."VatSum" - T0."TotalExpns" + T0."DiscSum" 
						 When 0 Then 0
						 Else (	Case T0."DocTotal" + T0."DpmAmnt" + IFNULL(WTax."WTSum", 0.0)- T0."VatSum" - T0."TotalExpns" + T0."DiscSum"
								When 0 Then 0
								Else T0."DiscSum"/ (T0."DocTotal" + T0."DpmAmnt" + IFNULL(WTax."WTSum", 0.0)- T0."VatSum" - T0."TotalExpns" + T0."DiscSum")
								End )
						 End)
					  ) * T6."LineTotal" * -1
			End as SalesAmount,
			Case T6."TaxOnly" 
				When 'Y' Then 0
				Else (1 -
						(Case T0."DocTotalSy" - T0."VatSumSy" - T0."TotalExpSC" + T0."DiscSumSy" 
						 When 0 Then 0
						 Else (	Case T0."DocTotalSy" + T0."DpmAmntSC" + IFNULL(WTax."WTSumSC", 0.0)- T0."VatSumSy" - T0."TotalExpSC" + T0."DiscSumSy"
								When 0 Then 0
								Else T0."DiscSumSy"/ (T0."DocTotalSy" + T0."DpmAmntSC" + IFNULL(WTax."WTSumSC", 0.0)- T0."VatSumSy" - T0."TotalExpSC" + T0."DiscSumSy")
								End )
						 End)
					  ) * T6."TotalSumSy" * -1
			End as SalesAmountSC,
			TO_DECIMAL(T6."GPTtlBasPr") * -1 as BaseAmount,
			TO_DECIMAL(T6."GrssProfit") * -1 as GrossProfit,
			TO_DECIMAL(T6."GrssProfSC") * -1 as GrossProfitSC,
			T7."Code" AS "Period",
			(CASE WHEN T6."Project" IS NULL OR  LENGTH(T6."Project") = 0 then '-' ELSE T6."Project" END) AS "Project"
			From ORIN T0 
			LEFT JOIN "RIN12" T1 ON T0."DocEntry" = T1."DocEntry"
			Join RIN1 T6 On T0."DocEntry" = T6."DocEntry"
			INNER JOIN "ORIN_STAGING_DELTA_TABLE" TEMP ON TEMP."DocEntry" = T0."DocEntry"
			Left Outer Join 
				(
					Select Header."DocEntry", Header."WTSum", Header."WTSumSC"
					  From ORIN Header 
					  Join RIN5 WT On WT."AbsEntry" = Header."DocEntry"
								And WT."Category" = 'I'
					  Group By Header."DocEntry", Header."WTSum", Header."WTSumSC"
				) WTax On T0."DocEntry" = WTax."DocEntry"
			Left Outer Join OFPR T7
			ON  T0."DocDate" >= T7."F_RefDate" AND T0."DocDate" <= T7."T_RefDate"
			Where T0."Instance" = 0 
				And T0."CANCELED" = 'N'
				And T6."BaseType" <> 203;
END
