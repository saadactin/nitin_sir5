Create Procedure ETL_SALES_LINES_INIT()
As
Begin
	Insert Into ETL_SALES_LINES
		Select 13 as DocType, 				
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
 				T6."Quantity" * T6."NumPerMsr",
 				

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
						  ) * T6."LineTotal" 
				End,
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
						  ) * T6."TotalSumSy" 
				End,
				TO_DECIMAL(T6."GPTtlBasPr"),
				TO_DECIMAL(T6."GrssProfit"),
				TO_DECIMAL(T6."GrssProfSC"),
				
				T7."Code" AS "Period",
				(CASE WHEN T6."Project" IS NULL OR  LENGTH(T6."Project") = 0 then '-' ELSE T6."Project" END) AS "Project"
 				From OINV T0 
 				LEFT JOIN "INV12" T1 ON T0."DocEntry" = T1."DocEntry"
 				Join INV1 T6 On T0."DocEntry" = T6."DocEntry"
 				Left Outer Join 
 					(
 						Select Header."DocEntry", Header."WTSum", Header."WTSumSC"
 						  From OINV Header 
 						  Join INV5 WT On WT."AbsEntry" = Header."DocEntry"
 						  			And WT."Category" = 'I'
 						  Group By Header."DocEntry", Header."WTSum", Header."WTSumSC"
 					) WTax On T0."DocEntry" = WTax."DocEntry"
				Left Outer Join OFPR T7
				ON  T0."DocDate" >= T7."F_RefDate" AND T0."DocDate" <= T7."T_RefDate"
 				Where T0."Instance" = 0 
 					And T0."CANCELED" = 'N'
 					And (T0."DocType" = 'S' Or T6."Quantity" <> 0);

	Insert Into ETL_SALES_LINES
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
 				-T6."Quantity" * T6."NumPerMsr", 					

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
				End,
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
				End,
				TO_DECIMAL(T6."GPTtlBasPr") * -1,
				TO_DECIMAL(T6."GrssProfit") * -1,
				TO_DECIMAL(T6."GrssProfSC") * -1,
				
				T7."Code" AS "Period",
				(CASE WHEN T6."Project" IS NULL OR  LENGTH(T6."Project") = 0 then '-' ELSE T6."Project" END) AS "Project"
 				From ORIN T0 
 				LEFT JOIN "RIN12" T1 ON T0."DocEntry" = T1."DocEntry"
 				Join RIN1 T6 On T0."DocEntry" = T6."DocEntry"
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

	Insert Into ETL_SALES_LINES
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
 				T6."Quantity" * T6."NumPerMsr",
						

				TO_DECIMAL(T6."LineTotal"),
				TO_DECIMAL(T6."TotalSumSy"),
				TO_DECIMAL(T6."GPTtlBasPr"),
				TO_DECIMAL(T6."GrssProfit"),
				TO_DECIMAL(T6."GrssProfSC"),

				T7."Code" AS "Period",
				(CASE WHEN T6."Project" IS NULL OR  LENGTH(T6."Project") = 0 then '-' ELSE T6."Project" END) AS "Project"
 				From OCSI T0 
 				LEFT JOIN "CSI12" T1 ON T0."DocEntry" = T1."DocEntry"
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

	Insert Into ETL_SALES_LINES
		Select 166,				
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
 				T6."Quantity" * T6."NumPerMsr",
 				

				TO_DECIMAL(T6."LineTotal"),
				TO_DECIMAL(T6."TotalSumSy"),
				TO_DECIMAL(T6."GPTtlBasPr"),
				TO_DECIMAL(T6."GrssProfit"),
				TO_DECIMAL(T6."GrssProfSC"),
				
				T7."Code" AS "Period",
				(CASE WHEN T6."Project" IS NULL OR  LENGTH(T6."Project") = 0 then '-' ELSE T6."Project" END) AS "Project"
 				From OCSV T0 
 				LEFT JOIN "CSV12" T1 ON T0."DocEntry" = T1."DocEntry"
 				Join CSV1 T6 On T0."DocEntry" = T6."DocEntry"
 				Left Outer Join 
 					(
 						Select Header."DocEntry", Header."WTSum", Header."WTSumSC"
 						  From OCSV Header 
 						  Join CSV5 WT On WT."AbsEntry" = Header."DocEntry"
 						  			And WT."Category" = 'I'
 						  Group By Header."DocEntry", Header."WTSum", Header."WTSumSC"
 					) WTax On T0."DocEntry" = WTax."DocEntry"
				Left Outer Join OFPR T7
				ON  T0."DocDate" >= T7."F_RefDate" AND T0."DocDate" <= T7."T_RefDate"
 				Where T0."Instance" = 0 
 					And T0."CANCELED" = 'N';
End
