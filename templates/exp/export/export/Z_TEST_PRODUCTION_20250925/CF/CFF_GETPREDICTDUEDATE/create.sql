-- B1 DEPENDS: AFTER:PT:PROCESS_END AFTER:SP:CFF_CREATEDBOBJECTS

CREATE PROCEDURE CFF_GETPREDICTDUEDATE( IN dueDates			CFF_DATE_T, 
										IN holidays			CFF_HOLIDAY_T,
										OUT predictDueDates	CFF_PREDICT_DUEDATE_T)
LANGUAGE LLANG 
SQL SECURITY INVOKER
AS
BEGIN
	typedef Table<Int32 "AbsEntry", Date "DocDueDate", Int32 "AdjustDocDueDate", 
				  Int32 "DeltaMonths", Int32 "AdjustDay", String "HldCode",
				  String "DocType", Int32 "DocEntry", String "CardCode", 
				  String "CtlAccount", Fixed12<6> "Debit", Fixed12<6> "Credit", 
				  String "OriginalType", String "Group", Int32 "InstID", 
				  Int32 "DocNum", Int32 "AvrageLate"> T_DUEDATES;

	typedef Table<String "HldCode", Int32 "WndFrm", Int32 "WndTo",
				  Int32 "IsCurYear", Int32 "IgnrWnd",
				  Date "StrDate", Date "EndDate">  T_HOLIDAYS;
	typedef Table<Int32 "AbsEntry", Date "PredictDueDate"> T_PREDICT_DUEDATE;
	
	
	typedef Column<Int32> COL_INT;
	typedef Column<Date> COL_DATE;
	typedef Column<String> COL_STR;
	
	Date predictDueDate(Date dueDate, String hldCode, T_HOLIDAYS holidays)
	{
		COL_STR colHldCode = holidays."HldCode";
		COL_DATE colStrDate = holidays."StrDate";
		COL_DATE colEndDate = holidays."EndDate";
		
		COL_INT colWndFrom = holidays."WndFrm";
		COL_INT colWndTo = holidays."WndTo";
		
		Date predictedDate = dueDate;
		Date strDate;
		Date endDate;
		
		Int32 weekday;
		Int32 wndFrom;
		Int32 wndTo;
		
		Size row = 0z;
		while(row < holidays.getSize() )
		{
			if (hldCode != colHldCode[row])
			{
				row = row + 1z;
				continue;
			}
			strDate = colStrDate[row];
			endDate = colEndDate[row];
			wndFrom = colWndFrom[row];
			wndTo = colWndTo[row];
			
			if(0 == holidays."IsCurYear"[row])
			{
				strDate = Date(dueDate.getYear(), strDate.getMonth(), strDate.getDay());
				endDate = Date(dueDate.getYear(), endDate.getMonth(), endDate.getDay());
			}
			if(strDate <= predictedDate && predictedDate <= endDate )
			{
				predictedDate = endDate.addDays(1);
			}
			
			if(0 == holidays."IgnrWnd"[row])
			{
				// Monday, Tuesday, ..., Saturday, Sunday
				// B1: 	2, 3, ..., 7, 1
				// HANA:0, 1, ..., 5, 6
				weekday = (predictedDate.getWeekDay() + 2) % 7;
				
				if (wndFrom <= wndTo)
				{
					if (wndFrom <= weekday && weekday <= wndTo)
					{
						predictedDate = predictedDate.addDays(wndTo - weekday + 1);
					}
				}
				else 
				{
					if (weekday >= wndFrom )
					{
						predictedDate = predictedDate.addDays(8 + wndTo - weekday);
					}
					else if (weekday <= wndTo)
					{
						predictedDate = predictedDate.addDays(wndTo - weekday + 1);
					}
				}
			}

			row = row + 1z;
		} 
		
		return predictedDate;
	}
	
	export Void main(T_DUEDATES dueDates, T_HOLIDAYS holidays,T_PREDICT_DUEDATE &predictDueDates)
	{
		COL_INT colAdjust = dueDates."AdjustDocDueDate";
		COL_DATE colDueDate = dueDates."DocDueDate";
		
		COL_INT colPredictEntry = predictDueDates."AbsEntry";
		COL_DATE colPredictDate = predictDueDates."PredictDueDate";
		colPredictEntry.setSize(dueDates.getSize());
		colPredictDate.setSize(dueDates.getSize());

		Int32 curYear;
		Int32 curMonth;
		Int32 deltaMonths;
		Int32 adjustDay;
		String hldCode;
		
		Size row = 0z;
		while (row < dueDates.getSize())
		{
			colPredictDate[row] = colDueDate[row];

			hldCode = dueDates."HldCode"[row];
			colPredictDate[row] = predictDueDate(colPredictDate[row], hldCode, holidays);
			colPredictEntry[row] = dueDates."AbsEntry"[row];

			row = row + 1z;
			
		}
		
	}
END;











