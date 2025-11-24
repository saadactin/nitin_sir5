-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END
CREATE PROCEDURE ATP_B8_CUMULATE (
IN TIMESERIES ATP_INT_QTY,
OUT CUMULATED ATP_INT_QTY)
LANGUAGE LLANG
SQL SECURITY INVOKER
READS SQL DATA
AS
BEGIN
typedef Int32 DATE_T;
typedef Fixed12<6> QTY_T;
typedef Column<DATE_T> COL_DATE_T;
typedef Column<QTY_T> COL_QTY_T;
typedef Table<DATE_T "DATE", QTY_T "QTY"> DATE_QTY_TABLE_T;
typedef DATE_QTY_TABLE_T TS_T;
Void cumulate(TS_T timeSeries, TS_T & cumulated)
{
    
    
    COL_QTY_T tsQuantities = timeSeries.getColumn<QTY_T>("QTY");
    COL_DATE_T tsDates = timeSeries.getColumn<DATE_T>("DATE");
    COL_QTY_T cuQuantities = cumulated.getColumn<QTY_T>("QTY");
    COL_DATE_T cuDates = cumulated.getColumn<DATE_T>("DATE");
	cuQuantities.setSize(tsQuantities.getSize());
    cuDates.setSize(tsDates.getSize());
    Size row = 0z;
    // copy output table
    while(row < tsQuantities.getSize())
    {
		cuQuantities[row] = tsQuantities[row];
		cuDates[row] = tsDates[row];
		row = row + 1z;
    }
    row = 0z;
    // calculate free atp sets
    QTY_T zeroQty = Fixed12<6>(0);
    QTY_T qty = zeroQty;
    QTY_T deduced = zeroQty;
    while(row < cuQuantities.getSize())
    {
		qty = cuQuantities[row];
		if(qty < zeroQty)
		{
		    cuQuantities[row] = zeroQty;
		    deduced = qty;
		    if(row > 0z)
		    {
			    Size j = row - 1z;
			    while(j >= 0z)
			    {
					QTY_T prev = cuQuantities[j];

					    deduced = qty + prev;
					    if(deduced <= zeroQty)
					    {
							cuQuantities[j] = zeroQty;
							qty = deduced;
					    }
					    else
					    {
							cuQuantities[j] = deduced;
							deduced = zeroQty;
							break;
					    }

					// L cannot make size -1 (would throw arithmetic error)
					if(j == 0z)
					{
						break;
					}
					j = j - 1z;
			    }
		    }
		    if(deduced < zeroQty)
		    {
				cuQuantities[0z] = cuQuantities[0z] + deduced;
		    }
		}
		row = row + 1z;
    }
    // accumulate free atp sets
    QTY_T current = zeroQty;
    row = 0z;
    while(row < cuQuantities.getSize())
    {
		current = current + cuQuantities[row];
		cuQuantities[row] = current;
		row = row + 1z;
    }
}
export Void main(TS_T "timeSeries" timeSeries, TS_T "cumulated" & cumulated)
{
    cumulate(timeSeries, cumulated);
}
END;











