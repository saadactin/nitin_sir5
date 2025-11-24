-- B1 DEPENDS: AFTER:SP:ATP_A0_CREATE_DB_TYPES AFTER:PT:PROCESS_END
CREATE PROCEDURE ATP_A9_CANDALGO (
     IN timeSeries ATP_INT_QTY,
     IN scheduleLines ATP_INT_QTY,
     OUT confirmations ATP_INT_QTY)
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
typedef Block<QTY_T> AAD_T;
typedef DATE_QTY_TABLE_T CANDIDATE_T;
typedef DATE_QTY_TABLE_T TS_T;
typedef DATE_QTY_TABLE_T SL_T;
Void traceTab(CANDIDATE_T & data) {
    Trace t;
    t.append("Candidates").endl();
    t.append(data, data.getSize());
}
Void addCandidate(CANDIDATE_T & candidates,
                  DATE_T date,
                  QTY_T value)
{
    COL_DATE_T dates = candidates.getColumn<DATE_T>("DATE");
    COL_QTY_T values = candidates.getColumn<QTY_T>("QTY");
    Size currSize = dates.getSize();
    dates[currSize] = date;
    values[currSize] = value;
}
Void removeCandidates(CANDIDATE_T & candidates,
                      QTY_T accumulated)

{
    //Trace t;
    //t.append("rm cand amount").append(accumulated).endl();
    COL_DATE_T dates = candidates.getColumn<DATE_T>("DATE");
    COL_QTY_T values = candidates.getColumn<QTY_T>("QTY");
    Size currSize = Size(dates.getSize());
    QTY_T lastValue;
    QTY_T zero;
    while ((accumulated < zero) && (currSize > 0z)) {
        lastValue = values[currSize - 1z];
        //t.append(accumulated).append(" ").append(lastValue).endl(); */
        if ((lastValue + accumulated) <= zero)
        {
            accumulated = accumulated + lastValue;
            // BUG: In lieu of currSize = currSize - 1z;
            if (currSize == 1z) { currSize = 0z; } else { currSize = currSize - 1z; }
            dates.setSize(currSize);
            values.setSize(currSize);
        }
        else
        {
            values[currSize - 1z] = values[currSize - 1z] + accumulated;
            accumulated = zero;
        }
    }
}
Void modify(DATE_T date,
            AAD_T & aad,
            CANDIDATE_T & candidates)
{
    //Trace t;
    //t.append("Modify ").append(date).endl();
    Size accumulatedz = 0z;
    Size acquiredz = 1z;
    Size desiredz = 2z;
    // Increase readability of the following part by extracting aad into vars
    QTY_T acquired = aad[acquiredz];
    QTY_T accumulated = aad[accumulatedz];
    QTY_T desired = aad[desiredz];
    QTY_T zero;
    //t.append("AAD ").append(accumulated).append(" ").append(acquired).append(" ").append(desired).endl();
    //t.append("acc - des = ").append(acquired - desired).endl();
    // The time series item is larger than what we need (desired-acquired)
    // and what we need is larger than zero
    if ((accumulated >= (desired - acquired)) && ((desired - acquired) > zero)) {
        addCandidate(candidates, date, desired - acquired);
        accumulated = accumulated - (desired - acquired);
        acquired = desired;
    // What we need is larger than accumulated and there is actually something
    // to accumulate
    } else if (((desired - acquired) > accumulated) && (accumulated > zero)) {
        addCandidate(candidates, date, accumulated);
        acquired = acquired + accumulated;
        accumulated = zero;
    } else {
        if (accumulated < zero) {
            QTY_T oldAcquired = acquired;
            removeCandidates(candidates, accumulated);
            acquired = acquired + accumulated;
            if (acquired < zero) {
                acquired = zero;
            }
            accumulated = accumulated + oldAcquired;
            if (accumulated > zero) {
                accumulated = zero;
            }
        }
    }
    //t.append("AAD after").append(accumulated).append(" ").append(acquired).append(" ").append(desired).endl();
    aad[accumulatedz] = accumulated;
    aad[acquiredz] = acquired;
    aad[desiredz] = desired;
}
Void calculateCandidates(TS_T timeSeries,
                         SL_T scheduleLines,
                         CANDIDATE_T & candidates)
{
    //Trace t;
    //t.append("TS").endl();
    //traceTab(timeSeries);
    //t.append("SL").endl();
    //traceTab(scheduleLines);
    
    Block<QTY_T> aad = Block<QTY_T>(3z);
    aad.setSize(3z);
    
    Size accumulated = 0z;
    Size acquired = 1z;
    Size desired = 2z;
    QTY_T oldAccumulated;
    COL_DATE_T tsDates = timeSeries.getColumn<DATE_T>("DATE");
    COL_QTY_T tsQuantities = timeSeries.getColumn<QTY_T>("QTY");
    COL_DATE_T slDates = scheduleLines.getColumn<DATE_T>("DATE");
    COL_QTY_T slQuantities = scheduleLines.getColumn<QTY_T>("QTY");
    Size row; // pointer to current element of timeseries
    Size line; // pointer to current element of schedule line
    DATE_T currDate; // current date of timeseries
    QTY_T currQty; // current qty of timeseries
    DATE_T desiredDate; // current date of schedule lines
    QTY_T desiredAmount; // current qty of schedule lines
    DATE_T checkDate;
    while ((row < timeSeries.getSize()) || (line < scheduleLines.getSize()))
    {
        //t.append("row ").append(row).append(" / ").append(timeSeries.getSize()).append("       ").append("line ").append(line).append(" / ").append(scheduleLines.getSize()).endl();
        if ((line >= scheduleLines.getSize()) || (row < timeSeries.getSize() && tsDates[row] < slDates[line]))  {
            currDate = tsDates[row];
            currQty  = tsQuantities[row];
            aad[accumulated] = aad[accumulated] + currQty;
            checkDate = currDate;
            row = row + 1z;
        }
        else {
            if (row >= timeSeries.getSize() ||
               (line < scheduleLines.getSize() && slDates[line] < (tsDates[row]) )  ) {
                desiredDate = slDates[line];
                desiredAmount  = slQuantities[line];
                aad[desired] = aad[desired] + desiredAmount;
                checkDate = desiredDate;
                line = line + 1z;
            }
            else {
                currDate = tsDates[row];
                currQty  = tsQuantities[row];
                aad[accumulated] = aad[accumulated] + currQty;
                checkDate = currDate;
                row = row + 1z;
                desiredDate = slDates[line];
                desiredAmount  = slQuantities[line];
                aad[desired] = aad[desired] + desiredAmount;
                checkDate = desiredDate;
                line = line + 1z;
            }
        }
        //t.append("row ").append(row).append("       ").append("line ").append(line).endl();
        modify(checkDate, aad, candidates);
    }
}
export Void main(TS_T "timeSeries" timeSeries,
                 SL_T "scheduleLines" scheduleLines,
                 CANDIDATE_T "confirmations" & confirmations)
{
    calculateCandidates(timeSeries, scheduleLines, confirmations);
}
END;











