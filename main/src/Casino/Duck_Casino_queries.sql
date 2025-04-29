SELECT 
    m.MachineID,
    m.MachineType,
    DATE_TRUNC('month', s.StartTime) as Month,
    COUNT(DISTINCT s.SessionID) as TotalSessions,
    SUM(s.TotalBets) as TotalBets,
    SUM(s.TotalPayouts) as TotalPayouts,
    SUM(s.TotalBets) - SUM(s.TotalPayouts) as Revenue
FROM 
    Dim_Machines m
JOIN 
    F_Slots_Sessions s ON m.MachineID = s.MachineID
GROUP BY 
    m.MachineID, m.MachineType, DATE_TRUNC('month', s.StartTime)
ORDER BY 
    m.MachineID, Month
LIMIT 20;

SELECT 
    m.MachineID,
    m.MachineType,
    COUNT(DISTINCT s.SessionID) as TotalSessions,
    SUM(s.TotalBets) as TotalBets,
    SUM(s.TotalPayouts) as TotalPayouts,
    SUM(s.TotalBets) - SUM(s.TotalPayouts) as Revenue
FROM 
    Dim_Machines m
JOIN 
    F_Slots_Sessions s ON m.MachineID = s.MachineID
GROUP BY 
    m.MachineID, m.MachineType
ORDER BY 
    Revenue DESC
LIMIT 10;

SELECT 
    EXTRACT(HOUR FROM Timestamp) as Hour,
    COUNT(*) as TransactionCount,
    AVG(BetAmount) as AvgBetAmount,
    AVG(PayoutAmount) as AvgPayoutAmount
FROM 
    F_Slots_Transactions
GROUP BY 
    EXTRACT(HOUR FROM Timestamp)
ORDER BY 
    Hour;

-- Shift transactions around for seasonality:
WITH transactions_to_move AS (
    SELECT SessionID, Timestamp
    FROM hotel_reservations.main.F_Slots_Transactions
    WHERE EXTRACT(MONTH FROM Timestamp) = 4  -- April - this is the month we are taking transactions from
    ORDER BY RANDOM()  -- Randomize the selection
    LIMIT 30000
)
UPDATE hotel_reservations.main.F_Slots_Transactions
SET Timestamp = Timestamp - INTERVAL '3 months'
WHERE SessionID IN (SELECT SessionID FROM transactions_to_move);

/* CREATE VIEW FOR SESSIONS SUMMARY*/

-- Create the materialized view
CREATE VIEW MV_Slots_Sessions AS
SELECT 
    SessionID,
    MachineID,
    CustomerID,
    MIN(Timestamp) AS StartTime,
    MAX(Timestamp) AS EndTime,
    SUM(BetAmount) AS TotalBets,
    SUM(PayoutAmount) AS TotalPayouts,
    SUM(BetAmount) - SUM(PayoutAmount) AS Revenue,
    COUNT(*) AS TransactionCount
FROM 
    F_Slots_Transactions
GROUP BY 
    SessionID, MachineID, CustomerID;

/*
--Volume
    January being the busiest month, 
    followed by May and December, 
    April and June are the slowest.
*/