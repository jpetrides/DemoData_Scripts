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