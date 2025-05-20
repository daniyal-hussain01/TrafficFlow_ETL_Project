-- Total accidents by vehicle type in 2025

SELECT v.VehicleType, COUNT(*) as AccidentCount
FROM Fact_Accidents f 
JOIN Dim_Vehicle v ON f.VehicleID = v.VehicleID 
JOIN Dim_Date d ON f.DateID = d.DateID 
WHERE d.Year = 2025
GROUP BY v.VehicleType;	

-- Average severity score for accidents on snowy surfaces

SELECT r.Surface, AVG(f.SeverityScore) as AvgSeverity 
FROM Fact_Accidents f 
JOIN Dim_RoadCondition r ON f.RoadConditionID = r.ConditionID 
WHERE r.Surface = 'Snowy' 
GROUP BY r.Surface;	

-- Locations with the highest number of severe accidents

SELECT l.LocationName, COUNT(*) as SevereAccidents 
FROM Fact_Accidents f 
JOIN Dim_Location l ON f.LocationID = l.LocationID 
WHERE f.SeverityScore = 3 
GROUP BY l.LocationName 
ORDER BY SevereAccidents 
DESC LIMIT 5;	

-- Accidents by month and severity

SELECT d.Month, f.SeverityScore, COUNT(*) as AccidentCount 
FROM Fact_Accidents f 
JOIN Dim_Date d ON f.DateID = d.DateID 
GROUP BY d.Month, f.SeverityScore 
ORDER BY d.Month;	

-- Count of accidents under rainy visibility by vehicle type

SELECT v.VehicleType, COUNT(*) as AccidentCount 
FROM Fact_Accidents f 
JOIN Dim_Vehicle v ON f.VehicleID = v.VehicleID 
JOIN Dim_RoadCondition r ON f.RoadConditionID = r.ConditionID 
WHERE r.Visibility = 'Rainy' 
GROUP BY v.VehicleType;	

-- Total vehicles involved by location

SELECT l.LocationName, SUM(f.VehiclesInvolved) as TotalVehicles 
FROM Fact_Accidents f 
JOIN Dim_Location l ON f.LocationID = l.LocationID 
GROUP BY l.LocationName 
ORDER BY TotalVehicles 
DESC;	



