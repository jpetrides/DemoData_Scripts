UPDATE main.TTH_F_Reservation t
SET ADR = ADR * 
    CASE 
        -- WARM WEATHER DESTINATIONS - Winter Peak
        WHEN p."Metro Area" IN ('Miami', 'Miami - Fort Lauderdale', 'Tampa', 
                              'Tampa - St. Petersburg', 'Orlando', 'Phoenix', 
                              'Phoenix - Mesa', 'San Diego', 'San Diego - Chula Vista',
                              'Los Angeles', 'Los Angeles - Long Beach') THEN
            CASE 
                WHEN MONTH(t.CheckinDate_DT) IN (12, 1, 2, 3) THEN 1.35   -- Winter peak
                WHEN MONTH(t.CheckinDate_DT) IN (6, 7, 8) THEN 0.85       -- Summer low
                ELSE 1.0
            END
            
        -- LAS VEGAS - Special Events + Weekends
        WHEN p."Metro Area" = 'Las Vegas' THEN
            CASE 
                WHEN MONTH(t.CheckinDate_DT) = 1 AND DAY(t.CheckinDate_DT) BETWEEN 5 AND 12 THEN 1.8  -- CES
                WHEN MONTH(t.CheckinDate_DT) = 12 AND DAY(t.CheckinDate_DT) = 31 THEN 2.2            -- NYE
                WHEN DAYOFWEEK(t.CheckinDate_DT) IN (6, 7) THEN 1.4                                  -- Weekends
                ELSE 1.1
            END
            
        -- NORTHEAST CITIES - Fall/Spring Peak
        WHEN p."Metro Area" IN ('Boston', 'Boston - Cambridge', 'New York', 'New York - Newark',
                              'Philadelphia', 'Philadelphia - Camden', 'Washington', 
                              'Washington - Arlington', 'Baltimore', 'Pittsburgh, PA') THEN
            CASE 
                WHEN MONTH(t.CheckinDate_DT) IN (9, 10) THEN 1.3      -- Fall foliage
                WHEN MONTH(t.CheckinDate_DT) IN (5, 6) THEN 1.25      -- Graduation/tourism
                WHEN MONTH(t.CheckinDate_DT) IN (1, 2) THEN 0.85      -- Winter low
                WHEN MONTH(t.CheckinDate_DT) = 12 THEN 1.15           -- Holidays
                ELSE 1.0
            END
            
        -- TEXAS CITIES - Avoid summer heat
        WHEN p."Metro Area" IN ('Austin', 'Dallas', 'Dallas - Fort Worth', 
                              'Houston', 'Houston - The Woodlands', 'San Antonio') THEN
            CASE 
                WHEN p."Metro Area" = 'Austin' AND MONTH(t.CheckinDate_DT) = 3 THEN 2.0  -- SXSW
                WHEN MONTH(t.CheckinDate_DT) IN (7, 8) THEN 0.8                        -- Too hot
                WHEN MONTH(t.CheckinDate_DT) IN (3, 4, 10, 11) THEN 1.15              -- Best weather
                ELSE 1.0
            END
            
        -- MIDWEST/COLD CITIES - Summer Peak
        WHEN p."Metro Area" IN ('Chicago', 'Chicago - Naperville', 'Minneapolis', 
                              'Minneapolis - St. Paul', 'Detroit', 'Detroit - Warren',
                              'Cincinnati, OH', 'St. Louis, MO') THEN
            CASE 
                WHEN MONTH(t.CheckinDate_DT) IN (6, 7, 8) THEN 1.25    -- Summer peak
                WHEN MONTH(t.CheckinDate_DT) IN (12, 1, 2) THEN 0.85   -- Winter low
                ELSE 1.0
            END
            
        -- PACIFIC NORTHWEST - Summer Peak
        WHEN p."Metro Area" IN ('Seattle', 'Seattle - Tacoma', 'Portland') THEN
            CASE 
                WHEN MONTH(t.CheckinDate_DT) IN (7, 8) THEN 1.3       -- Dry season
                WHEN MONTH(t.CheckinDate_DT) IN (11, 12, 1) THEN 0.8  -- Rainy season
                ELSE 1.0
            END
            
        -- CALIFORNIA CITIES - Steady with slight seasonal
        WHEN p."Metro Area" IN ('San Francisco', 'San Francisco - Oakland', 'Sacramento') THEN
            CASE 
                WHEN MONTH(t.CheckinDate_DT) IN (9, 10) THEN 1.2    -- Best weather
                WHEN MONTH(t.CheckinDate_DT) IN (6, 7, 8) THEN 1.15 -- Tourism
                ELSE 1.0
            END
            
        -- MOUNTAIN CITIES - Winter sports + Summer
        WHEN p."Metro Area" IN ('Denver', 'Denver - Aurora') THEN
            CASE 
                WHEN MONTH(t.CheckinDate_DT) IN (12, 1, 2, 3) THEN 1.3  -- Ski season
                WHEN MONTH(t.CheckinDate_DT) IN (6, 7, 8) THEN 1.2      -- Summer activities
                ELSE 1.0
            END
            
        ELSE 1.0  -- Default for any other metros
    END
FROM hotel_reservations.main.TTH_D_Property p
WHERE t.Property_Code = p.Property_Code;

-- 3. HOLIDAY SPIKES
-- ============================================
UPDATE main.TTH_F_Reservation
SET ADR = ADR * 
    CASE 
        -- Christmas/New Year
        WHEN MONTH(CheckinDate_DT) = 12 AND DAY(CheckinDate_DT) BETWEEN 23 AND 31 THEN 1.5
        -- July 4th week
        WHEN MONTH(CheckinDate_DT) = 7 AND DAY(CheckinDate_DT) BETWEEN 1 AND 7 THEN 1.3
        -- Thanksgiving week
        WHEN MONTH(CheckinDate_DT) = 11 AND DAY(CheckinDate_DT) BETWEEN 22 AND 28 THEN 1.4
        -- Memorial Day weekend
        WHEN MONTH(CheckinDate_DT) = 5 AND DAY(CheckinDate_DT) BETWEEN 25 AND 31 THEN 1.25
        -- Labor Day weekend
        WHEN MONTH(CheckinDate_DT) = 9 AND DAY(CheckinDate_DT) BETWEEN 1 AND 7 THEN 1.25
        ELSE 1.0
    END;

-- 4. BOOKING LEAD TIME ADJUSTMENTS
-- ============================================
UPDATE main.TTH_F_Reservation
SET ADR = ADR * 
    CASE 
        -- Very last minute (0-1 days)
        WHEN DATEDIFF('day', "ReservationDate-DT", CheckinDate_DT) <= 1 THEN 1.15
        -- Last minute (2-7 days)
        WHEN DATEDIFF('day', "ReservationDate-DT", CheckinDate_DT) <= 7 THEN 1.08
        -- Short lead time (8-21 days)
        WHEN DATEDIFF('day', "ReservationDate-DT", CheckinDate_DT) <= 21 THEN 1.0
        -- Advance booking (22-60 days) - small discount
        WHEN DATEDIFF('day', "ReservationDate-DT", CheckinDate_DT) <= 60 THEN 0.95
        -- Far advance booking (60+ days) - bigger discount
        ELSE 0.90
    END;

-- 5. LENGTH OF STAY DISCOUNTS
-- ============================================
UPDATE main.TTH_F_Reservation
SET ADR = ADR * 
    CASE 
        WHEN LengthOfStay >= 7 THEN 0.90   -- Weekly stay discount
        WHEN LengthOfStay >= 4 THEN 0.95   -- Long weekend discount
        WHEN LengthOfStay = 1 THEN 1.05    -- Single night premium
        ELSE 1.0
    END;

-- 6. CHANNEL-BASED PRICING
-- ============================================
UPDATE main.TTH_F_Reservation
SET ADR = ADR * 
    CASE BookingChannel
        WHEN 'Direct' THEN 0.92      -- Best rate guarantee
        WHEN 'OTA' THEN 1.12         -- Commission markup
        WHEN 'Group' THEN 0.85       -- Group negotiated rates
        ELSE 1.0
    END;

-- 7. ADD RANDOM VARIATION (for realism)
-- ============================================
UPDATE main.TTH_F_Reservation
SET ADR = ADR * (0.92 + (RANDOM() * 0.16))  -- ±8% variation
WHERE RANDOM() < 0.15;  -- Only affect 15% of records

-- 8. SPECIAL COMPRESSION NIGHTS (high occupancy = higher rates)
-- ============================================
WITH high_demand_dates AS (
    SELECT Property_Code, CheckinDate_DT, COUNT(*) as booking_count
    FROM main.TTH_F_Reservation
    GROUP BY Property_Code, CheckinDate_DT
    HAVING COUNT(*) > 30  -- Threshold for "high demand"
)
UPDATE main.TTH_F_Reservation t
SET ADR = ADR * 1.15
FROM high_demand_dates h
WHERE t.Property_Code = h.Property_Code 
  AND t.CheckinDate_DT = h.CheckinDate_DT;

-- 9. APPLY CAPS AND FLOORS
-- ============================================
UPDATE main.TTH_F_Reservation
SET ADR = 
    CASE 
        WHEN ADR > 1500 THEN 1500   -- Max cap
        WHEN ADR < 45 THEN 45        -- Min floor
        ELSE ADR
    END;

-- 10. ROUND TO REALISTIC VALUES
-- ============================================
UPDATE main.TTH_F_Reservation
SET ADR = ROUND(ADR, -1) + 9;  -- Round to nearest 10 and add 9 (e.g., 129, 149, 189)

-- Update RM_Revenue to match new ADR
UPDATE main.TTH_F_Reservation
SET RM_Revenue = ADR * LengthOfStay;