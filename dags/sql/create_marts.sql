-- create_marts.sql
-- Создание и заполнение витрин данных

-- 1. Витрина маршрутов (создается с данными сразу!)
CREATE TABLE mart_route_analysis AS
SELECT 
    r.route_id,
    o.airport_code AS origin_code,
    o.city AS origin_city,
    d.airport_code AS dest_code,
    d.city AS dest_city,
    COUNT(f.flight_id) AS total_flights,
    ROUND(AVG(f.price), 2) AS avg_price,
    MIN(f.price) AS min_price,
    MAX(f.price) AS max_price,
    ROUND(AVG(f.duration), 0) AS avg_duration_min
FROM fact_flights f
JOIN dim_route r ON f.route_id = r.route_id
JOIN dim_airport o ON r.origin_airport_id = o.airport_id
JOIN dim_airport d ON r.destination_airport_id = d.airport_id
GROUP BY r.route_id, o.airport_code, o.city, d.airport_code, d.city
ORDER BY total_flights DESC;

-- 2. Витрина авиакомпаний (SCD2)
CREATE TABLE mart_airline_performance AS
SELECT 
    a.airline_bk AS airline_code,
    a.airline_name,
    a.country,
    COUNT(f.flight_id) AS flights_operated,
    ROUND(AVG(f.price), 2) AS avg_ticket_price,
    ROUND(AVG(f.duration), 0) AS avg_flight_duration_min,
    SUM(CASE WHEN ft.type_category = 'direct' THEN 1 ELSE 0 END) AS direct_flights_count,
    ROUND(SUM(CASE WHEN ft.type_category = 'direct' THEN 1 ELSE 0 END) * 100.0 / COUNT(f.flight_id), 2) AS direct_flights_percent
FROM fact_flights f
JOIN dim_airline a ON f.airline_sk = a.airline_sk AND a.is_current = 1
JOIN dim_flight_type ft ON f.flight_type_id = ft.flight_type_id
GROUP BY a.airline_bk, a.airline_name, a.country;

-- 3. Временные паттерны
CREATE TABLE mart_temporal_patterns AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    d.day_of_week,
    d.day_name,
    d.is_weekend,
    COUNT(f.flight_id) AS flights_count,
    ROUND(AVG(f.price), 2) AS avg_price_this_period,
    SUM(f.price) AS total_revenue_this_period
FROM fact_flights f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name, d.day_of_week, d.day_name, d.is_weekend
ORDER BY d.year, d.month, d.day_of_week;

-- 4. Типы перелетов
CREATE TABLE mart_flight_type_analysis AS
SELECT 
    ft.flight_type_id,
    ft.type_category,
    ft.description,
    ft.transfers_count,
    COUNT(f.flight_id) AS total_flights,
    ROUND(COUNT(f.flight_id) * 100.0 / (SELECT COUNT(*) FROM fact_flights), 2) AS percentage_of_total,
    ROUND(AVG(f.price), 2) AS avg_price_for_type,
    ROUND(AVG(f.duration), 0) AS avg_duration_for_type
FROM fact_flights f
JOIN dim_flight_type ft ON f.flight_type_id = ft.flight_type_id
GROUP BY ft.flight_type_id, ft.type_category, ft.description, ft.transfers_count
ORDER BY total_flights DESC;

-- 5. Сводка по ценам
CREATE TABLE mart_price_summary AS
SELECT 
    'current_month' AS period,
    COUNT(*) AS total_flights,
    ROUND(AVG(price), 2) AS avg_price,
    ROUND(MIN(price), 2) AS min_price,
    ROUND(MAX(price), 2) AS max_price,
    SUM(price) AS total_revenue
FROM fact_flights f
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.month = EXTRACT(MONTH FROM SYSDATE) 
  AND d.year = EXTRACT(YEAR FROM SYSDATE);

-- Индексы для производительности
CREATE INDEX idx_mart_route_origin ON mart_route_analysis(origin_code);
CREATE INDEX idx_mart_route_dest ON mart_route_analysis(dest_code);
CREATE INDEX idx_mart_temp_date ON mart_temporal_patterns(year, month);