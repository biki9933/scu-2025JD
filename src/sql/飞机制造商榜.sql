 -- 2. 飞机制造商榜
-- 【增强版】从机型描述中提取制造商信息，并统计其航班总数
-- 使用 WITH 子句创建一个名为 manufacturer_data 的临时结果集
WITH manufacturer_data AS (
    SELECT
        -- 在这个子查询内部，我们先创建好 manufacturer 这一列
        CASE
            WHEN da.equipment_description LIKE 'Airbus%' THEN 'Airbus'
            WHEN da.equipment_description LIKE 'Boeing%' THEN 'Boeing'
            WHEN da.equipment_description LIKE 'Embraer%' THEN 'Embraer'
            WHEN da.equipment_description LIKE 'Canadair%' OR da.equipment_description LIKE 'CRJ%' THEN 'Bombardier/Canadair'
            WHEN da.equipment_description LIKE 'McDonnell Douglas%' OR da.equipment_description LIKE 'MD-%' THEN 'McDonnell Douglas'
            ELSE 'Other'
        END AS manufacturer
    FROM
        flight_dw.fact_flight_ticket ft
    JOIN
        flight_dw.dim_aircraft da ON ft.aircraft_fk = da.aircraft_fk
    WHERE
        da.equipment_description IS NOT NULL AND da.equipment_description != ''
)
-- 然后，从上面创建的临时结果集 manufacturer_data 中进行查询
-- 此时 manufacturer 已经是一个真实存在的列了
SELECT
    manufacturer,
    COUNT(1) AS flight_count
FROM
    manufacturer_data
GROUP BY
    manufacturer
ORDER BY
    flight_count DESC;