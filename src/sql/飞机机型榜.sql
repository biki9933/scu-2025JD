-- 飞机机型榜
-- 飞机型号 执飞航班总数
DROP TABLE IF EXISTS flight_dw.summary_aircraft_ranking;

-- 使用CTAS创建  飞机机型榜  的汇总表
CREATE TABLE flight_dw.summary_aircraft_ranking
COMMENT '飞机机型航班量排名汇总表，用于Superset可视化'
STORED AS PARQUET
AS
SELECT
    da.equipment_description, -- 飞机型号
    COUNT(1) AS flight_count  -- 执飞航班总数
FROM
    flight_dw.fact_flight_ticket ft
JOIN
    flight_dw.dim_aircraft da ON ft.aircraft_fk = da.aircraft_fk
WHERE
    da.equipment_description IS NOT NULL AND da.equipment_description != ''
GROUP BY
    da.equipment_description;