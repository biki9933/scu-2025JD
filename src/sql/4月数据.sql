-- flight_dw.flight_data_202204 definition

CREATE TABLE `flight_data_202204` (
  `leg_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `flight_date` date DEFAULT NULL,
  `day_of_week` int(11) DEFAULT NULL,
  `flight_month` int(11) DEFAULT NULL,
  `airline_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `departure_airport_code` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `arrival_airport_code` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `base_fare` float DEFAULT NULL,
  `total_fare` float DEFAULT NULL,
  `stops` int(11) DEFAULT NULL,
  `search_date` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE flight_data_202204 AS
SELECT
  f.leg_id,
  fd.full_date AS flight_date,
  fd.day_of_week,
  fd.month AS flight_month,

  a.airline_name,
  dap.iata_code AS departure_airport_code,
  aap.iata_code AS arrival_airport_code,

  f.base_fare,
  f.total_fare,

  CASE
    WHEN fs.is_non_stop = true THEN 0
    ELSE fs.total_segments - 1
  END AS stops,

  fs.search_date

FROM fact_flight_ticket f
LEFT JOIN dim_date fd ON f.flight_date_fk = fd.date_fk
LEFT JOIN dim_airline a ON f.airline_fk = a.airline_fk
LEFT JOIN dim_airport dap ON f.departure_airport_fk = dap.airport_fk
LEFT JOIN dim_airport aap ON f.arrival_airport_fk = aap.airport_fk
LEFT JOIN fact_flight_segments fs ON f.leg_id = fs.leg_id

WHERE fd.year = 2022 AND fd.month = 4;