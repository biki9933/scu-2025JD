-- flight_dw.fact_flight_segments definition

CREATE TABLE `fact_flight_segments` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `leg_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'èˆªæ®µIDï¼Œä½œä¸ºä¸»é”®',
  `total_segments` int(11) NOT NULL DEFAULT '0' COMMENT 'æ€»èˆªæ®µæ•°',
  `is_non_stop` varchar(5) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '0' COMMENT 'æ˜¯å¦ç›´è¾¾ (1:æ˜¯, 0:å¦)',
  `search_date` date NOT NULL COMMENT 'æœç´¢æ—¥æœŸ',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=18350001 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='èˆªæ®µä¿¡æ¯è¡¨';