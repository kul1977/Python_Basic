DROP TABLE stg.LOG_EVENTS cascade;
CREATE TABLE stg.LOG_EVENTS (
  `DATE_TIME` datetime NOT NULL,
  `NAME` varchar(255) NOT NULL,
  `CITY` varchar(255) NOT NULL,
  `ZIPCODE` varchar(50) NOT NULL,
  `BBAN` varchar(50) NOT NULL,
  `LOCALE` varchar(50) NOT NULL,
  `BANK_COUNTRY` varchar(5) NOT NULL,
  `IBAN` varchar(50) NOT NULL,
  `COUNTRY_CALLING_CODE` varchar(30) NOT NULL,
  `MSISDN` varchar(50) NOT NULL,
  `PHONE_NUMBER` varchar(50) NOT NULL,
  `STATUS` varchar(50) NOT NULL,
  `GENDER` varchar(5) NOT NULL,
  `STG_SOURCE` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
