CREATE DATABASE IF NOT EXISTS baker_hughes;

CREATE SCHEMA IF NOT EXISTS web_scrapes;

CREATE TABLE IF NOT EXISTS web_scrapes.us_states_l_os
(Date DATE, Country TEXT, Region_type TEXT, Region TEXT, Property_type TEXT, Quantity INTEGER);

CREATE TABLE IF NOT EXISTS web_scrapes.canada_provinces_l_os
(Date DATE, Country TEXT, Region_type TEXT, Region TEXT, Property_type TEXT, Quantity INTEGER);