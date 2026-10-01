-- =============================================================================
-- Airline Crew Disruption Management - Database Setup
-- =============================================================================
-- Description:
--   Schema for airline crew disruption tracking pipeline using a two-stage
--   view pattern. The Airflow DAG reads the second view and applies its own
--   filtering logic before writing to the final summary table.
--
-- Objects created:
--   Source tables : flights, disruption_events, crew_members
--   View 1        : vw_flight_disruptions   (joins flights + disruption_events)
--   View 2        : vw_crew_disruption_detail (joins vw_flight_disruptions + crew_members)
--   Final table   : disruption_summary      (populated by Airflow DAG)
--
--   Customer / marketing objects:
--   Source table  : customer                (customer master; contains PII)
--   DQ table      : invalid_email           (customers whose email fails validation)
--   Procedure     : sp_load_marketing_leads (customer -> marketing_leads)
--   Target table  : marketing_leads         (PII-free subset + derived columns)
--
-- Pipeline:
--   flights ──────────────────────┐
--                                 ├──► vw_flight_disruptions
--   disruption_events ────────────┘         │
--                                           ├──► vw_crew_disruption_detail
--   crew_members ──────────────────────────┘         │
--                                                     ▼
--                                           Airflow DAG (filters + aggregates)
--                                                     │
--                                                     ▼
--                                           disruption_summary
--
--   customer ──► invalid_email
--   customer ──► sp_load_marketing_leads() ──► marketing_leads
--
-- Author  : Generated for Airflow + watsonx.data OpenLineage integration
-- Created : April 2026
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Drop existing objects (safe re-run)
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS public.sp_load_marketing_leads();
DROP TABLE IF EXISTS marketing_leads         CASCADE;
DROP TABLE IF EXISTS invalid_email           CASCADE;
DROP TABLE IF EXISTS customer                CASCADE;
DROP TABLE IF EXISTS disruption_summary      CASCADE;
DROP TABLE IF EXISTS disruption_events       CASCADE;
DROP TABLE IF EXISTS flights                 CASCADE;
DROP TABLE IF EXISTS crew_members            CASCADE;
DROP VIEW  IF EXISTS vw_crew_disruption_detail;
DROP VIEW  IF EXISTS vw_flight_disruptions;


-- =============================================================================
-- SOURCE TABLE 1: flights
-- Stores scheduled flights with route, aircraft and passenger information
-- =============================================================================
CREATE TABLE flights (
    flight_id       SERIAL        PRIMARY KEY,
    flight_number   VARCHAR(10)   NOT NULL UNIQUE,
    origin          CHAR(3)       NOT NULL,   -- IATA airport code
    destination     CHAR(3)       NOT NULL,   -- IATA airport code
    scheduled_dep   TIMESTAMP     NOT NULL,
    scheduled_arr   TIMESTAMP     NOT NULL,
    aircraft_type   VARCHAR(10)   NOT NULL,   -- B737, A320, B777 etc.
    status          VARCHAR(20)   NOT NULL DEFAULT 'SCHEDULED',
                                              -- SCHEDULED, DELAYED, CANCELLED, DEPARTED
    passenger_count INTEGER       NOT NULL DEFAULT 0,
    created_at      TIMESTAMP     NOT NULL DEFAULT NOW()
);

INSERT INTO flights
    (flight_number, origin, destination, scheduled_dep, scheduled_arr, aircraft_type, status, passenger_count)
VALUES
    ('UA101', 'ORD', 'LAX', '2026-04-12 06:00:00', '2026-04-12 08:30:00', 'B737', 'DELAYED',   189),
    ('UA202', 'LAX', 'JFK', '2026-04-12 09:00:00', '2026-04-12 17:30:00', 'A321', 'SCHEDULED', 200),
    ('UA303', 'JFK', 'SFO', '2026-04-12 10:30:00', '2026-04-12 14:00:00', 'B777', 'CANCELLED', 315),
    ('UA404', 'SFO', 'ORD', '2026-04-12 12:00:00', '2026-04-12 18:00:00', 'B787', 'DELAYED',   248),
    ('UA505', 'ORD', 'JFK', '2026-04-12 14:00:00', '2026-04-12 17:30:00', 'B737', 'SCHEDULED', 175),
    ('UA606', 'JFK', 'LAX', '2026-04-12 15:30:00', '2026-04-12 19:00:00', 'A320', 'SCHEDULED', 180),
    ('UA707', 'LAX', 'SFO', '2026-04-12 07:00:00', '2026-04-12 08:15:00', 'A320', 'DELAYED',   150),
    ('UA808', 'SFO', 'LAX', '2026-04-12 11:00:00', '2026-04-12 12:15:00', 'B737', 'SCHEDULED', 162);


-- =============================================================================
-- SOURCE TABLE 2: disruption_events
-- Stores disruption events linked to flights. crew_id is nullable because
-- not all disruptions are caused by a specific crew member (e.g. weather).
-- =============================================================================
CREATE TABLE disruption_events (
    disruption_id        SERIAL        PRIMARY KEY,
    flight_id            INTEGER       NOT NULL REFERENCES flights(flight_id),
    crew_id              INTEGER,                -- FK to crew_members, set where applicable
    disruption_type      VARCHAR(30)   NOT NULL, -- CREW_SICK, CREW_TIMEOUT, WEATHER, ATC_DELAY
    severity             VARCHAR(10)   NOT NULL, -- LOW, MEDIUM, HIGH, CRITICAL
    description          TEXT,
    estimated_delay_mins INTEGER       NOT NULL DEFAULT 0,
    cost_estimate        NUMERIC(10,2) NOT NULL DEFAULT 0,
    reported_at          TIMESTAMP     NOT NULL DEFAULT NOW(),
    resolved_at          TIMESTAMP,              -- NULL means still active/unresolved
    created_at           TIMESTAMP     NOT NULL DEFAULT NOW()
);

INSERT INTO disruption_events
    (flight_id, crew_id, disruption_type, severity, description,
     estimated_delay_mins, cost_estimate, reported_at, resolved_at)
VALUES
    (1, 5,    'CREW_SICK',    'HIGH',     'Captain Marcus Johnson called in sick 2hrs before departure',                120,  45000.00, '2026-04-12 04:00:00', NULL),
    (1, 2,    'CREW_TIMEOUT', 'MEDIUM',   'First Officer Sarah Chen exceeded max duty hours on previous rotation',       90,  22000.00, '2026-04-12 04:30:00', NULL),
    (3, 5,    'CREW_SICK',    'CRITICAL', 'Captain unavailable - same crew member sick, affects transcon B777 flight',  999,  98000.00, '2026-04-12 05:00:00', NULL),
    (4, 9,    'CREW_TIMEOUT', 'HIGH',     'First Officer Ahmed Hassan grounded by medical - duty time violation',        180,  55000.00, '2026-04-12 06:00:00', NULL),
    (7, 3,    'CREW_TIMEOUT', 'MEDIUM',   'Captain Carlos Rivera approaching max duty hours on LAX-SFO short hop',       60,  18000.00, '2026-04-12 06:30:00', NULL),
    (2, NULL, 'WEATHER',      'LOW',      'Thunderstorms at JFK may cause arrival delays',                               45,   8000.00, '2026-04-12 07:00:00', '2026-04-12 07:30:00'),
    (5, NULL, 'ATC_DELAY',    'LOW',      'ATC ground stop at ORD due to volume',                                        30,   5000.00, '2026-04-12 08:00:00', '2026-04-12 09:00:00');


-- =============================================================================
-- SOURCE TABLE 3: crew_members
-- Stores crew roster with role, qualifications and availability status
-- =============================================================================
CREATE TABLE crew_members (
    crew_id        SERIAL        PRIMARY KEY,
    employee_id    VARCHAR(10)   NOT NULL UNIQUE,
    full_name      VARCHAR(100)  NOT NULL,
    email          VARCHAR(100),              -- intentionally dirty for data-quality demos:
                                              -- two rows have an invalid address (missing '@'
                                              -- and NULL)
    role           VARCHAR(30)   NOT NULL,    -- CAPTAIN, FIRST_OFFICER, FLIGHT_ATTENDANT
    base_airport   CHAR(3)       NOT NULL,    -- IATA airport code
    status         VARCHAR(20)   NOT NULL DEFAULT 'AVAILABLE',
                                              -- AVAILABLE, ON_DUTY, OFF_DUTY, SICK, GROUNDED
    qualifications TEXT[],                    -- aircraft type ratings e.g. {B737, A320}
    hours_on_duty  NUMERIC(4,1)  NOT NULL DEFAULT 0.0,
    max_duty_hours NUMERIC(4,1)  NOT NULL DEFAULT 12.0,
    created_at     TIMESTAMP     NOT NULL DEFAULT NOW()
);

INSERT INTO crew_members
    (employee_id, full_name, email, role, base_airport, status, qualifications, hours_on_duty, max_duty_hours)
VALUES
    ('EMP001', 'James Mitchell',  'james.mitchell@united.com',  'CAPTAIN',          'ORD', 'AVAILABLE', ARRAY['B737','B757'],  0.0,  12.0),
    ('EMP002', 'Sarah Chen',      'sarah.chen@united.com',      'FIRST_OFFICER',    'ORD', 'AVAILABLE', ARRAY['B737'],         0.0,  12.0),
    ('EMP003', 'Carlos Rivera',   'carlos.rivera@united.com',   'CAPTAIN',          'LAX', 'ON_DUTY',   ARRAY['A320','A321'],  6.5,  12.0),
    ('EMP004', 'Priya Sharma',    'priya.sharma@united.com',    'FIRST_OFFICER',    'LAX', 'AVAILABLE', ARRAY['A320'],         0.0,  12.0),
    ('EMP005', 'Marcus Johnson',  'marcus.johnson@united.com',  'CAPTAIN',          'JFK', 'SICK',      ARRAY['B777','B737'],  0.0,  12.0),
    ('EMP006', 'Linda Park',      'linda.park@united.com',      'FLIGHT_ATTENDANT', 'JFK', 'AVAILABLE', ARRAY['B737','B777'],  0.0,  14.0),
    ('EMP007', 'Tom Bradley',     'tom.bradleyunited.com',      'FLIGHT_ATTENDANT', 'ORD', 'AVAILABLE', ARRAY['A320','B737'],  3.0,  14.0),
    ('EMP008', 'Yuki Tanaka',     'yuki.tanaka@united.com',     'CAPTAIN',          'SFO', 'AVAILABLE', ARRAY['B787','B777'],  0.0,  12.0),
    ('EMP009', 'Ahmed Hassan',    NULL,                         'FIRST_OFFICER',    'SFO', 'GROUNDED',  ARRAY['B787'],         0.0,  12.0),
    ('EMP010', 'Rachel O''Brien', 'rachel.obrien@united.com',   'FLIGHT_ATTENDANT', 'LAX', 'AVAILABLE', ARRAY['A320','A321'],  0.0,  14.0);

-- Add FK constraint now that crew_members exists
ALTER TABLE disruption_events
    ADD CONSTRAINT fk_disruption_crew
    FOREIGN KEY (crew_id) REFERENCES crew_members(crew_id);


-- =============================================================================
-- VIEW 1: vw_flight_disruptions
-- Joins flights and disruption_events.
-- Provides a flight-centric view of all disruptions regardless of cause,
-- with derived fields for severity ranking and resolution status.
-- =============================================================================
CREATE OR REPLACE VIEW vw_flight_disruptions AS
SELECT
    -- Disruption fields
    de.disruption_id,
    de.crew_id,
    de.disruption_type,
    de.severity,
    de.description              AS disruption_description,
    de.estimated_delay_mins,
    de.cost_estimate,
    de.reported_at,
    de.resolved_at,
    (de.resolved_at IS NULL)    AS is_unresolved,

    -- Derived severity rank for ordering (lower = more severe)
    CASE de.severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH'     THEN 2
        WHEN 'MEDIUM'   THEN 3
        ELSE                 4
    END                         AS severity_rank,

    -- Flag crew-related vs external disruptions
    (de.disruption_type IN ('CREW_SICK', 'CREW_TIMEOUT')) AS is_crew_disruption,

    -- Flight fields
    f.flight_id,
    f.flight_number,
    f.origin,
    f.destination,
    f.scheduled_dep,
    f.scheduled_arr,
    f.aircraft_type,
    f.status                    AS flight_status,
    f.passenger_count,

    -- Derived flight fields
    EXTRACT(EPOCH FROM (f.scheduled_arr - f.scheduled_dep)) / 3600
                                AS flight_duration_hrs,
    (f.status IN ('DELAYED', 'CANCELLED'))
                                AS is_impacted_flight

FROM disruption_events  de
JOIN flights            f   ON de.flight_id = f.flight_id;


-- =============================================================================
-- VIEW 2: vw_crew_disruption_detail
-- Joins vw_flight_disruptions with crew_members to enrich crew-related
-- disruptions with full crew details. Rows where crew_id is NULL (weather,
-- ATC etc.) are still included via LEFT JOIN with NULL crew columns.
-- This is the view the Airflow DAG reads from.
-- =============================================================================
CREATE OR REPLACE VIEW vw_crew_disruption_detail AS
SELECT
    -- All columns from view 1
    fd.disruption_id,
    fd.disruption_type,
    fd.severity,
    fd.severity_rank,
    fd.disruption_description,
    fd.estimated_delay_mins,
    fd.cost_estimate,
    fd.reported_at,
    fd.resolved_at,
    fd.is_unresolved,
    fd.is_crew_disruption,
    fd.flight_id,
    fd.flight_number,
    fd.origin,
    fd.destination,
    fd.scheduled_dep,
    fd.scheduled_arr,
    fd.aircraft_type,
    fd.flight_status,
    fd.passenger_count,
    fd.flight_duration_hrs,
    fd.is_impacted_flight,

    -- Crew fields (NULL for non-crew disruptions)
    cm.crew_id,
    cm.employee_id,
    cm.full_name                AS crew_name,
    cm.email                    AS crew_email,
    cm.role                     AS crew_role,
    cm.base_airport             AS crew_base_airport,
    cm.status                   AS crew_status,
    cm.qualifications           AS crew_qualifications,
    cm.hours_on_duty,
    cm.max_duty_hours,

    -- Derived crew fields (NULL safe)
    (cm.max_duty_hours - cm.hours_on_duty)
                                AS crew_hours_remaining,
    (cm.status IN ('SICK', 'GROUNDED'))
                                AS crew_is_unavailable,
    (cm.base_airport = fd.origin)
                                AS crew_based_at_origin

FROM vw_flight_disruptions  fd
LEFT JOIN crew_members       cm  ON fd.crew_id = cm.crew_id;


-- =============================================================================
-- FINAL TABLE: disruption_summary
-- Populated by the Airflow DAG which reads vw_crew_disruption_detail
-- and applies the following filtering logic before inserting:
--   - Only unresolved disruptions (is_unresolved = TRUE)
--   - Only crew-related disruption types (is_crew_disruption = TRUE)
--   - Grouped by flight (one row per flight per summary date)
--   - Aggregated delay, cost and severity counts per flight
-- =============================================================================
CREATE TABLE disruption_summary (
    summary_id              SERIAL        PRIMARY KEY,
    summary_date            DATE          NOT NULL,
    flight_number           VARCHAR(10)   NOT NULL,
    origin                  CHAR(3)       NOT NULL,
    destination             CHAR(3)       NOT NULL,
    aircraft_type           VARCHAR(10)   NOT NULL,
    flight_status           VARCHAR(20)   NOT NULL,
    passenger_count         INTEGER       NOT NULL DEFAULT 0,
    total_crew_disruptions  INTEGER       NOT NULL DEFAULT 0,
    critical_count          INTEGER       NOT NULL DEFAULT 0,
    high_count              INTEGER       NOT NULL DEFAULT 0,
    medium_count            INTEGER       NOT NULL DEFAULT 0,
    low_count               INTEGER       NOT NULL DEFAULT 0,
    total_delay_mins        INTEGER       NOT NULL DEFAULT 0,
    total_cost_estimate     NUMERIC(12,2) NOT NULL DEFAULT 0,
    affected_crew_count     INTEGER       NOT NULL DEFAULT 0,
    processed_at            TIMESTAMP     NOT NULL DEFAULT NOW(),
    risk_flag               VARCHAR(10)   DEFAULT 'NORMAL',  -- CRITICAL, HIGH, MEDIUM, NORMAL
    risk_reason             TEXT,                            -- also added by the DAG (ADD COLUMN IF NOT EXISTS)
    UNIQUE (summary_date, flight_number)
);


-- =============================================================================
-- SOURCE TABLE 4: customer
-- Customer master data. Contains PII (ssn, date_of_birth, phone, address) and
-- is used as the source for the marketing and data-quality demos.
-- All rows are SYNTHETIC: SSNs use the never-issued 9xx area, phone numbers use
-- the reserved 555-01xx range, and emails use the reserved example.com domain.
-- Customers 2 and 57 have an email missing '@' for the data-quality demo.
-- =============================================================================
CREATE TABLE customer (
    customer_id    INTEGER,
    first_name     VARCHAR(50),
    last_name      VARCHAR(50),
    ssn            VARCHAR(11),
    date_of_birth  DATE,
    email          VARCHAR(100),    -- intentionally dirty: some rows are missing '@'
    phone_number   VARCHAR(20),
    address_line1  VARCHAR(100),
    address_line2  VARCHAR(100),
    city           VARCHAR(50),
    state          VARCHAR(2),
    zip_code       VARCHAR(10)
);

INSERT INTO customer
    (customer_id, first_name, last_name, ssn, date_of_birth, email, phone_number,
     address_line1, address_line2, city, state, zip_code)
VALUES
    (1, 'Patricia', 'Baker', '960-58-8794', '1991-09-29', 'patricia.baker@example.com', '555-0108', '2027 River Road', 'Apt 2D', 'Phoenix', 'AZ', '85085'),
    (2, 'Aaron', 'Scott', '990-56-9907', '1979-12-31', 'aaron.scottexample.com', '555-0158', '5024 Maple Ave', NULL, 'Austin', 'TX', '78774'),
    (3, 'Lisa', 'Scott', '947-87-8421', '2002-09-27', 'lisa.scott@example.com', '555-0137', '8065 Hillcrest Rd', NULL, 'Minneapolis', 'MN', '55415'),
    (4, 'Jennifer', 'Anderson', '923-27-9218', '1976-04-20', 'jennifer.anderson@example.com', '555-0153', '1610 Birch Court', NULL, 'Boston', 'MA', '02127'),
    (5, 'Sofia', 'Mitchell', '904-56-1532', '1968-05-23', 'sofia.mitchell@example.com', '555-0133', '2433 Cedar Lane', NULL, 'San Diego', 'CA', '92155'),
    (6, 'Ryan', 'Robinson', '943-22-5346', '1975-09-05', 'ryan.robinson@example.com', '555-0184', '574 Pine Road', 'Suite 379', 'Atlanta', 'GA', '30355'),
    (7, 'Ashley', 'Rivera', '937-93-7564', '1998-08-30', 'ashley.rivera@example.com', '555-0128', '7099 Willow Way', 'Suite 314', 'San Diego', 'CA', '92187'),
    (8, 'Nicholas', 'Moore', '966-51-8841', '2003-07-30', 'nicholas.moore@example.com', '555-0182', '5681 Park Place', 'Suite 457', 'Atlanta', 'GA', '30317'),
    (9, 'Chloe', 'Young', '990-89-2944', '2004-07-30', 'chloe.young@example.com', '555-0111', '8449 Pine Road', 'Suite 473', 'San Diego', 'CA', '92196'),
    (10, 'Anthony', 'Thomas', '914-61-4672', '1987-08-03', 'anthony.thomas@example.com', '555-0106', '4929 Cedar Lane', 'Suite 145', 'Denver', 'CO', '80230'),
    (11, 'Tyler', 'Wright', '978-74-1890', '1967-12-23', 'tyler.wright@example.com', '555-0109', '4765 Elm Drive', NULL, 'Salt Lake City', 'UT', '84136'),
    (12, 'Jessica', 'Green', '926-97-3347', '1980-05-25', 'jessica.green@example.com', '555-0192', '5414 Pine Road', 'Suite 247', 'Charlotte', 'NC', '28287'),
    (13, 'Rebecca', 'Thompson', '910-74-8385', '1969-11-01', 'rebecca.thompson@example.com', '555-0182', '2204 Sunset Ave', NULL, 'Salt Lake City', 'UT', '84125'),
    (14, 'Aaron', 'Lee', '949-31-3395', '1985-01-19', 'aaron.lee@example.com', '555-0193', '2049 Willow Way', 'Suite 224', 'Boston', 'MA', '02182'),
    (15, 'Brian', 'Wilson', '935-15-6428', '2001-03-09', 'brian.wilson@example.com', '555-0172', '4481 Cedar Lane', NULL, 'Portland', 'OR', '97289'),
    (16, 'Jacob', 'Davis', '983-75-7286', '1965-09-01', 'jacob.davis@example.com', '555-0142', '6172 Maple Ave', NULL, 'Portland', 'OR', '97206'),
    (17, 'Sandra', 'Lopez', '990-89-1713', '1984-04-12', 'sandra.lopez@example.com', '555-0150', '8795 Park Place', 'Apt 7B', 'Phoenix', 'AZ', '85018'),
    (18, 'Elizabeth', 'Scott', '932-95-7574', '1973-02-25', 'elizabeth.scott@example.com', '555-0128', '2736 Oak Street', NULL, 'Seattle', 'WA', '98168'),
    (19, 'Jessica', 'Clark', '901-56-5401', '1975-01-14', 'jessica.clark@example.com', '555-0180', '8870 Maple Ave', NULL, 'Columbus', 'OH', '43209'),
    (20, 'Linda', 'Davis', '908-39-8703', '1965-02-13', 'linda.davis@example.com', '555-0169', '129 Lakeview Blvd', 'Suite 252', 'Portland', 'OR', '97228'),
    (21, 'Mark', 'Johnson', '984-66-1915', '1968-06-01', 'mark.johnson@example.com', '555-0192', '2417 Cedar Lane', NULL, 'Salt Lake City', 'UT', '84151'),
    (22, 'William', 'Thompson', '915-15-2485', '1984-10-18', 'william.thompson@example.com', '555-0134', '7524 Lakeview Blvd', NULL, 'Austin', 'TX', '78708'),
    (23, 'Kevin', 'Reyes', '977-39-2641', '1971-06-09', 'kevin.reyes@example.com', '555-0189', '5627 Willow Way', NULL, 'Miami', 'FL', '33143'),
    (24, 'Jason', 'Lopez', '930-31-3292', '1995-04-26', 'jason.lopez@example.com', '555-0162', '8412 Cedar Lane', 'Suite 237', 'Denver', 'CO', '80217'),
    (25, 'Elizabeth', 'Allen', '933-47-4191', '1998-10-03', 'elizabeth.allen@example.com', '555-0169', '2300 Lakeview Blvd', 'Apt 3D', 'Portland', 'OR', '97246'),
    (26, 'Daniel', 'Young', '961-27-4623', '1993-02-04', 'daniel.young@example.com', '555-0144', '367 Oak Street', 'Apt 2B', 'Miami', 'FL', '33135'),
    (27, 'Olivia', 'Martinez', '912-17-6817', '1990-03-10', 'olivia.martinez@example.com', '555-0152', '2219 Lakeview Blvd', 'Suite 380', 'Portland', 'OR', '97250'),
    (28, 'Anthony', 'Adams', '936-16-7879', '1997-04-22', 'anthony.adams@example.com', '555-0142', '8235 Birch Court', NULL, 'San Diego', 'CA', '92196'),
    (29, 'Brian', 'Lewis', '981-66-3699', '1974-06-21', 'brian.lewis@example.com', '555-0199', '9364 Sunset Ave', NULL, 'Atlanta', 'GA', '30353'),
    (30, 'Melissa', 'Cooper', '922-30-4993', '1980-10-04', 'melissa.cooper@example.com', '555-0187', '7442 River Road', 'Apt 28D', 'Boston', 'MA', '02179'),
    (31, 'Elizabeth', 'Adams', '989-17-3044', '2003-12-15', 'elizabeth.adams@example.com', '555-0148', '9109 Park Place', NULL, 'San Diego', 'CA', '92134'),
    (32, 'Eric', 'Nelson', '945-46-6056', '1966-06-01', 'eric.nelson@example.com', '555-0110', '9992 Lakeview Blvd', NULL, 'Portland', 'OR', '97207'),
    (33, 'Brian', 'Campbell', '980-82-3578', '1995-01-17', 'brian.campbell@example.com', '555-0127', '8701 Lakeview Blvd', 'Suite 384', 'Salt Lake City', 'UT', '84142'),
    (34, 'Sandra', 'Adams', '973-63-5052', '1985-11-30', 'sandra.adams@example.com', '555-0155', '5096 Hillcrest Rd', 'Apt 6A', 'San Diego', 'CA', '92163'),
    (35, 'Susan', 'Taylor', '978-16-7886', '2000-02-10', 'susan.taylor@example.com', '555-0109', '3428 Maple Ave', NULL, 'Austin', 'TX', '78782'),
    (36, 'Elizabeth', 'Anderson', '919-31-7453', '1967-11-28', 'elizabeth.anderson@example.com', '555-0158', '2923 Hillcrest Rd', NULL, 'Chicago', 'IL', '60699'),
    (37, 'Joseph', 'Torres', '916-65-5228', '1988-08-04', 'joseph.torres@example.com', '555-0117', '8479 River Road', NULL, 'Chicago', 'IL', '60698'),
    (38, 'Michelle', 'Brown', '934-13-8074', '1978-12-06', 'michelle.brown@example.com', '555-0168', '7391 Cedar Lane', NULL, 'Charlotte', 'NC', '28295'),
    (39, 'Jacob', 'Lewis', '903-35-1165', '1967-12-05', 'jacob.lewis@example.com', '555-0115', '3263 Park Place', NULL, 'Denver', 'CO', '80202'),
    (40, 'Chloe', 'Lopez', '963-50-2624', '1985-02-05', 'chloe.lopez@example.com', '555-0109', '9892 Birch Court', NULL, 'Salt Lake City', 'UT', '84167'),
    (41, 'Jennifer', 'Martinez', '977-39-1271', '1986-01-19', 'jennifer.martinez@example.com', '555-0138', '3352 River Road', NULL, 'Minneapolis', 'MN', '55470'),
    (42, 'Michelle', 'Wright', '955-91-2015', '1992-10-11', 'michelle.wright@example.com', '555-0163', '7174 Pine Road', NULL, 'Minneapolis', 'MN', '55417'),
    (43, 'Anthony', 'Brown', '926-66-7324', '1988-12-16', 'anthony.brown@example.com', '555-0178', '6726 Maple Ave', 'Apt 11A', 'Austin', 'TX', '78705'),
    (44, 'Jason', 'Mitchell', '956-27-7922', '1985-12-31', 'jason.mitchell@example.com', '555-0160', '7468 Elm Drive', NULL, 'San Diego', 'CA', '92180'),
    (45, 'Patricia', 'Thomas', '911-27-1757', '1964-10-25', 'patricia.thomas@example.com', '555-0127', '141 Park Place', 'Apt 19B', 'Boston', 'MA', '02130'),
    (46, 'James', 'Baker', '936-85-8108', '1996-07-28', 'james.baker@example.com', '555-0108', '1410 Willow Way', NULL, 'Portland', 'OR', '97201'),
    (47, 'Michelle', 'Garcia', '979-82-9237', '1985-10-04', 'michelle.garcia@example.com', '555-0103', '8343 Willow Way', NULL, 'Seattle', 'WA', '98187'),
    (48, 'Tyler', 'Martinez', '921-29-5239', '1997-02-17', 'tyler.martinez@example.com', '555-0146', '4287 River Road', 'Apt 25A', 'Phoenix', 'AZ', '85051'),
    (49, 'Thomas', 'Thompson', '983-52-2070', '1998-11-10', 'thomas.thompson@example.com', '555-0177', '1485 Birch Court', 'Suite 475', 'San Diego', 'CA', '92102'),
    (50, 'James', 'Torres', '988-41-8153', '1983-01-19', 'james.torres@example.com', '555-0122', '3780 Sunset Ave', NULL, 'Nashville', 'TN', '37215'),
    (51, 'Sandra', 'Clark', '913-52-2986', '1974-11-19', 'sandra.clark@example.com', '555-0170', '5267 Lakeview Blvd', NULL, 'Miami', 'FL', '33134'),
    (52, 'Tyler', 'Thompson', '912-76-1188', '1979-10-05', 'tyler.thompson@example.com', '555-0141', '6187 Pine Road', 'Apt 8C', 'Charlotte', 'NC', '28267'),
    (53, 'Eric', 'Scott', '909-83-4362', '1991-12-27', 'eric.scott@example.com', '555-0135', '5407 Cedar Lane', NULL, 'Chicago', 'IL', '60621'),
    (54, 'Ashley', 'Young', '948-56-3362', '1990-11-01', 'ashley.young@example.com', '555-0106', '3764 Cedar Lane', NULL, 'Charlotte', 'NC', '28228'),
    (55, 'Emily', 'Anderson', '999-86-2061', '1989-05-13', 'emily.anderson@example.com', '555-0198', '5062 Lakeview Blvd', NULL, 'Phoenix', 'AZ', '85063'),
    (56, 'Chloe', 'Martin', '999-41-2984', '1996-12-12', 'chloe.martin@example.com', '555-0195', '903 Birch Court', NULL, 'Charlotte', 'NC', '28215'),
    (57, 'Daniel', 'Hall', '971-58-3257', '1969-02-03', 'daniel.hallexample.com', '555-0171', '1924 Elm Drive', 'Suite 420', 'Phoenix', 'AZ', '85014'),
    (58, 'Sarah', 'Thomas', '919-72-4587', '1984-09-26', 'sarah.thomas@example.com', '555-0182', '9223 Cedar Lane', NULL, 'San Diego', 'CA', '92160'),
    (59, 'Jason', 'Lewis', '972-60-9309', '1965-10-23', 'jason.lewis@example.com', '555-0195', '8318 Elm Drive', NULL, 'San Diego', 'CA', '92111'),
    (60, 'Susan', 'Lee', '999-95-5470', '2003-06-06', 'susan.lee@example.com', '555-0116', '1468 Elm Drive', NULL, 'Minneapolis', 'MN', '55455'),
    (61, 'Christopher', 'Adams', '928-95-3536', '1970-01-01', 'christopher.adams@example.com', '555-0159', '4775 Oak Street', 'Suite 102', 'Atlanta', 'GA', '30328'),
    (62, 'Matthew', 'Robinson', '902-25-2008', '1963-08-04', 'matthew.robinson@example.com', '555-0184', '3290 Elm Drive', NULL, 'Minneapolis', 'MN', '55496'),
    (63, 'David', 'King', '956-48-8031', '1968-11-04', 'david.king@example.com', '555-0158', '5791 Elm Drive', NULL, 'Nashville', 'TN', '37201'),
    (64, 'Brian', 'Robinson', '911-75-5251', '1988-02-09', 'brian.robinson@example.com', '555-0169', '2333 Elm Drive', NULL, 'Charlotte', 'NC', '28283'),
    (65, 'David', 'Thompson', '931-47-6182', '1991-04-01', 'david.thompson@example.com', '555-0128', '5156 Maple Ave', NULL, 'Chicago', 'IL', '60650'),
    (66, 'Daniel', 'Wilson', '933-97-9044', '1962-12-04', 'daniel.wilson@example.com', '555-0176', '5024 Pine Road', 'Suite 377', 'Nashville', 'TN', '37253'),
    (67, 'Christopher', 'Baker', '966-36-6602', '2000-01-16', 'christopher.baker@example.com', '555-0124', '9746 Maple Ave', NULL, 'Phoenix', 'AZ', '85075'),
    (68, 'Thomas', 'Flores', '908-67-2022', '1993-04-05', 'thomas.flores@example.com', '555-0129', '3528 Park Place', NULL, 'Columbus', 'OH', '43241'),
    (69, 'Michael', 'Nguyen', '981-84-4632', '2003-11-27', 'michael.nguyen@example.com', '555-0110', '9576 Pine Road', NULL, 'Atlanta', 'GA', '30310'),
    (70, 'Betty', 'Davis', '984-85-9666', '1986-01-18', 'betty.davis@example.com', '555-0121', '1216 Elm Drive', 'Apt 15B', 'Salt Lake City', 'UT', '84178'),
    (71, 'Christopher', 'Miller', '971-79-9968', '1978-01-30', 'christopher.miller@example.com', '555-0102', '6818 Pine Road', NULL, 'Nashville', 'TN', '37239'),
    (72, 'Sandra', 'Mitchell', '958-39-4342', '1988-09-17', 'sandra.mitchell@example.com', '555-0145', '5497 Park Place', 'Suite 178', 'Salt Lake City', 'UT', '84198'),
    (73, 'James', 'Flores', '994-27-8290', '1977-05-28', 'james.flores@example.com', '555-0120', '8027 Lakeview Blvd', 'Apt 13C', 'Columbus', 'OH', '43294'),
    (74, 'Noah', 'Adams', '986-64-3147', '1976-09-01', 'noah.adams@example.com', '555-0130', '7912 Pine Road', NULL, 'Denver', 'CO', '80212'),
    (75, 'Jennifer', 'Hall', '951-52-8265', '1985-01-19', 'jennifer.hall@example.com', '555-0179', '2787 Willow Way', 'Apt 21A', 'San Diego', 'CA', '92156'),
    (76, 'Aaron', 'Davis', '972-14-1845', '1985-01-14', 'aaron.davis@example.com', '555-0120', '1464 Lakeview Blvd', NULL, 'Nashville', 'TN', '37287'),
    (77, 'Anthony', 'Kim', '991-70-9135', '2003-09-17', 'anthony.kim@example.com', '555-0125', '4524 River Road', 'Apt 20B', 'Miami', 'FL', '33148'),
    (78, 'Jacob', 'Wright', '978-56-8607', '1967-10-07', 'jacob.wright@example.com', '555-0132', '4444 Hillcrest Rd', 'Apt 9B', 'Minneapolis', 'MN', '55461'),
    (79, 'Ashley', 'Brown', '902-31-8335', '1972-06-30', 'ashley.brown@example.com', '555-0145', '4787 Birch Court', 'Apt 14C', 'Atlanta', 'GA', '30304'),
    (80, 'Karen', 'Miller', '971-16-7950', '1986-10-25', 'karen.miller@example.com', '555-0192', '3266 Cedar Lane', 'Apt 30C', 'Denver', 'CO', '80213'),
    (81, 'Lisa', 'Taylor', '989-78-4194', '1969-01-26', 'lisa.taylor@example.com', '555-0132', '1454 Oak Street', NULL, 'Boston', 'MA', '02113'),
    (82, 'Christopher', 'White', '922-16-9102', '1993-07-29', 'christopher.white@example.com', '555-0129', '6714 Birch Court', NULL, 'Denver', 'CO', '80244'),
    (83, 'Jennifer', 'Taylor', '921-74-5851', '1983-12-11', 'jennifer.taylor@example.com', '555-0102', '8998 Oak Street', NULL, 'Charlotte', 'NC', '28295'),
    (84, 'Patricia', 'Harris', '938-22-2115', '1976-08-08', 'patricia.harris@example.com', '555-0129', '1956 Cedar Lane', NULL, 'Austin', 'TX', '78743'),
    (85, 'Robert', 'Reyes', '940-75-1430', '1998-07-08', 'robert.reyes@example.com', '555-0167', '4182 Sunset Ave', NULL, 'Nashville', 'TN', '37206'),
    (86, 'Tyler', 'Jackson', '917-27-2063', '2001-07-10', 'tyler.jackson@example.com', '555-0107', '1304 Willow Way', NULL, 'Boston', 'MA', '02108'),
    (87, 'Christopher', 'Nguyen', '945-18-4180', '1989-12-20', 'christopher.nguyen@example.com', '555-0100', '9374 Park Place', 'Suite 244', 'Miami', 'FL', '33134'),
    (88, 'Amanda', 'Lee', '949-71-1679', '1983-11-14', 'amanda.lee@example.com', '555-0113', '5137 Lakeview Blvd', NULL, 'Nashville', 'TN', '37263'),
    (89, 'Amanda', 'Cooper', '914-68-4155', '1962-04-06', 'amanda.cooper@example.com', '555-0105', '6890 Willow Way', NULL, 'Phoenix', 'AZ', '85021'),
    (90, 'Hannah', 'O''Neil', '988-96-9135', '1991-01-01', 'hannah.oneil@example.com', '555-0104', '446 Hillcrest Rd', 'Apt 1A', 'Chicago', 'IL', '60612'),
    (91, 'Sarah', 'Nelson', '913-60-9505', '1994-10-31', 'sarah.nelson@example.com', '555-0134', '2606 Willow Way', 'Apt 15C', 'Denver', 'CO', '80204'),
    (92, 'Laura', 'Thomas', '927-31-2989', '1964-02-19', 'laura.thomas@example.com', '555-0171', '475 Oak Street', 'Apt 3B', 'Miami', 'FL', '33192'),
    (93, 'Elizabeth', 'Flores', '957-76-8128', '1966-06-12', 'elizabeth.flores@example.com', '555-0136', '1959 Hillcrest Rd', 'Suite 113', 'Atlanta', 'GA', '30395'),
    (94, 'Matthew', 'Allen', '935-68-2918', '1987-05-22', 'matthew.allen@example.com', '555-0151', '7234 Pine Road', 'Suite 151', 'Phoenix', 'AZ', '85085'),
    (95, 'Mark', 'Clark', '978-70-3588', '1975-03-13', 'mark.clark@example.com', '555-0195', '8440 Pine Road', 'Apt 17D', 'Charlotte', 'NC', '28238'),
    (96, 'Susan', 'Martin', '952-23-5185', '1997-03-18', 'susan.martin@example.com', '555-0112', '2243 Hillcrest Rd', 'Apt 7D', 'Seattle', 'WA', '98198'),
    (97, 'Ethan', 'Carter', '977-41-7922', '1980-03-18', 'ethan.carter@example.com', '555-0120', '1685 Maple Ave', 'Apt 10C', 'Seattle', 'WA', '98180'),
    (98, 'Matthew', 'Lewis', '914-75-4997', '1988-05-20', 'matthew.lewis@example.com', '555-0174', '6562 Sunset Ave', NULL, 'Denver', 'CO', '80277'),
    (99, 'Tyler', 'King', '982-41-5302', '1992-05-12', 'tyler.king@example.com', '555-0160', '2280 Sunset Ave', NULL, 'Denver', 'CO', '80206'),
    (100, 'Karen', 'Scott', '969-40-5009', '1977-09-02', 'karen.scott@example.com', '555-0154', '9914 Pine Road', NULL, 'Nashville', 'TN', '37251'),
    (101, 'Amanda', 'Brown', '948-16-1947', '2001-02-27', 'amanda.brown@example.com', '555-0122', '9437 Sunset Ave', NULL, 'Columbus', 'OH', '43297'),
    (102, 'Sandra', 'White', '933-19-5864', '1973-09-20', 'sandra.white@example.com', '555-0142', '7517 Birch Court', 'Apt 13A', 'Salt Lake City', 'UT', '84119');


-- =============================================================================
-- DQ TABLE: invalid_email
-- Customers whose email address fails basic format validation.
-- =============================================================================
CREATE TABLE invalid_email (
    email          VARCHAR(100),
    customer_id    INTEGER,
    last_name      VARCHAR(50),
    first_name     VARCHAR(50)
);

INSERT INTO invalid_email (email, customer_id, last_name, first_name)
SELECT email, customer_id, last_name, first_name
FROM customer
WHERE email IS NOT NULL
  AND email !~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$';


-- =============================================================================
-- TARGET TABLE: marketing_leads
-- PII-free subset of customer with derived columns, populated by
-- sp_load_marketing_leads(). ssn, date_of_birth, phone and street address are
-- deliberately excluded.
-- =============================================================================
CREATE TABLE marketing_leads (
    customer_id    INTEGER       PRIMARY KEY,
    full_name      VARCHAR(201),            -- first_name || ' ' || last_name
    email          VARCHAR,                 -- lower(email)
    city           VARCHAR,
    state          VARCHAR,
    age            INTEGER,                 -- whole years from date_of_birth
    age_band       VARCHAR(10),             -- 18-24, 25-34, 35-44, 45-54, 55+, unknown
    email_is_valid BOOLEAN,                 -- regex format check
    loaded_at      TIMESTAMP     DEFAULT NOW()
);


-- =============================================================================
-- PROCEDURE: sp_load_marketing_leads
-- Full refresh of marketing_leads from customer (truncate + insert).
-- Static SQL only, so lineage scanners can trace column-level lineage.
-- Usage: CALL sp_load_marketing_leads();
-- =============================================================================
CREATE OR REPLACE PROCEDURE public.sp_load_marketing_leads()
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TABLE IF NOT EXISTS public.marketing_leads (
        customer_id    integer PRIMARY KEY,
        full_name      varchar(201),
        email          varchar,
        city           varchar,
        state          varchar,
        age            integer,
        age_band       varchar(10),
        email_is_valid boolean,
        loaded_at      timestamp DEFAULT now()
    );

    TRUNCATE TABLE public.marketing_leads;

    INSERT INTO public.marketing_leads
        (customer_id, full_name, email, city, state, age, age_band, email_is_valid)
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name                        AS full_name,
        lower(c.email)                                            AS email,
        c.city,
        c.state,
        date_part('year', age(current_date, c.date_of_birth))::int AS age,
        CASE
            WHEN c.date_of_birth IS NULL THEN 'unknown'
            WHEN age(current_date, c.date_of_birth) < interval '25 years' THEN '18-24'
            WHEN age(current_date, c.date_of_birth) < interval '35 years' THEN '25-34'
            WHEN age(current_date, c.date_of_birth) < interval '45 years' THEN '35-44'
            WHEN age(current_date, c.date_of_birth) < interval '55 years' THEN '45-54'
            ELSE '55+'
        END                                                       AS age_band,
        coalesce(c.email ~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$', false) AS email_is_valid
    FROM public.customer c;
END;
$$;

CALL public.sp_load_marketing_leads();
