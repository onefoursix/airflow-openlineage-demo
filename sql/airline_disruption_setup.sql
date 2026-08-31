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
-- Author  : Generated for Airflow + watsonx.data OpenLineage integration
-- Created : April 2026
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Drop existing objects (safe re-run)
-- -----------------------------------------------------------------------------
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
    UNIQUE (summary_date, flight_number)
);
