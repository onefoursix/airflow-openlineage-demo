# =============================================================================
# Airline Crew Disruption Management - Airflow DAG
# =============================================================================
# Description:
#   Single-task pipeline that reads from vw_crew_disruption_detail and writes
#   a daily crew disruption summary to disruption_summary.
#
#   The SQL is split into two statements passed as a list to
#   SQLExecuteQueryOperator so that the OpenLineage postgres provider can
#   correctly extract BOTH the view read (input) and the table write (output)
#   as separate lineage edges in IBM watsonx.data Intelligence:
#
#     Statement 1: SELECT from vw_crew_disruption_detail into a temp table
#                  -> OpenLineage records vw_crew_disruption_detail as INPUT
#
#     Statement 2: INSERT into disruption_summary from the temp table
#                  -> OpenLineage records disruption_summary as OUTPUT
#
#   View lineage:
#     flights + disruption_events  -> vw_flight_disruptions
#     vw_flight_disruptions + crew_members -> vw_crew_disruption_detail
#
# OpenLineage events are automatically emitted to IBM watsonx.data
# Intelligence via the custom IbmWatsonxTransport.
#
# Prerequisites:
#   - PostgreSQL connection configured in Airflow with conn_id="postgres"
#   - airline_disruption_setup.sql executed against the target database
#   - IBM OpenLineage transport configured via openlineage.yml
#
# Author  : Generated for Airflow + watsonx.data OpenLineage integration
# Created : April 2026
# =============================================================================

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airline_crew_disruption",
    default_args=default_args,
    description=(
        "Read unresolved crew disruptions from vw_crew_disruption_detail, "
        "classify by risk level and write daily summary to disruption_summary"
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["airline", "crew", "disruption", "openlineage"],
) as dag:

    # -------------------------------------------------------------------------
    # Single task: process_disruption_summary
    #
    # Two SQL statements passed as a list so OpenLineage captures both:
    #   - INPUT:  vw_crew_disruption_detail  (from Statement 1 SELECT)
    #   - OUTPUT: disruption_summary         (from Statement 2 INSERT)
    #
    # Statement 1: SELECT from the view into a temp table.
    #   The temp table is session-scoped and dropped automatically when
    #   the connection closes. OpenLineage sees the view as the data source.
    #
    # Statement 2: INSERT from the temp table into disruption_summary,
    #   applying aggregation, risk classification and upsert logic.
    #   OpenLineage sees disruption_summary as the data target.
    # -------------------------------------------------------------------------
    process_disruption_summary = SQLExecuteQueryOperator(
        task_id="process_disruption_summary",
        conn_id="postgres",
        sql=[

            # -----------------------------------------------------------------
            # Statement 1: Read from view into temp table
            # OpenLineage INPUT: vw_crew_disruption_detail
            # -----------------------------------------------------------------
            """
            -- Ensure risk columns exist on the target table (idempotent)
            ALTER TABLE disruption_summary
                ADD COLUMN IF NOT EXISTS risk_flag   VARCHAR(10) DEFAULT 'NORMAL',
                ADD COLUMN IF NOT EXISTS risk_reason TEXT;

            -- Read from view into a session-scoped temp table.
            -- Filtering happens here so OpenLineage can see the view as input.
            -- Only unresolved crew-related disruptions are selected.
            CREATE TEMP TABLE tmp_disruption_staging AS
            SELECT
                flight_number,
                origin,
                destination,
                aircraft_type,
                flight_status,
                passenger_count,
                disruption_id,
                severity,
                estimated_delay_mins,
                cost_estimate,
                crew_id
            FROM vw_crew_disruption_detail
            WHERE is_unresolved      = TRUE
              AND is_crew_disruption = TRUE;
            """,

            # -----------------------------------------------------------------
            # Statement 2: Aggregate, classify risk and write to final table
            # OpenLineage OUTPUT: disruption_summary
            # -----------------------------------------------------------------
            """
            INSERT INTO disruption_summary (
                summary_date,
                flight_number,
                origin,
                destination,
                aircraft_type,
                flight_status,
                passenger_count,
                total_crew_disruptions,
                critical_count,
                high_count,
                medium_count,
                low_count,
                total_delay_mins,
                total_cost_estimate,
                affected_crew_count,
                risk_flag,
                risk_reason,
                processed_at
            )
            WITH aggregated AS (
                -- Aggregate the staging data by flight
                SELECT
                    CURRENT_DATE                                    AS summary_date,
                    flight_number,
                    origin,
                    destination,
                    aircraft_type,
                    flight_status,

                    -- Passenger count is the same across all disruption rows
                    -- for a given flight so MAX avoids double counting
                    MAX(passenger_count)                            AS passenger_count,

                    COUNT(DISTINCT disruption_id)                   AS total_crew_disruptions,

                    COUNT(DISTINCT CASE WHEN severity = 'CRITICAL'
                          THEN disruption_id END)                   AS critical_count,
                    COUNT(DISTINCT CASE WHEN severity = 'HIGH'
                          THEN disruption_id END)                   AS high_count,
                    COUNT(DISTINCT CASE WHEN severity = 'MEDIUM'
                          THEN disruption_id END)                   AS medium_count,
                    COUNT(DISTINCT CASE WHEN severity = 'LOW'
                          THEN disruption_id END)                   AS low_count,

                    COALESCE(SUM(estimated_delay_mins), 0)          AS total_delay_mins,
                    COALESCE(SUM(cost_estimate),        0)          AS total_cost_estimate,

                    COUNT(DISTINCT crew_id)                         AS affected_crew_count

                FROM tmp_disruption_staging
                GROUP BY
                    flight_number,
                    origin,
                    destination,
                    aircraft_type,
                    flight_status
            ),
            with_risk AS (
                -- Classify each flight by risk level
                SELECT
                    a.*,
                    CASE
                        WHEN a.critical_count      >  0   THEN 'CRITICAL'
                        WHEN a.high_count          >  0
                          OR a.total_delay_mins    >= 240
                          OR a.affected_crew_count >  1   THEN 'HIGH'
                        WHEN a.medium_count        >  0
                          OR a.total_delay_mins    >= 60   THEN 'MEDIUM'
                        ELSE                                    'NORMAL'
                    END                                         AS risk_flag,
                    CASE
                        WHEN a.critical_count > 0
                            THEN 'CRITICAL severity crew disruption on flight'
                        WHEN a.affected_crew_count > 1
                            THEN 'Multiple crew members disrupted on same flight'
                        WHEN a.total_delay_mins >= 240
                            THEN 'Cumulative delay exceeds 4 hours'
                        WHEN a.high_count > 0
                            THEN 'HIGH severity crew disruption on flight'
                        WHEN a.total_delay_mins >= 60
                            THEN 'Cumulative delay exceeds 1 hour'
                        ELSE
                            'Within acceptable disruption thresholds'
                    END                                         AS risk_reason
                FROM aggregated a
            )
            SELECT
                summary_date,
                flight_number,
                origin,
                destination,
                aircraft_type,
                flight_status,
                passenger_count,
                total_crew_disruptions,
                critical_count,
                high_count,
                medium_count,
                low_count,
                total_delay_mins,
                total_cost_estimate,
                affected_crew_count,
                risk_flag,
                risk_reason,
                NOW()
            FROM with_risk

            ON CONFLICT (summary_date, flight_number)
            DO UPDATE SET
                flight_status          = EXCLUDED.flight_status,
                total_crew_disruptions = EXCLUDED.total_crew_disruptions,
                critical_count         = EXCLUDED.critical_count,
                high_count             = EXCLUDED.high_count,
                medium_count           = EXCLUDED.medium_count,
                low_count              = EXCLUDED.low_count,
                total_delay_mins       = EXCLUDED.total_delay_mins,
                total_cost_estimate    = EXCLUDED.total_cost_estimate,
                affected_crew_count    = EXCLUDED.affected_crew_count,
                risk_flag              = EXCLUDED.risk_flag,
                risk_reason            = EXCLUDED.risk_reason,
                processed_at           = NOW();
            """,
        ],
    )
