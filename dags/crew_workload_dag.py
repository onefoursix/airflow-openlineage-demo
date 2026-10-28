# =============================================================================
# Airline Crew Workload Summary - Airflow DAG
# =============================================================================
# Description:
#   Joins crew_members to disruption_events on crew_id and writes one row per
#   crew member to crew_workload_summary. This pre-computes the same
#   calculations as the "Crew Disruption Workload Report" dashboard (see
#   openlineage-events/crew-dashboard), including:
#
#       hours_remaining = max_duty_hours - hours_on_duty
#
#   so a dashboard can simply run:
#
#       SELECT * FROM crew_workload_summary ORDER BY total_cost_estimate DESC;
#
#   The load is a single INSERT ... SELECT that reads the two base tables
#   directly (no temp table), so the OpenLineage SQL parser can emit both
#   table-level lineage (crew_members + disruption_events -> crew_workload_summary)
#   and column-level lineage (e.g. hours_on_duty, max_duty_hours -> hours_remaining)
#   to IBM watsonx.data Intelligence.
#
#   The statements run in one transaction (autocommit off), so the dashboard
#   never sees an empty table between the TRUNCATE and the INSERT.
#
# Prerequisites:
#   - PostgreSQL connection configured in Airflow with conn_id="postgres"
#     pointing at the demo database (e.g. host 34.208.67.138, port 5432,
#     database postgres). Keep the password in the Airflow connection, not here.
#   - airline_disruption_setup.sql executed against the target database
#   - IBM OpenLineage transport configured (see README)
# =============================================================================

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airline_crew_workload_summary",
    default_args=default_args,
    description=(
        "Join crew_members to disruption_events and write per-crew workload "
        "metrics (incl. hours_remaining) to crew_workload_summary"
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["airline", "crew", "workload", "openlineage"],
) as dag:

    build_crew_workload_summary = SQLExecuteQueryOperator(
        task_id="build_crew_workload_summary",
        conn_id="postgres",
        autocommit=False,
        sql=[
            # -----------------------------------------------------------------
            # Statement 1: create the target table on first run (idempotent)
            # -----------------------------------------------------------------
            """
            CREATE TABLE IF NOT EXISTS crew_workload_summary (
                crew_id               INTEGER       PRIMARY KEY,
                employee_id           VARCHAR(20)   NOT NULL,
                crew_name             VARCHAR(100)  NOT NULL,
                crew_role             VARCHAR(50)   NOT NULL,
                base_airport          CHAR(3),
                hours_on_duty         NUMERIC(5,1),
                max_duty_hours        NUMERIC(5,1),
                hours_remaining       NUMERIC(5,1),   -- max_duty_hours - hours_on_duty
                disruption_count      INTEGER       NOT NULL,
                critical_disruptions  INTEGER       NOT NULL,
                total_delay_mins      INTEGER       NOT NULL,
                total_cost_estimate   NUMERIC(12,2) NOT NULL,
                last_disruption_at    TIMESTAMP,
                refreshed_at          TIMESTAMP     NOT NULL DEFAULT NOW()
            );
            """,

            # -----------------------------------------------------------------
            # Statement 2: full refresh (same transaction as the INSERT)
            # -----------------------------------------------------------------
            "TRUNCATE TABLE crew_workload_summary;",

            # -----------------------------------------------------------------
            # Statement 3: join, calculate and load
            # OpenLineage INPUTS : crew_members, disruption_events
            # OpenLineage OUTPUT : crew_workload_summary
            # -----------------------------------------------------------------
            """
            INSERT INTO crew_workload_summary (
                crew_id,
                employee_id,
                crew_name,
                crew_role,
                base_airport,
                hours_on_duty,
                max_duty_hours,
                hours_remaining,
                disruption_count,
                critical_disruptions,
                total_delay_mins,
                total_cost_estimate,
                last_disruption_at,
                refreshed_at
            )
            SELECT
                cm.crew_id,
                cm.employee_id,
                cm.full_name                                            AS crew_name,
                cm.role                                                 AS crew_role,
                cm.base_airport,
                cm.hours_on_duty,
                cm.max_duty_hours,
                cm.max_duty_hours - cm.hours_on_duty                    AS hours_remaining,
                COUNT(de.disruption_id)                                 AS disruption_count,
                SUM(CASE WHEN de.severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_disruptions,
                COALESCE(SUM(de.estimated_delay_mins), 0)               AS total_delay_mins,
                COALESCE(SUM(de.cost_estimate), 0)                      AS total_cost_estimate,
                MAX(de.reported_at)                                     AS last_disruption_at,
                NOW()                                                   AS refreshed_at
            FROM crew_members cm
            JOIN disruption_events de
              ON de.crew_id = cm.crew_id
            GROUP BY
                cm.crew_id,
                cm.employee_id,
                cm.full_name,
                cm.role,
                cm.base_airport,
                cm.hours_on_duty,
                cm.max_duty_hours;
            """,
        ],
    )
