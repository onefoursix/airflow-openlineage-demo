#!/bin/bash
# Refreshes eventTime/runId/nominalTime/dagRun/taskInstance fields in the
# airflow OpenLineage event files so they look like a fresh DAG run before
# replaying them into watsonx.data intelligence.
#
# This mirrors ../mock-dashboard/refresh-mock-dashboard-events.sh, but the
# airflow events carry more identifiers that all have to stay internally
# consistent:
#   - run.runId              the TASK run id (same value in both files)
#   - run.facets.parent.*    the PARENT DAG run id (same value in both files,
#                             distinct from the task run id above)
#   - run.facets.airflow.taskUuid            a task-instance-scoped id
#   - run.facets.airflow.dagRun.run_id       an Airflow-style string that
#                             embeds the run_after timestamp, e.g.
#                             "manual__2026-08-31T14:22:11.123456+00:00"
#   - run.facets.airflow.taskInstance.log_url embeds a URL-encoded copy of
#                             that same dagRun.run_id string
#
# Usage: ./refresh-airflow-events.sh
set -e

START_FILE="./airflow-start-event.json"
COMPLETE_FILE="./airflow-complete-event.json"

python3 - "$START_FILE" "$COMPLETE_FILE" <<'EOF'
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

start_file, complete_file = sys.argv[1:]

def iso(dt):
    # e.g. 2026-08-31T14:22:11.123456+00:00
    return dt.isoformat()

def iso_short(dt):
    # e.g. 2026-08-31T14:22:11+00:00 (no microseconds)
    return dt.replace(microsecond=0).isoformat()

now = datetime.now(timezone.utc)

# Identifiers - task run id and parent DAG run id must differ, and each must
# be identical across the start and complete event files.
run_id = str(uuid.uuid4())
parent_run_id = str(uuid.uuid4())
task_uuid = str(uuid.uuid4())

# Nominal/logical time for the DAG run itself.
nominal_time = iso_short(now)

# Airflow's own dagRun.run_id string embeds the trigger timestamp; start_date
# follows shortly after (mirrors the ~0.3s gap seen in real captures).
run_after_dt = now + timedelta(seconds=1)
run_after = iso(run_after_dt)
dag_run_id_str = f"manual__{run_after}"
dag_start_date = iso(run_after_dt + timedelta(microseconds=275000))
log_url_run_id = quote(dag_run_id_str, safe="")

# Top-level eventTime: start now, complete a few seconds later (task duration).
start_event_time = iso(now)
complete_event_time = iso(now + timedelta(seconds=3, microseconds=421000))
complete_nominal_end = iso_short(now + timedelta(seconds=3))


def apply_common(doc):
    doc["run"]["runId"] = run_id

    facets = doc["run"]["facets"]

    nt = facets["nominalTime"]
    nt["nominalStartTime"] = nominal_time

    parent = facets["parent"]
    parent["run"]["runId"] = parent_run_id
    parent["root"]["run"]["runId"] = parent_run_id

    af = facets["airflow"]
    af["taskUuid"] = task_uuid

    dag_run = af["dagRun"]
    dag_run["data_interval_start"] = nominal_time
    dag_run["data_interval_end"] = nominal_time
    dag_run["logical_date"] = nominal_time
    dag_run["run_after"] = run_after
    dag_run["run_id"] = dag_run_id_str
    dag_run["start_date"] = dag_start_date

    task_instance = af["taskInstance"]
    task_instance["log_url"] = (
        "http://localhost:8080/dags/airline_crew_disruption/runs/"
        f"{log_url_run_id}/tasks/process_disruption_summary?try_number=1"
    )


with open(start_file) as f:
    start = json.load(f)
apply_common(start)
start["eventTime"] = start_event_time
start["run"]["facets"]["nominalTime"]["nominalEndTime"] = nominal_time
with open(start_file, "w") as f:
    json.dump(start, f, indent=2)
    f.write("\n")

with open(complete_file) as f:
    complete = json.load(f)
apply_common(complete)
complete["eventTime"] = complete_event_time
complete["run"]["facets"]["nominalTime"]["nominalEndTime"] = complete_nominal_end
with open(complete_file, "w") as f:
    json.dump(complete, f, indent=2)
    f.write("\n")

print(f"runId:        {run_id}")
print(f"parentRunId:  {parent_run_id}")
print(f"taskUuid:     {task_uuid}")
print(f"start:        {start_event_time}")
print(f"complete:     {complete_event_time}")
EOF

echo "Refreshed: $START_FILE"
echo "Refreshed: $COMPLETE_FILE"
