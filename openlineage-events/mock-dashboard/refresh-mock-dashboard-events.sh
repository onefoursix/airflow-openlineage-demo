#!/bin/bash
# Refreshes eventTime/nominalTime/runId in the mock-dashboard OpenLineage event
# files so they look like a fresh run before replaying them into .data intelligence.
# Usage: ./refresh-openlineage-events.sh
set -e

START_FILE="./dashboard-start-event.json"
COMPLETE_FILE="./dashboard-complete-event.json"

RUN_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S.000000+00:00")
COMPLETE_TIME=$(date -u -v+3S +"%Y-%m-%dT%H:%M:%S.421000+00:00" 2>/dev/null \
  || date -u -d "+3 seconds" +"%Y-%m-%dT%H:%M:%S.421000+00:00")
START_TIME_SHORT=$(date -u +"%Y-%m-%dT%H:%M:%S+00:00")
COMPLETE_TIME_SHORT=$(date -u -v+3S +"%Y-%m-%dT%H:%M:%S+00:00" 2>/dev/null \
  || date -u -d "+3 seconds" +"%Y-%m-%dT%H:%M:%S+00:00")

python3 - "$START_FILE" "$COMPLETE_FILE" "$RUN_ID" "$START_TIME" "$COMPLETE_TIME" "$START_TIME_SHORT" "$COMPLETE_TIME_SHORT" <<'EOF'
import json
import sys

start_file, complete_file, run_id, start_time, complete_time, start_short, complete_short = sys.argv[1:]

with open(start_file) as f:
    start = json.load(f)
start["eventTime"] = start_time
start["run"]["runId"] = run_id
start["run"]["facets"]["nominalTime"]["nominalStartTime"] = start_short
start["run"]["facets"]["nominalTime"]["nominalEndTime"] = start_short
with open(start_file, "w") as f:
    json.dump(start, f, indent=2)
    f.write("\n")

with open(complete_file) as f:
    complete = json.load(f)
complete["eventTime"] = complete_time
complete["run"]["runId"] = run_id
complete["run"]["facets"]["nominalTime"]["nominalStartTime"] = start_short
complete["run"]["facets"]["nominalTime"]["nominalEndTime"] = complete_short
with open(complete_file, "w") as f:
    json.dump(complete, f, indent=2)
    f.write("\n")

print(f"runId:    {run_id}")
print(f"start:    {start_time}")
print(f"complete: {complete_time}")
EOF

echo "Refreshed: $START_FILE"
echo "Refreshed: $COMPLETE_FILE"
