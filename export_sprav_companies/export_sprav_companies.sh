#!/bin/bash

TEMP=$(getopt -o "" --long input-file:,input-table:,config-path:,uid:,dry-run -n 'ERROR' -- "$@")
eval set -- "$TEMP"

INPUT_FILE=""
INPUT_TABLE=""
RESULT_INPUT_TABLE=""
CONFIG_PATH=""
NMAPS_UID=""
DRY_RUN=0

while true; do
  case "$1" in
    --input-file)
      INPUT_FILE="$2"
      shift 2
      ;;
    --input-table)
      INPUT_TABLE="$2"
      shift 2
      ;;
    --config-path)
      CONFIG_PATH="$2"
      shift 2
      ;;
    --uid)
      NMAPS_UID="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift 1
      ;;
    --)
      shift
      break
      ;;
  esac
done

if [ -z "$NMAPS_UID" ]; then
  echo "ERROR: empty uid"
  exit 1
fi
if [ -z "$INPUT_FILE" ] && [ -z "$INPUT_TABLE" ]; then
  echo "ERROR: empty input"
  exit 1
fi
if [ -n "$INPUT_FILE" ] && [ -n "$INPUT_TABLE" ]; then
  echo "ERROR: too many input"
  exit 1
fi

if [ -n "$INPUT_FILE" ]; then
    cat query.sql | sed "s/\^\^USER\^\^/$USER/g" | sed "s/\^\^FILE\^\^/$INPUT_FILE/g" > query_tmp.sql
    ya yql -i query_tmp.sql -F $INPUT_FILE
    rm -rf query_tmp.sql
    RESULT_INPUT_TABLE="tmp/$USER/permalinks"
else
    RESULT_INPUT_TABLE=$INPUT_TABLE
fi

if [ "$DRY_RUN" -eq 0 ]; then
    ./add_sprav_companies \
    --companies-yt-path="$RESULT_INPUT_TABLE" \
    --nmap-uid="$NMAPS_UID" \
    --services-cfg-path="$CONFIG_PATH"
else
    ./add_sprav_companies \
    --companies-yt-path="$RESULT_INPUT_TABLE" \
    --nmap-uid="$NMAPS_UID" \
    --services-cfg-path="$CONFIG_PATH" \
    --dry-run
fi
