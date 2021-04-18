#!/bin/bash

RAW_PATH="out/yarspg/raw"
YARSPG_FILES_PATH="$RAW_PATH/composite-merged-fk"

# check if serialized yarspg directory exists
[ -d YARSPG_FILES_PATH ] && echo "$YARSPG_FILES_PATH does not exist, exiting" && exit 1

echo "Started merging files, it make take a while..."

# Add all except metadata
find "$YARSPG_FILES_PATH/" -type f -name '*.yarspg' -exec cat {} \; > "$RAW_PATH/all.yarspg"

echo "File has been successfully merged."