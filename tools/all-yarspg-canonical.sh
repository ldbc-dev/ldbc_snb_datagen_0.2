#!/bin/bash

RAW_PATH="out/yarspg/raw"
YARSPG_FILES_PATH="$RAW_PATH/composite-merged-fk"
TEMP_FILE_PATH="$RAW_PATH/temp.yarspg"

# check if serialized yarspg directory exists
[ -d YARSPG_FILES_PATH ] && echo "$YARSPG_FILES_PATH does not exist, exiting" && exit 1

ARBITRARY_FILE="$YARSPG_FILES_PATH/dynamic/Person/part_0_0.yarspg"
if ! grep -q "^S" "$ARBITRARY_FILE" ; then
         echo 'Invalid file, make sure that the file have schemas. Existing.' ;
         exit 1;
fi

echo "Started merging files, it make take a while..."

# Add all except metadata
find "$YARSPG_FILES_PATH/" -type f -name '*.yarspg' -exec cat {} \; | grep -v "^[-:#]" > "$TEMP_FILE_PATH"

# Add node schemas with section
echo "%NODE SCHEMAS" >"$RAW_PATH/all-canonical.yarspg"
cat "$TEMP_FILE_PATH" | grep -E "^S\(ID-[0-9]+{" >>"$RAW_PATH/all-canonical.yarspg"

# Add node schemas with section
echo "%EDGE SCHEMAS" >>"$RAW_PATH/all-canonical.yarspg"
cat "$TEMP_FILE_PATH" | grep -E "^S\(ID-[0-9]+\)-" >>"$RAW_PATH/all-canonical.yarspg"

# Add nodes with section
echo "%NODES" >>"$RAW_PATH/all-canonical.yarspg"
cat "$TEMP_FILE_PATH" | grep -E "^\(ID-[0-9]+{" >>"$RAW_PATH/all-canonical.yarspg"

# Add edges with section
echo "%EDGES" >>"$RAW_PATH/all-canonical.yarspg"
cat "$TEMP_FILE_PATH" | grep -E "^\(ID-[0-9]+\)-" >>"$RAW_PATH/all-canonical.yarspg"

rm "$TEMP_FILE_PATH"

echo "File has been successfully merged."