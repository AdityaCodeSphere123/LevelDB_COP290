#!/bin/bash

# Define your directories
DIR_BASE="leveldb_base"
DIR_NEW="leveldb"
OUT_DIR="leveldb_diffs"

# Clean up any previous run and create the output directory
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "Comparing implementation files between '$DIR_BASE' and '$DIR_NEW'..."

# Find only implementation files (.h, .cc, .c, .cpp) in the base directory
find "$DIR_BASE" -type f \( -name "*.cc" -o -name "*.h" -o -name "*.c" -o -name "*.cpp" \) | while read -r BASE_FILE; do
    
    # Extract the relative path to find the matching file in the other directory
    REL_PATH="${BASE_FILE#$DIR_BASE/}"
    NEW_FILE="$DIR_NEW/$REL_PATH"

    # Check if the exact matching file exists in the modified directory
    if [ -f "$NEW_FILE" ]; then
        
        # Check if the files are actually different before running diff
        if ! cmp -s "$BASE_FILE" "$NEW_FILE"; then
            
            # Create the matching sub-directory structure in the output folder
            mkdir -p "$OUT_DIR/$(dirname "$REL_PATH")"
            
            # Generate the diff and save it with a .diff extension
            diff -u "$BASE_FILE" "$NEW_FILE" > "$OUT_DIR/$REL_PATH.diff"
            echo "Diff found and saved: $REL_PATH"
        fi
    fi
done

echo "Done! Check the '$OUT_DIR' folder for the results."