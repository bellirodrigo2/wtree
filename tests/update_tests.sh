#!/bin/bash

# Update all test files to new API

for file in test_wtree3*.c; do
    echo "Updating $file..."
    
    # 1. Change WTREE3_EXTRACTOR to WTREE3_VERSION  
    sed -i 's/WTREE3_EXTRACTOR(\([0-9]\+\), *\([0-9]\+\))/WTREE3_VERSION(\1, \2)/g' "$file"
    
    # 2. Remove .key_extractor_id lines from configs
    sed -i '/\.key_extractor_id/d' "$file"
    
    # 3. Add version parameter to wtree3_db_open calls
    # Find lines like: wtree3_db_open(path, size, max_dbs, flags, &error)
    # Change to: wtree3_db_open(path, size, max_dbs, WTREE3_VERSION(1, 0), flags, &error)
    sed -i 's/wtree3_db_open(\([^,]*\), *\([^,]*\), *\([^,]*\), *\([^,)]*\), *&error)/wtree3_db_open(\1, \2, \3, WTREE3_VERSION(1, 0), \4, \&error)/g' "$file"
    
    # 4. Update register_key_extractor calls to include version and flags
    # This needs manual inspection - just mark them
    grep -n "wtree3_db_register_key_extractor" "$file" || true
done

echo "Done! Please review register_key_extractor calls manually."
