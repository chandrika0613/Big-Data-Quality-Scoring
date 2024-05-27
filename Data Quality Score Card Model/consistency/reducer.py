#!/usr/bin/env python3

import sys
from collections import defaultdict

# Store data from Mapper 1 and Mapper 2
mapper1_data = defaultdict(dict)
mapper2_data = defaultdict(dict)
inconsistent_count = 0
total_records = 0

# Iterate through each line in the standard input
for line in sys.stdin:
    # Parse the input line
    identifier, key, value, composite_key = line.split('\t')
    total_records += 1

    # Store data in respective dictionaries based on the identifier
    if identifier == "M1":
        mapper1_data[key][composite_key] = value
    elif identifier == "M2":
        mapper2_data[key][composite_key] = value

# Check for inconsistencies between Mapper 1 and Mapper 2 data
for key, mapper1_records in mapper1_data.items():
    for composite_key, value1 in mapper1_records.items():
        if key in mapper2_data:
            if composite_key in mapper2_data[key]:
                value2 = mapper2_data[key][composite_key]
                # Compare values for the same composite key
                if value1 != value2:
                    inconsistent_count += 1

# Print the results
print(f"Inconsistent Count\t{inconsistent_count}")
print(f"Total Records Count\t{total_records}")
