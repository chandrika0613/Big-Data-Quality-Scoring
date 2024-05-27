#!/usr/bin/env python3

import sys

# Initialize counters
latest_count = 0
old_count = 0

# Iterate through lines from the mapper
for line in sys.stdin:
    # Split each line into date and identifier
    date, identifier = line.strip().split()
    
    # Update the counters based on the identifier
    if identifier == "L":
        latest_count += 1
    elif identifier == "O":
        old_count += 1

# Print the final counts
print(f"Latest\t{latest_count}")
print(f"Old\t{old_count}")
