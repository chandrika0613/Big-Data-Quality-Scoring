#!/usr/bin/env python3

import sys
import csv

# Read header from header.txt
list_header = []
with open('/Users/shalinidhamodharan/Downloads/header.txt', 'r') as files:
    for line in files:
        list_header.append(line.strip())

# Read CSV data from standard input
csvreader = csv.reader(sys.stdin)
header = next(csvreader)

# Iterate through each row in the CSV file
for row in csvreader:
    # Iterate through specific columns for generating pairs
    for i in range(6, 49):          if row[i] == "":
            continue
        else:
            try:
                value_i = float(row[i])
            except ValueError:
                continue
            
            # Iterate through subsequent columns for pairs
            for j in range(i + 1, 50):  
                if row[j] == "":
                    continue
                else:
                    try:
                        value_j = float(row[j])
                    except ValueError:
                        continue

                    # Output pairs in the format: (header_i, header_j)  value_i  value_j
                    pair_1 = row[i]
                    pair_2 = row[j]
                    print("("+list_header[i]+","+list_header[j]+")" + '\t' + pair_1 + '\t' + pair_2)
