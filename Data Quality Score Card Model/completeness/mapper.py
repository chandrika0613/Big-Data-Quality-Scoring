#!/usr/bin/env python3
import sys
import csv

# Process the remaining rows
list_header = []
with open('/Users/shalinidhamodharan/Downloads/header.txt', 'r') as files:
    for line in files:
        list_header.append(line.strip())

# Read CSV data from standard input
csvreader = csv.reader(sys.stdin)
header = next(csvreader)

# Iterate through each row in the CSV data
for row in csvreader:
    # Check each element in the row for empty strings
    for i in range(len(row)):
        if row[i] == "":
            # If an element is empty, print the corresponding header with count '1'
            print(list_header[i] + '\t' + '1')
