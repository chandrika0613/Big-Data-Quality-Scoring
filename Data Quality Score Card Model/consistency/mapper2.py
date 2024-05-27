#!/usr/bin/env python3

import csv
import sys

# Read header from consistency_header.txt
list_header = []
with open('/Users/shalinidhamodharan/Downloads/consistency_header.txt', 'r') as files:
    for line in files:
        list_header.append(line.strip())

# input_csv_file = "/user/shalinidhamodharan/COVID-19_Vaccinations_in_the_United_States_Jurisdiction_20231024.csv"
input_csv_file = sys.stdin
reader = csv.reader(input_csv_file)
header = next(reader)  # Read the header

# Identify the indices of key columns
Date_index = list_header.index("Date")
Location_index = list_header.index("Location  ")

# Iterate through each row in the CSV file
for row in reader:
    # Extract values from the row
    Date = row[Date_index]
    Location = row[2]  # Assuming Location is the third column (index 2)
    fields = row[1:2] + row[3:]

    # Create the composite key
    composite_key = f"{Date}_{Location}"

    # Emit key-value pairs with headers as keys and values plus composite key as values
    for header_field, value in zip(header, fields):
        identifier = "M2"
        separator = '\t'
        # Output format: M2\t{header_field}\t{value}\t{composite_key}
        print(f"{identifier}{separator}{header_field}{separator}{value}{separator}{composite_key}")
