#!/usr/bin/env python3

import csv
import sys

# Read header from header.txt
list_header = []
with open('/Users/shalinidhamodharan/Downloads/header.txt', 'r') as files:
    for line in files:
        list_header.append(line.strip())

# input_csv_file = "/user/shalinidhamodharan/COVID-19_Vaccinations_in_the_United_States_County.csv"
input_csv_file = sys.stdin
reader = csv.reader(input_csv_file)
header = next(reader)  # Read the header

# Identify the indices of key columns
Date_index = list_header.index("Date")
recip_State_index = list_header.index("Recip_State")

# Iterate through each row in the CSV file
for row in reader:
    # Extract values from the row
    Date = row[Date_index]
    Recip_State = row[recip_State_index]

    # Extract fields excluding Date and Recip_State
    fields = row[Date_index:recip_State_index] + row[recip_State_index+1:]

    # Create the composite key
    composite_key = f"{Date}_{Recip_State}"

    # Emit key-value pairs with headers as keys and values plus composite key as values
    for header_field, value in zip(header, fields):
        identifier = "M1"
        separator = '\t'
        # Output format: M1\t{header_field}\t{value}\t{composite_key}
        print(f"{identifier}{separator}{header_field}{separator}{value}{separator}{composite_key}")
