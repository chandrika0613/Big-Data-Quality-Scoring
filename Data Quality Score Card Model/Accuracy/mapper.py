#!/usr/bin/env python3

import csv
import sys

# File paths for input text files
input_txt_file = '/Users/shalinidhamodharan/Downloads/Statecode.txt'
input_txt_file_1 = '/Users/shalinidhamodharan/Downloads/County_names.txt'
input_txt_file_2 = '/Users/shalinidhamodharan/Downloads/Percentage.txt'

# Lists to store values from input text files
values_list = []
Countycodes = []
Percentagecolumn = []
indeces = []
list_header = []  # List to store header from a separate header.txt file

# Reading header from header.txt file
with open('/Users/shalinidhamodharan/Downloads/header.txt', 'r') as files:
    for line in files:
        list_header.append(line.strip())

# Reading values from Statecode.txt file
with open(input_txt_file, 'r') as file:
    for line in file:
        cleaned_line = line.strip()
        values_list.append(cleaned_line)

# Reading county names from County_names.txt file
with open(input_txt_file_1, 'r') as file_1:
    for line in file_1:
        County = line.strip()
        Countycodes.append(County)

# Reading percentage values from Percentage.txt file
with open(input_txt_file_2, 'r') as file_2:
    for line in file_2:
        Percentage = line.strip()
        Percentagecolumn.append(Percentage)

# Identifying indices of columns with percentage values in the header
for x in list_header:
    if x in Percentagecolumn:
        k = list_header.index(x)
        indeces.append(k)

# Indices for specific columns
recip_State_index = list_header.index("Recip_State")
Recip_County_index = list_header.index("Recip_County")

# Reading CSV data from standard input
reader = csv.reader(sys.stdin)
header = next(reader)

# Processing each row of the CSV data
for row in reader:
    Recip_State = row[recip_State_index]
    County = row[Recip_County_index]

    # Checking if Recip_State is in the Statecode.txt values_list
    if Recip_State in values_list:
        print("Recip_State" + '\t' + '1')

    # Checking if County is in the County_names.txt Countycodes
    if County in Countycodes:
        print("County" + '\t' + '1')

    # Checking percentage values in specific columns identified by indeces
    for i in indeces:
        if row[i] != "":
            try:
                value = float(row[i])
                if 0 <= value <= 100:
                    print(list_header[i] + '\t' + '1')
            except ValueError:
                continue
        else:
            print(list_header[i] + '\t' + '1')
