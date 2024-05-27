#!/usr/bin/env python3

import csv
import sys
from datetime import datetime

# Read header from header.txt
list_header = []
with open('/Users/shalinidhamodharan/Downloads/header.txt', 'r') as files:
    for line in files:
        list_header.append(line.strip())

dates = []
input_csv_file = sys.stdin
reader = csv.reader(input_csv_file)
header = next(reader)
Date_index = list_header.index("Date")

# Extract dates from the "Date" column
for row in reader:
    date_str = row[Date_index]
    date = datetime.strptime(date_str, "%m/%d/%Y")
    dates.append(date)

# Calculate date range and threshold for the top 20%
min_date = min(dates)
max_date = max(dates)
date_range = max_date - min_date
threshold = date_range * 0.2

# Classify each date as in the top 20% or not
for date in dates:
    if date >= max_date - threshold:
        print(f"{date.strftime('%m/%d/%Y')} L")  # Date in the top 20%
    else:
        print(f"{date.strftime('%m/%d/%Y')} O")  # Date not in the top 20%
