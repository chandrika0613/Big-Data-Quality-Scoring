#!/usr/bin/env python3
import sys
import math

# Initialize variables
current_word = None

current_x = 0.0
current_y = 0.0
current_xx = 0.0
current_yy = 0.0
current_xy = 0.0
current_n = 0.0

# Iterate through the input lines
for line in sys.stdin:
    # Parse the input line
    word, count_x, count_y = line.strip().split('\t')
    count_x = float(count_x)
    count_y = float(count_y)

    # Check if the word in the current line is the same as the previous word
    if word == current_word:
        current_x += count_x
        current_y += count_y
        current_xx += (count_x ** 2)
        current_yy += (count_y ** 2)
        current_xy += (count_x * count_y)
        current_n += 1
    else:
        # If it's a new word, calculate the Pearson correlation coefficient for the previous word
        if current_word:
            numerator = current_xy - (current_x * current_y) / current_n
            denominator1 = current_xx - (current_x ** 2) / current_n
            denominator2 = current_yy - (current_y ** 2) / current_n

            denominator = math.sqrt(denominator1 * denominator2)
            
            # Avoid division by zero
            if denominator != 0:
                coefficient = numerator / denominator
                print(f"{current_word}\t{round(coefficient, 5)}")

        # Update the current word and reset the count
        current_word = word
        current_x = count_x
        current_y = count_y
        current_xx = 0.0
        current_yy = 0.0
        current_xy = 0.0
        current_n = 0.0

# Print the final Pearson correlation coefficient for the last word
if current_word:
    numerator = current_xy - (current_x * current_y) / current_n
    denominator1 = current_xx - (current_x ** 2) / current_n
    denominator2 = current_yy - (current_y ** 2) / current_n

    denominator = math.sqrt(denominator1 * denominator2)
    
    # Avoid division by zero
    if denominator != 0:
        coefficient = numerator / denominator
        print(f"{current_word}\t{round(coefficient, 5)}")
