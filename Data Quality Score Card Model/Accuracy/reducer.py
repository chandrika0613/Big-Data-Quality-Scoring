#!/usr/bin/env python3
import sys

# Initialize variables to keep track of the current word and its count
curr_word = None
curr_count = 0

# Iterate through each line in the standard input
for line in sys.stdin:
    # Split the line into word and count
    word, count = line.split('\t')
    count = int(count)
    
    # Check if the current word is the same as the previous one
    if word == curr_word:
        # Increment the count if the word is the same
        curr_count += count
    else:
        # Print the result for the previous word if it exists
        if curr_word:
            print('{0}\t{1}'.format(curr_word, curr_count))
        
        # Update current word and reset count for a new word
        curr_word = word
        curr_count = count

# Print the final result for the last word
if curr_word == word:
    print('{0}\t{1}'.format(curr_word, curr_count))
