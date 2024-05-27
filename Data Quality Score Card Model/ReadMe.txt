# Data Quality Assessment Project

This project focuses on assessing the data quality of various metrics, including accuracy, completeness, consistency, timeliness, and correlation. The assessment is performed on data stored in the Hadoop Distributed File System (HDFS).

## Project Structure

The project consists of several Python scripts for data copying, assessment, and scoring. Below is an overview of the key files:

1. **`data_quality_assessment.py`**
    - Main script that orchestrates the data quality assessment.
    - Performs file copying from HDFS to the local directory.
    - Calculates scores for accuracy, completeness, consistency, timeliness, and correlation by performing a binomial test to determine the significance of inconsistencies
    - Outputs the overall data quality score.

2. **`hadoop_file_copy.py`**
    - Calls Hadoop commands to MapReduce processes for accuracy, completeness, consistency, timeliness, and correlation calculations.
    - Utilizes the `subprocess` module for Hadoop commands.
    - Utilizes multithreading for concurrent file copying.

3
## Getting Started

1. Ensure you have the necessary dependencies installed. You can refer them in the following file:
	 requirements.txt
    

2. Update the configuration parameters in `data_quality_assessment.py` if necessary, including HDFS paths and weights for scoring.

3. Run the main scripts in the folowing order:

	1. hadoop_file_copy.py
	2. data_quality_assessment.py


## Dependencies

- Python 3.7 or more
- NumPy
- SciPy
