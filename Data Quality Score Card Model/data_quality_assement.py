import subprocess
import os
import pprint
import warnings
import scipy.stats as stats
from scipy.stats import binom_test
from concurrent.futures import ThreadPoolExecutor

# Suppress warnings
warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning,
                        message="WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable")

# Local directory where you want to save the copied files
local_directory = "/Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/Hadoop-Outputs/"

# Clear the local directory by removing all files
for file_name in os.listdir(local_directory):
    file_path = os.path.join(local_directory, file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)

# List of HDFS file paths you want to copy
hdfs_file_paths = [
    "/user/shalinidhamodharan/Consistency_Output/part-00000",
    "/user/shalinidhamodharan/Timeliness_Output/part-00000",
    "/user/shalinidhamodharan/Completeness_Output/part-00000",
    "/user/shalinidhamodharan/Accuracy_Output/part-00000",
    "/user/shalinidhamodharan/Correlation_Output/part-00000",
    "/user/shalinidhamodharan/Total_Count_Output/part-00000",
]

# Dictionaries to store data from Hadoop output files
dict_Accuracy_Output = {}
dict_Completeness_Output = {}
dict_Timeliness_Output = {}
dict_Correlation_Output = {}
dict_Consistency_Output = {}
dict_Total_Count_output = {}

# Function to copy files from HDFS to local
def copy_hdfs_files():
    for hdfs_file_path in hdfs_file_paths:
        hdfs_file_name = hdfs_file_path.split("/")[-2]
        local_file_path = os.path.join(local_directory, hdfs_file_name)
        copy_command = f"/Users/shalinidhamodharan/hadoop-3.2.3//bin/hdfs dfs -get {hdfs_file_path} {local_file_path}"

        try:
            subprocess.check_output(copy_command, shell=True)
        except subprocess.CalledProcessError as e:
            print(f"Error copying the file: {e}")
            continue

        try:
            with open(local_file_path, "r") as file:
                file_name = local_file_path.split("/")[-1]

                if file_name == "Consistency_Output":
                    for line in file:
                        parts = line.strip().split("\t")
                        if parts:
                            key = parts[0]
                            value = parts[1]
                            dict_Consistency_Output[key] = value

                # Similar blocks for other file types...

        except FileNotFoundError:
            print(f"File not found: {local_file_path}")
        except Exception as e:
            print(f"An error occurred: {e}")

# Multithreaded execution of file copying
with ThreadPoolExecutor(max_workers=len(hdfs_file_paths)) as executor:
    futures = {executor.submit(copy_hdfs_files)}
    for future in futures:
        future.result()

# Timeliness score calculation
latest_value = int(dict_Timeliness_Output['Latest'])
old_value = int(dict_Timeliness_Output['Old'])
total = latest_value + old_value
percentage_latest = (latest_value / total) * 100
percentage_not_latest = (old_value / total) * 100
p_value = stats.binom_test(x=old_value, n=total, p=0.05, alternative='greater')
significance_threshold = 0.05

if p_value <= significance_threshold:
    Timeliness_Score = 100
else:
    Timeliness_Score = (percentage_latest - (1.96 * ((percentage_latest * (100 - percentage_latest)) / total) ** 0.5))

# Consistency score calculation
observed_inconsistent_count = int(dict_Consistency_Output['Inconsistent Count'])
total_records_count = int(dict_Consistency_Output['Total Records Count'])
expected_proportion = 0.05
p_value = stats.binom_test(observed_inconsistent_count, total_records_count, expected_proportion, alternative='two-sided')
significance_threshold = 0.05

if p_value <= significance_threshold:
    Consistency_Score = 100
else:
    Consistency_Score = (percentage_latest - (1.96 * ((percentage_latest * (100 - percentage_latest)) / total) ** 0.5))

# Accuracy score calculation
row_count = int(dict_Total_Count_output['Row_Count'])
column_accuracies = {}
desired_accuracy = 0.95
significance_level = 0.05

for column, missing_count in dict_Accuracy_Output.items():
    if column != 'Row_Count':
        missing_count = row_count - int(missing_count)
        accuracy = 1 - (missing_count / row_count)
        column_accuracies[column] = accuracy

overall_accuracy = sum(column_accuracies[column] for column in column_accuracies) / len(column_accuracies)

for column, accuracy in column_accuracies.items():
    p_value = binom_test(int(row_count * (1 - accuracy)), row_count, p=desired_accuracy, alternative='greater')

    if p_value <= significance_level:
        adjustment_factor = 0.9
        adjusted_accuracy = overall_accuracy * adjustment_factor
    else:
        adjusted_accuracy = overall_accuracy

Accuracy_Score = adjusted_accuracy * 100

# Completeness score calculation
column_completeness = {}
desired_completeness = 0.8
significance_level = 0.2

for column, missing_count in dict_Completeness_Output.items():
    if column != 'Row_Count':
        missing_count = int(missing_count)
        completeness = 1 - (missing_count / row_count)
        column_completeness[column] = completeness

overall_completeness = sum(column_completeness[column] for column in column_completeness) / len(column_completeness)

for column, completeness in column_completeness.items():
    p_value = binom_test(int(row_count * (1 - completeness)), row_count, p=desired_completeness, alternative='greater')

    if p_value <= significance_level:
        adjustment_factor = 0.9
        adjusted_completeness = overall_completeness * adjustment_factor
    else:
        adjusted_completeness = overall_completeness

Completeness_Score = adjusted_completeness * 100

# Correlation score calculation
Total_Columns = 80
values = set()

for pair, correlation_coefficient in dict_Correlation_Output.items():
    correlation_coefficient = float(correlation_coefficient)

    if abs(correlation_coefficient) > 0.9:
        trimmed_string = pair.strip('()')
        pair1, pair2 = trimmed_string.split(',')
        values.add(pair1)
        values.add(pair2)

Correlation_Score = (len(values) / Total_Columns) * 100

# Overall Data Quality Score calculation
weight_timeliness = 0.2
weight_consistency = 0.2
weight_accuracy = 0.2
weight_completeness = 0.2
weight_correlation = 0.2

Overall_Score = (
    weight_timeliness * Timeliness_Score +
    weight_consistency * Consistency_Score +
    weight_accuracy * Accuracy_Score +
    weight_completeness * Completeness_Score +
    weight_correlation * Correlation_Score
)

# Print the overall data quality score
print(f"The Overall Data Quality Score is: {Overall_Score:.2f}%")
