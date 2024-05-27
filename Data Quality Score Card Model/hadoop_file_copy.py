import subprocess
from concurrent.futures import ThreadPoolExecutor

# Define a list of HDFS directories to delete
hdfs_directories_to_delete = [
    "/user/shalinidhamodharan/Accuracy_Output",
    "/user/shalinidhamodharan/Timeliness_Output",
    "/user/shalinidhamodharan/Completeness_Output",
    "/user/shalinidhamodharan/Consistency_Output",
    "/user/shalinidhamodharan/Total_Count_Output",
    "/user/shalinidhamodharan/Correlation_Output"
]

# Function to check if an HDFS directory exists
def hdfs_directory_exists(directory):
    try:
        subprocess.check_output(["/Users/shalinidhamodharan/hadoop-3.2.3//bin/hdfs", "dfs", "-test", "-d", directory], stderr=subprocess.STDOUT, universal_newlines=True)
        return True
    except subprocess.CalledProcessError:
        return False

# Run the HDFS command to delete each directory, but only if it exists
for directory in hdfs_directories_to_delete:
    if hdfs_directory_exists(directory):
        delete_command = ["/Users/shalinidhamodharan/hadoop-3.2.3//bin/hdfs", "dfs", "-rm", "-r", directory]
        try:
            subprocess.check_output(delete_command, stderr=subprocess.STDOUT, universal_newlines=True)
            print(f"Deleted HDFS directory: {directory}")
        except subprocess.CalledProcessError as e:
            print(f"Error deleting HDFS directory {directory}:", e.returncode, e.output)
    else:
        print(f"HDFS directory does not exist: {directory}")

# List everything in the root folder of HDFS
list_command = ["/Users/shalinidhamodharan/hadoop-3.2.3//bin/hdfs", "dfs", "-ls", "/user/shalinidhamodharan"]
try:
    output = subprocess.check_output(list_command, stderr=subprocess.STDOUT, universal_newlines=True)
    print(output)
except subprocess.CalledProcessError as e:
    print("Error listing HDFS root directory:", e.returncode, e.output)

# Define a list of Hadoop streaming commands
commands = [
    "/Users/shalinidhamodharan/hadoop-3.2.3//bin/hadoop jar /Users/shalinidhamodharan/hadoop-3.2.3/share/hadoop/tools/lib/hadoop-streaming-3.2.3.jar -file  /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/completeness/mapper.py -mapper mapper.py -file /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/completeness/reducer.py -reducer reducer.py -input /user/shalinidhamodharan/COVID-19_Vaccinations_in_the_United_States_County.csv -output /user/shalinidhamodharan/Completeness_Output",
    "/Users/shalinidhamodharan/hadoop-3.2.3//bin/hadoop jar /Users/shalinidhamodharan/hadoop-3.2.3/share/hadoop/tools/lib/hadoop-streaming-3.2.3.jar -file  /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/Timeliness/mapper.py -mapper mapper.py -file /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/Timeliness/reducer.py -reducer reducer.py -input /user/shalinidhamodharan/COVID-19_Vaccinations_in_the_United_States_County.csv -output /user/shalinidhamodharan/Timeliness_Output",
    "/Users/shalinidhamodharan/hadoop-3.2.3//bin/hadoop jar /Users/shalinidhamodharan/hadoop-3.2.3/share/hadoop/tools/lib/hadoop-streaming-3.2.3.jar -file  /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/Accuracy/mapper.py -mapper mapper.py -file /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/Accuracy/reducer.py -reducer reducer.py -input /user/shalinidhamodharan/COVID-19_Vaccinations_in_the_United_States_County.csv -output /user/shalinidhamodharan/Accuracy_Output",
    "/Users/shalinidhamodharan/hadoop-3.2.3//bin/hadoop jar /Users/shalinidhamodharan/hadoop-3.2.3/share/hadoop/tools/lib/hadoop-streaming-3.2.3.jar -mapper /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/consistency/mapper1.py -input /user/shalinidhamodharan/COVID-19_Vaccinations_in_the_United_States_County.csv -mapper Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/consistency/mapper2.py -input /user/shalinidhamodharan/COVID-19_Vaccinations_in_the_United_States_Jurisdiction_20231024.csv -reducer /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/consistency/reducer.py -output /user/shalinidhamodharan/Consistency_Output",
    "/Users/shalinidhamodharan/hadoop-3.2.3//bin/hadoop jar /Users/shalinidhamodharan/hadoop-3.2.3/share/hadoop/tools/lib/hadoop-streaming-3.2.3.jar -file  /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/Total_Count/mapper.py -mapper mapper.py -file /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/Total_Count/reducer.py -reducer reducer.py -input /user/shalinidhamodharan/COVID-19_Vaccinations_in_the_United_States_County.csv -output /user/shalinidhamodharan/Total_Count_Output",
    "/Users/shalinidhamodharan/hadoop-3.2.3//bin/hadoop jar /Users/shalinidhamodharan/hadoop-3.2.3/share/hadoop/tools/lib/hadoop-streaming-3.2.3.jar -file  /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/correlation/mapper.py -mapper mapper.py -file /Users/shalinidhamodharan/Documents/UHCL_DS/Fall2023/Capstone/Code/correlation/reducer.py -reducer reducer.py -input /user/shalinidhamodharan/COVID-19_Vaccinations_in_the_United_States_County.csv -output /user/shalinidhamodharan/Correlation_Output"
]

# Function to run a Hadoop streaming command
def run_command(command):
    try:
        result = subprocess.check_output(command, shell=True, universal_newlines=True)
        return (command, result)
    except subprocess.CalledProcessError as e:
        return (command, f"Error: {e}")

# Run Hadoop streaming commands in parallel using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=len(commands)) as executor:
    futures = {executor.submit(run_command, cmd): cmd for cmd in commands}

    for future in futures:
        command = futures[future]
        result = future.result()
        print(f"Command '\n{command}\n' output:\n{result}")
