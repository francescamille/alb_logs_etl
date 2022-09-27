from datetime import datetime

def extract(file_path):
    pass

def transform(data):
    pass

def load(target_file, data):
    pass

# Add simple logging
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

if __name__ == "__main__":
    source_file = "logs.txt"
    target_file = "processed_data.parquet"
    
    log("Started ETL process...")
    log("Extract phase started...")
    data = extract(source_file)
    log("Extract phase ended.")
    log("Transform phase started...")
    transformed_data = transform(data)
    log("Transform phase ended.")
    log("Load phase started...")
    load(target_file, transformed_data)
    log("Load phase ended.")
    log("Completed ETL process.")
