from datetime import datetime

import re
import pandas as pd

def extract(file_path):
    fields = [
        "type",
        "timestamp",
        "alb",
        "client_ip",
        "client_port",
        "backend_ip",
        "backend_port",
        "request_processing_time",
        "backend_processing_time",
        "response_processing_time",
        "alb_status_code",
        "backend_status_code",
        "received_bytes",
        "sent_bytes",
        "request_verb",
        "request_url",
        "request_proto",
        "user_agent",
        "ssl_cipher",
        "ssl_protocol",
        "target_group_arn",
        "trace_id",
        "domain_name",
        "chosen_cert_arn",
        "matched_rule_priority",
        "request_creation_time",
        "actions_executed",
        "redirect_url",
        "new_field",
    ]
    
    # REFERENCE: https://docs.aws.amazon.com/athena/latest/ug/application-load-balancer-logs.html#create-alb-table
    regex = r"([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) (.*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-_]+) ([A-Za-z0-9.-]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^\"]*)\" ([-.0-9]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^ ]*)\" \"([^\s]+?)\" \"([^\s]+)\" \"([^ ]*)\" \"([^ ]*)\""

    # Create pandas dataframe
    data = pd.DataFrame(columns=fields)
    
    # Match fields with corresponding values
    with open(file_path, 'r') as file:
        for line in file:
            matches = re.search(regex, line)
            if matches:
                values = {}
                for i, field in enumerate(fields):
                    values[field] = matches.group(i+1)
                values_df = pd.DataFrame([values])
                data = pd.concat([data, values_df], ignore_index=True)
    return data[["timestamp", "client_ip", "alb_status_code", "request_url", "user_agent"]]

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
