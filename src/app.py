from datetime import datetime

from urllib.parse import unquote, urlsplit, parse_qs
from user_agents import parse
import re
import pandas as pd

# Load country codes with Alpha-2 code
country_codes = pd.read_csv("https://gist.githubusercontent.com/metal3d/5b925077e66194551df949de64e910f6/raw/c5f20a037409d96958553e2eb6b8251265c6fd63/country-coord.csv")
country_codes.set_index("Country", inplace=True)

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
    # Convert timestamp to separate date and time
    # Handle invalid dates with `errors='coerce'`
    data["datetime"] = pd.to_datetime(data["timestamp"], errors='coerce')
    data["date"] = data["datetime"].dt.date
    data["time"] = data["datetime"].dt.time
    
    # Rename alb_status_code to response_code
    data = data.rename(columns={"alb_status_code": "response_code"})
    
    # Process url
    data = data.apply(process_url, axis=1)
    
    # Get user agent info
    data = data.apply(process_user_agent, axis=1)
    
    
    # Drop any row with missing value
    dropped_data = data[data.isna().any(axis=1)]
    data = data.dropna()
    
    # Drop rows with path != /prod/vast 
    # Notes: Used `/prod/vast` instead of `/prod/getvastxml`
    #        Sample logs only contain `/prod/vast`
    dropped_path = data[~data["request_path"].isin(["/prod/vast"])]
    data = data[data["request_path"].isin(["/prod/vast"])]
    dropped_data = pd.concat([dropped_data, dropped_path], ignore_index=True)
    
    # Drop rows with country not in alpha-2 code format
    dropped_country = data[~data["country"].isin(country_codes["Alpha-2 code"])] # dropped_data
    dropped_data = pd.concat([dropped_data, dropped_country], ignore_index=True)
    data = data[data["country"].isin(country_codes["Alpha-2 code"])]
    
    # Log dropped data
    dropped_data.to_csv("dropped_data.csv")

def load(target_file, data):
    pass
def process_url(row):
    url = urlsplit(row["request_url"])
    row["request_path"] = url.path
    # Parse query
    query = parse_qs(url.query)
    row["pageurl"] = unquote(query["pageurl"][0]) # Decode pageurl
    row["action"] = query["action"][0]
    row["country"] = query["country"][0].upper()
    return row

def process_user_agent(row):
    ua = parse(row["user_agent"])
    row["device"], row["os"], row["browser"] = str(ua).split(" / ")
    return row

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
