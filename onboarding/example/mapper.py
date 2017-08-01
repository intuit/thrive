#!/usr/bin/python
import json
import sys
from datetime import datetime


ctrl_A = "\x01"
fmt = ctrl_A.join(["%s"] * 6)
hive_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
for line in sys.stdin:
    try:
        data = json.loads(line)
        event_id = data["event_header"]["event_id"]
        server_timestamp_ms = data["event_header"]["server_timestamp"]
        topic_name = data["event_header"]["topic_name"]
        message = data["message"]
        source_timestamp = data["source_timestamp"]
        server_timestamp = datetime.fromtimestamp(server_timestamp_ms/1000).strftime("%Y-%m-%d %H:%M:%S")
        print fmt %(topic_name,
                    message,
                    source_timestamp,
                    event_id,
                    server_timestamp,
                    hive_timestamp)
    except:
        sys.stderr.write("reporter:counter:THRIVE,SKIPPED,1\n")