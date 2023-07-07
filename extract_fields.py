# extract_fields.py
from datetime import datetime
def extract_fields(nsg_log, file_path):
    extracted_logs = []
    for record in nsg_log['records']:
        flows = record['properties']['flows']
        for flow in flows:
            rule = flow['rule']
            flow_entries = flow['flows']
            for entry in flow_entries:
                mac = entry['mac']
                flow_tuples = entry['flowTuples']
                for flow_tuple in flow_tuples:
                    fields = flow_tuple.split(',')
                    timestamp = datetime.utcfromtimestamp(int(fields[0])).isoformat()
                    source_ip = fields[1]
                    destination_ip = fields[2]
                    source_port = int(fields[3])
                    destination_port = int(fields[4])
                    protocol = 'TCP' if fields[5] == 'T' else 'UDP'
                    traffic_flow = 'Inbound' if fields[6] == 'I' else 'Outbound'
                    traffic_decision = 'Allowed' if fields[7] == 'A' else 'Denied'

                    extracted_log = {
                        'timestamp': timestamp,
                        'source_ip': source_ip,
                        'destination_ip': destination_ip,
                        'source_port': source_port,
                        'destination_port': destination_port,
                        'protocol': protocol,
                        'traffic_flow': traffic_flow,
                        'traffic_decision': traffic_decision,
                        'rule': rule,
                        'file_path': file_path
                    }
                    extracted_logs.append(extracted_log)

    return extracted_logs
