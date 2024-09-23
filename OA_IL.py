import csv

def parse_lookup_table(lookup_file):
    lookup = {} 
    try:
        with open(lookup_file, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if 'tag' in row:
                    key = (row['dstport'], row['protocol'])
                    lookup[key] = row['tag']
                else:
                    print(f"Warning: 'tag' not found in row {row}")
    except FileNotFoundError:
        print(f"Error: The file {lookup_file} was not found.")
    except Exception as e:
        print(f"An error occurred while reading the lookup table: {e}")

    return lookup

def parse_flow_logs(flow_log_file, lookup):
    tag_count = {}
    port_protocol_count = {}
    untagged_key = "Untagged"
    
    with open(flow_log_file, 'r') as file:
        for line in file:
            fields = line.split()
            dstport = fields[6].strip().lower()
            protocol_num = int(fields[7])  # Ensure this conversion is correct
            protocol = 'tcp' if protocol_num == 6 else 'udp'  # Adjust as necessary
            lookup_key = (dstport.lower(), protocol.lower())
            tag = lookup.get(lookup_key, untagged_key)
            tag_count[tag] = tag_count.get(tag, 0) + 1
            
            port_protocol_key = (dstport, protocol)
            port_protocol_count[port_protocol_key] = port_protocol_count.get(port_protocol_key, 0) + 1
    
    return tag_count, port_protocol_count

def write_output(tag_count, port_protocol_count, output_file):
    try:
        with open(output_file, 'w') as file:
            file.write("Tag Counts:\nTag,Count\n")
            for tag, count in tag_count.items():
                file.write(f"{tag},{count}\n")
            
            file.write("\nPort/Protocol Combination Counts:\nPort,Protocol,Count\n")
            for (port, protocol), count in port_protocol_count.items():
                file.write(f"{port},{protocol},{count}\n")
    except Exception as e:
        print(f"An error occurred while writing to the output file: {e}")

if __name__ == "__main__":
    lookup_file = 'lookup_table.csv'
    flow_log_file = 'flow_logs.txt'
    output_file = 'output.csv'

    lookup = parse_lookup_table(lookup_file)
    tag_count, port_protocol_count = parse_flow_logs(flow_log_file, lookup)
    write_output(tag_count, port_protocol_count, output_file)
