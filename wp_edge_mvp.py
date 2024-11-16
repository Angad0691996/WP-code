#30/09/24 had proper multithreading and writes to plc
from opcua import Client, ua
from azure.iot.device import IoTHubDeviceClient, Message
import json
from datetime import datetime
import time
import threading
import signal
from threading import Lock
import queue 
import logging
#Error connecting to OPC UA servers or reading nodes: timed out

custom_c = None
stop_thread = threading.Event()
last_processed_message_ids = set()
MAX_MESSAGE_ID_HISTORY = 1000
# Global queue to hold incoming requests
request_queue = queue.Queue()
client_lock1 = Lock()
client_lock2 = Lock()

# Set up logging configuration
logging.basicConfig(
    filename='plc_logging.txt',  # Log file name
    level=logging.INFO,           # Log level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    datefmt='%Y-%m-%d %H:%M:%S'   # Date format
)

def get_current_date_time():
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")
    return current_date, current_time

opcua_urls = {
    "192.168.1.1": "PRT78_PLC1"
}

#CONNECTION_STRING = "HostName=Parking-T1.azure-devices.net;DeviceId=samsanplc;SharedAccessKey=x7vZVTMrFFgAUjKqa888ugIzciQwnHaT+AIoTAFdVW0="
CONNECTION_STRING = "HostName=Parking-T1.azure-devices.net;DeviceId=PLC1500;SharedAccessKey=XuFebbjw/qO/QBsDxBqn7bXCkNZIcmeUjAIoTOdvz5s="

with open('new_node_ids.txt', 'r') as cloud_nodes:
    plc_to_cloud_nodes = json.load(cloud_nodes)

def read_node_ids(file_path):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading node IDs from file: {e}")
        return {}
# Read node IDs from the file for parking_map
node_ids = read_node_ids("parking_maps.txt")

# Function to read non-zero values from specified nodes
def get_non_zero_values(client, node_ids):
    non_zero_values = []
    for key, node_id in node_ids.items():
        try:
            data_value = client.get_node(node_id).get_data_value()
            actual_value = data_value.Value
            
            # Check if the StatusCode is good
            if data_value.StatusCode.is_good():
                # Handle Variant values correctly
                if isinstance(actual_value, ua.Variant):
                    int_value = actual_value.Value
                    # Check for Int16 range
                    if isinstance(int_value, int) and -32768 <= int_value <= 32767:
                        if int_value > 0:
                            non_zero_values.append(int_value)
                else:
                    print(f"Unexpected type for {key}: {type(actual_value)}")
            else:
                print(f"Bad status for {key}: {data_value.StatusCode}")
        except Exception as e:
            print(f"Error reading {key}: {e}")
    return non_zero_values


def create_opcua_clients(timeout=60000):
    clients = {}
    for ip_address, name in opcua_urls.items():
        url = f"opc.tcp://{ip_address}:4840"
        try:
            client = Client(url)
            client.session_timeout = timeout
            clients[ip_address] = client
            
            # Attempt to connect to verify the client works
            client.connect()
            print(f"Successfully connected to {url}")
        except Exception as e:
            print(f"Failed to create or connect OPC UA client for {url}: {e}")
            # Optionally, you could choose to skip this client or handle it differently
            
    return clients

def connect_with_retries(client, retries=3, delay=1):
    for attempt in range(retries):
        try:
            client.connect()
            return True
        except Exception as e:
            print(f"Attempt {attempt + 1} to connect failed: {e}")
            time.sleep(delay)
    return False

def connect_to_opcua_server(max_retries=3):
    attempt = 0
    while attempt < max_retries:
        try:
            client = Client("opc.tcp://192.168.1.1:4840")
            client.connect()
            print("Successfully connected to OPC UA server")
            return client
        except Exception as e:
            print(f"Error connecting to OPC UA server: {e}")
            attempt += 1
            time.sleep(1)  # wait before retrying
    raise Exception("Failed to connect to OPC UA server after multiple attempts.")

# Usage
opcua_client = connect_to_opcua_server()


def read_request_ack(client): #server to plc
    try:
        ack_node_id = "ns=3;s=\"Mobile_App\".\"PLC_To_Server\".\"Request_Ack\""
        ack_node = client.get_node(ack_node_id)
        ack_value = ack_node.get_value()
        return ack_value
    except Exception as e:
        print(f"Error reading Request_Ack value: {e}")
        return None
    
def read_request_type(client):  # server to plc
    try:
        request_type_node_id = "ns=3;s=\"Mobile_App\".\"Server_To_PLC\".\"Request_Data\".\"Request_Type\""
        request_type_node = client.get_node(request_type_node_id)
        request_type_value = request_type_node.get_value()
        
        # Map the request type values
        if request_type_value == 1:
            return 3
        elif request_type_value == 3:
            return 5
        elif request_type_value == 4:
            return 6
        
        # If no mapping was applied, return the original value
        return request_type_value

    except Exception as e:
        print(f"Error reading Request_type value: {e}")
        return None
    

def log_request(data):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("writtenplc_requests.txt", "a") as log_file:
        log_entry = f"{timestamp} - Written Request: {data}\n"  # Add a newline at the end
        log_file.write(log_entry)
        print("written plc write entry into a text file")

def write_to_plc(data, c, plc_name):
    logging.info(f"Starting write operation for {plc_name}")
    print(f"Starting write operation for {plc_name}")
    plc_name = "PRT79_PLC1"
    
    try:
        if c is None:
            print(f"Error: Client is not initialized.")
            logging.error("Error: Client is not initialized.")
            return
        # Define node IDs and their types
        nodes = {
            "Token_No": ("ns=3;s=\"Mobile_App\".\"Server_To_PLC\".\"Request_Data\".\"Token_No\"", ua.VariantType.Int16),
            "Car_Type_Value": ("ns=3;s=\"Mobile_App\".\"Server_To_PLC\".\"Request_Data\".\"Car_Type\"", ua.VariantType.Byte),
            "Request_Type_Value": ("ns=3;s=\"Mobile_App\".\"Server_To_PLC\".\"Request_Data\".\"Request_Type\"", ua.VariantType.Byte),
            "Add_Request": ("ns=3;s=\"Mobile_App\".\"Server_To_PLC\".\"Add_Request\"", ua.VariantType.Boolean)
        }

        # Write provided data to PLC nodes
        for key, (node_id, variant_type) in nodes.items():
            if key in data and data[key] is not None:
                try:
                    node = c.get_node(node_id)
                    data_value = ua.DataValue(ua.Variant(data[key], variant_type))
                    node.set_value(data_value)
                    print(f"Written to {key}: {data[key]}")
                    #log_request({key: data[key]})

                except Exception as e:
                    print(f"Error writing to {key}: {e}")
        # Log the entire request after all writes
        log_request(data)
        # Introduce a short delay for the PLC to process
        #time.sleep(0.05)
        time.sleep(0.1)
        # Initial write of False to Add_Request
        add_request_node_id = nodes["Add_Request"][0]
        add_request_node = c.get_node(add_request_node_id)
        # Check Request_Ack and re-write Add_Request if necessary
        #time.sleep(0.5) #new delay for trial
        request_ack = read_request_ack(c)
        request_type_value = read_request_type(c)
        print(f"Request_Ack read from PLC: {request_ack}")
#        time.sleep(1)

        #add_request_node.set_value(ua.DataValue(ua.Variant(False, ua.VariantType.Boolean)))
        #print(f"Initial write to Add_Request: False")
        #request_ack = read_request_ack(c)


        # Proceed to read Token_No and send acknowledgment
        token_node_id = "ns=3;s=\"Mobile_App\".\"Server_To_PLC\".\"Request_Data\".\"Token_No\""
        token_node = c.get_node(token_node_id)
        token_no = token_node.get_value()
        print(f"Token_No read from PLC: {token_no}")
        #add_request_node.set_value(ua.DataValue(ua.Variant(False, ua.VariantType.Boolean)))
        if request_ack is not None and request_ack > 0:
            print("Request_Ack is greater than 0, writing False again to Add_Request")
            add_request_node.set_value(ua.DataValue(ua.Variant(False, ua.VariantType.Boolean)))
            print(f"Re-written to Add_Request: False")


        if request_ack is not None and request_ack > 0:
            current_date, current_time = get_current_date_time()
            data['System_Date'] = current_date
            data['System_Time'] = current_time
            ack_message = {
                "Message_Id": f"REQUEST_ACKNOWLEDGEMENT",
                "System_Date": current_date,
                "System_Time": current_time,
                "System_Code_No": plc_name,
                "System_Type": "0",
                "System_No": "0",
                "Token_No": token_no,
                "Request_Type_Value":request_type_value,
                "Ack_Status": request_ack, #original request_ack is valid but for the sake of backend mapping incremented bt 1 
            }
            
            send_to_azure_iot_hub(ack_message,c)
            #add_request_node.set_value(ua.DataValue(ua.Variant(False, ua.VariantType.Boolean)))
            time.sleep(0.05)
            #print(f"Sending acknowledgment message: {ack_message}")
        else:
            print("Request_Ack not ready ")
            #add_request_node.set_value(ua.DataValue(ua.Variant(False, ua.VariantType.Boolean)))

    except Exception as e:
        print(f"Error connecting to OPC UA server or writing to node: {e}")
    finally:
        #c.disconnect()

        print(f"Request-ACK from OPC UA server for {plc_name}")



def send_to_azure_iot_hub(json_output,client):
    try:
        if custom_c is None:
            return
        else:
            #client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
        

            if isinstance(json_output, dict) and 'body' in json_output:
                json_output = json_output['body']

            json_str = json.dumps(json_output)
            message_size = len(json_str)

            print(f"Sending message to Azure IoT Hub. Size: {message_size} bytes")
            message = Message(json_str)
            custom_c.send_message(message)
            print(f"Message sent to Azure IoT Hub: {json_output}")

    except Exception as e:
        print(f"Error sending message to Azure IoT Hub: {e}")
        # Optionally implement a reconnection strategy here.
    

def handle_extension_object(value):
    if isinstance(value, ua.ExtensionObject):
        return str(value)
    return value


def read_opcua_nodes(client, plc_name):
    data = {}
    current_date, current_time = get_current_date_time()
    data['System_Date'] = current_date
    data['System_Time'] = current_time
    data['PLC_Name'] = plc_name

    try:
        if connect_with_retries(client):
            queue_data_list = []

            for key, node_id in plc_to_cloud_nodes.items():
                try:
                    node = client.get_node(node_id)
                    value = node.get_value()
                    data[key] = handle_extension_object(value)
                except Exception as e:
                    print(f"Error reading node {key}: {e}")

            # Process queue data
            for i in range(107):  # Assuming a fixed number of queues
                token_no = data.get(f"Queue[{i}]_Token_No")
                Lift_no = data.get(f"Queue[{i}]_Lift_No")
                estimated_time = data.get(f"Queue[{i}]_Estimated_Time")
                #ETA = round(estimated_time / 60,2) if estimated_time and estimated_time > 0 else None 
                ETA = estimated_time

                if token_no == 9999:
                    continue  # Skip tokens with value 9999

                if token_no is not None and token_no != 0: #and Lift_no is not None and Lift_no != 0:
                    queue_data_list.append({
                        "Token_No": token_no,
                        "ETA": ETA,
                        "Request_Type": data.get(f"Queue[{i}]_Request_Type"),
                        "Request_Status": data.get(f"Queue[{i}]_Request_Status"),
                        "Lift_No":Lift_no if Lift_no > 0  else "NULL",
                        "Request_In_Progress": data.get(f"Queue[{i}]_Request_In_Progress")
                    })

            # Prepare formatted output
            formatted_dict1 = {
                "Message_Id": "PARKING_SITE_CONFIG",
                "System_Date": current_date,
                "System_Time": current_time,
                "System_Code_No": plc_name,
                "System_Type": data.get("System_Type"),
                "System_No": data.get("System_No"),
                "Max_Lift_No": data.get("Max_Lift_No"),
                "Max_Floor_No": data.get("Max_Floor_No"),
                "Max_Shuttle_No": data.get("Max_Shuttle_No"),
                "Total_Parking_Slots": data.get("Total_Parking_Slots"),
                "Slots_By_Type": [data.get(f"Type{i}_Slots") for i in range(1, 9)],
                "Total_Parked_Slots": data.get("Total_Parked_Slots"),
                "Parked_Slots_By_Type": [data.get(f"Type{i}_Parked_Slots") for i in range(1, 9)],
                "Total_Empty_Slots": data.get("Total_Empty_Slots"),
                "Empty_Slots_By_Type": [data.get(f"Type{i}_Empty_Slots") for i in range(1, 9)],
                "Total_Dead_Slots": data.get("Total_Dead_Slots"),
                "Dead_Slots_By_Type": [data.get(f"Type{i}_Dead_Slots") for i in range(1, 9)],
                "Total_Booked_Slots": data.get("Total_Booked_Slots"),
                "Booked_Slots_By_Type": [data.get(f"Type{i}_Booked_Slots") for i in range(1, 9)],
            }

            formatted_dict2 = {
                "Message_Id": "QUEUE_STATUS_UPDATES",
                "System_Date": current_date,
                "System_Time": current_time,
                "System_Code_No": plc_name,
                "System_Type": "0",
                "System_No": "0",
                "Queue_Data": queue_data_list,
            }

            # Create the parking map
            parking_map = create_parking_map(client, plc_name, current_date, current_time,queue_data_list)

            return formatted_dict1, formatted_dict2, parking_map

    except Exception as e:
        print(f"Error reading nodes for {plc_name}: {e}")
        time.sleep(1)  # Wait before retrying
        return read_opcua_nodes(client, plc_name)  # Retry logic
    finally:
        client.disconnect()  # Ensure client disconnects in all cases


def create_parking_map(client, plc_name, current_date, current_time, queue_data_list):
    try:
        node_ids = read_node_ids("parking_maps_testing.txt")
         
        # Separate the node IDs into Lift_Token_No, Token_No, and Shuttle_Token_No
        lift_token_ids = {k: v for k, v in node_ids.items() if k.startswith("Lift_Token_No")}
        token_ids = {k: v for k, v in node_ids.items() if k.startswith("Token_No")}
        shuttle_token_ids = {k: v for k, v in node_ids.items() if k.startswith("Shuttle_Token_No")}

        # Get non-zero values
        lift_token_values = get_non_zero_values(client, lift_token_ids)
        token_no_values = get_non_zero_values(client, token_ids)
        shuttle_token_values = get_non_zero_values(client, shuttle_token_ids)

        # Prepare the parking map
        parking_map = {
            "Message_Id": "PARKING_MAP",
            "System_Date": current_date,
            "System_Time": current_time,
            "System_Code_No": plc_name,
            "System_Type": "0",
            "System_No": "0",
            "Token_No": token_no_values,
            "Lift_Token_No": lift_token_values,
            "Shuttle_Token_Value": shuttle_token_values,
            "Queue_Data": queue_data_list  # Add the Queue_Data here
        }

        return parking_map

    except Exception as e:
        print(f"Error creating parking map: {e}")
        return {}
    
# Event to synchronize sending data
ack_event = threading.Event()

def send_data_continuously(interval,c):
    clients = create_opcua_clients()
    
    while not stop_thread.is_set():

        for ip_address, client in clients.items():
            plc_name = opcua_urls[ip_address]
            try:
                # Wait for the acknowledgment to be sent
                #ack_event.wait()  # This will block until the event is set
                data1, data2, parking_map = read_opcua_nodes(client, plc_name)
                # Check and send data1
                send_to_azure_iot_hub(data1,c)
                # Check and send data2
                send_to_azure_iot_hub(data2,c)
                # Check and send parking_map
                send_to_azure_iot_hub(parking_map,c)

            except Exception as e:
                print(f"Error processing data for PLC: {plc_name}: {e}")
                continue
        time.sleep(interval)


def is_client_connected(client):
    try:
        return client.is_connected()
    except Exception as e:
        print(f"Error checking connection status: {e}")
        return False

def reconnect_client(client):
    try:
        client.connect()
        print("Client reconnected successfully.")
    except Exception as e:
        print(f"Reconnect failed: {e}")

def process_request_type(request_type_value):
    """Modify the request_type_value based on specified conditions."""
    if request_type_value == 3:
        return 1
    elif request_type_value == 5:
        return 3
    elif request_type_value == 6:
        return 4
    return request_type_value

# Create a lock for thread-safe operations

def message_handler(message):
    try:
        raw_data = message.data.decode('utf-8').strip()
        message_size = len(raw_data)
        print(f"Raw message received: {raw_data}")
        print(f"Received message size: {message_size} bytes")

        if raw_data:
            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                return

            print(f"Message received from Azure IoT Hub: {data}")

            # Acquire the lock to ensure thread-safe operations
            with client_lock1:

                request_type_value = process_request_type(data.get("Request_Type_Value", 0))
                plc_name = "PRT79_PLC1"

                # Prepare data for writing
                filtered_data = {
                    "Message_Id": data.get("Message_Id"),
                    "Token_No": data.get("Token_No"),
                    "Car_Type_Value": data.get("Car_Type_Value"),
                    "Request_Type_Value": request_type_value,
                    "Add_Request": True,
                    "PLC_Name": plc_name  # Include plc_name in the data
                }

                print(f" 1 Queueing data to PLC: {filtered_data}")
                # Add request to the queue
                request_queue.put(filtered_data)
                time.sleep(0.5)


            # Print the queue size and contents
            print("2 Current queue size: ", request_queue.qsize())
            print("3 Current data appended to queue: ", filtered_data)

            # Optionally, you can print the entire queue for debugging
            current_queue_contents = list(request_queue.queue)
            print("4 Current Handler queue contents: ", current_queue_contents)

            #time.sleep(1)

            # Start processing the queue in a separate thread if not already started
            if not any(t.is_alive() for t in threading.enumerate() if t.name == "PLC_Writer"):
                threading.Thread(target=process_queue, name="PLC_Writer", daemon=True).start()
        else:
            print("Received empty message or message not in valid JSON format")
    except Exception as e:
        print(f"Error processing message: {e}")

def process_queue():
    while True:
       # Use the existing opcua_client instead of creating a new one
        c = opcua_client 
        # Print the current state of the queue
        current_queue_contents = list(request_queue.queue)
        print("5 Current process queue contents:", current_queue_contents)

        # Define node IDs and their types
        nodes_pre = {
            "Add_Request": ("ns=3;s=\"Mobile_App\".\"Server_To_PLC\".\"Add_Request\"", ua.VariantType.Boolean)
        }

        # Read add_request node
        add_request_node_id = nodes_pre["Add_Request"][0]
        add_request_node = c.get_node(add_request_node_id)
        add_request_val = add_request_node.get_value()

#        if add_request_val == 0:

            # Acquire the lock to ensure thread-safe operations
        with client_lock2:
                # Get the next request from the queue
                data = request_queue.get()
                print("6 data to write from queue " + str(data))
                if data is None:  # Exit signal
                    c.disconnect()
                    break
            
                plc_name = data["PLC_Name"]  # Retrieve plc_name from data

                # Get the current timestamp
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Save the data to a text file with a timestamp
                with open("request_data_in_queue.txt", "a") as f:
                    f.write(f"{timestamp} - {data}\n")  # Write the timestamp and data to the file


                     

                    # Check if the client is connected
                    if not is_client_connected(c):
                        print("Client is not connected, attempting to reconnect...")
                        reconnect_client(c)

                    # Write the request data to PLC
                    time.sleep(0.1)
                    write_to_plc(data, c, plc_name)
                    #time.sleep(0.05)
            
        # Introduce a delay of 100ms before processing the next message
        #time.sleep(0.1)
        request_queue.task_done()  # Mark the task as done


opcua_urls_inv = {v: k for k, v in opcua_urls.items()}

def on_message_received(message):
    return message_handler(message)

def listen_to_azure_iot_hub(c):
    try:
        
        c.on_message_received = on_message_received
        print("Listening for messages from Azure IoT Hub...")
        c.connect()
        while not stop_thread.is_set():
            time.sleep(1)
    except Exception as e:
        print(f"Error in IoT Hub listener: {e}")
    finally:
        c.shutdown()

def signal_handler(sig, frame):
    print('Signal received, stopping...')
    stop_thread.set()
    
if __name__ == "__main__":
    c = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    custom_c = c

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start the worker thread for processing PLC requests
    worker_thread = threading.Thread(target=process_queue)
    worker_thread.start()

    # Start the thread for sending data continuously
    send_data_thread = threading.Thread(target=send_data_continuously, args=(5, c))
    send_data_thread.start()

    # Start listening to Azure IoT Hub
    listen_to_azure_iot_hub(c)

    # Wait for threads to finish
    send_data_thread.join()
    # You might want to signal the worker to stop before joining, depending on your use case
    request_queue.put(None)  # clear the request_queue
    worker_thread.join()  # Wait for the worker thread to finish

    print("Program exited cleanly.")





