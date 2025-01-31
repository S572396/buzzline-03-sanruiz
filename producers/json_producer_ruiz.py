import os
import sys
import time
import pathlib
import json
from dotenv import load_dotenv
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("buzz.json")

def generate_messages(file_path: pathlib.Path):
    while True:
        try:
            with open(DATA_FILE, "r") as json_file:
                json_data: list = json.load(json_file)
                if not isinstance(json_data, list):
                    raise ValueError(f"Expected a list of JSON objects, got {type(json_data)}.")
                for buzz_entry in json_data:
                    yield buzz_entry
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in file: {file_path}. Error: {e}")
            sys.exit(2)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)

def main():
    logger.info("START producer.")
    verify_services()
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    producer = create_kafka_producer(value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting message production to topic '{topic}'...")
    message_count = 0
    start_time = time.time()
    try:
        for message_dict in generate_messages(DATA_FILE):
            if message_count >= 250:  # Stop after sending 250 messages
                logger.info("Message limit reached. Stopping producer.")
                break
            
            msg_start_time = time.time()
            producer.send(topic, value=message_dict)
            msg_end_time = time.time()
            message_count += 1

            latency = msg_end_time - msg_start_time
            logger.info(f"Sent message {message_count} to topic '{topic}' in {latency:.4f} sec: {message_dict}")

            if message_count % 10 == 0:
                elapsed_time = msg_end_time - start_time
                throughput = message_count / elapsed_time
                logger.info(f"Processed {message_count} messages in {elapsed_time:.2f} sec ({throughput:.2f} msg/sec)")
            
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")
    
    logger.info("END producer.")

if __name__ == "__main__":
    main()


