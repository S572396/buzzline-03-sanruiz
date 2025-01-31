#####################################
# Import Modules
#####################################

import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_bootstrap_servers() -> str:
    """Fetch Kafka bootstrap servers from environment or use default."""
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Kafka bootstrap servers: {servers}")
    return servers

#####################################
# Message Processing
#####################################

def process_message(message: dict):
    """
    Process incoming Kafka message.
    
    Args:
        message (dict): Incoming Kafka message containing timestamp, temperature, and classification.
    """
    temp = message.get("temperature", None)
    classification = message.get("classification", "Unknown")
    timestamp = message.get("timestamp", "Unknown")
    
    if temp is None:
        logger.error("Received message without temperature data.")
        return
    
    # Determine additional messages based on temperature
    additional_message = ""
    if 70 <= temp <= 75:
        additional_message = "Its a nice temp."
    elif temp >= 89:
        additional_message = "Its getting warmer!"
    elif temp >100:
        additional_message = "It's getting hot!"
    
    # Log processed message
    logger.info(f"[{timestamp}] Temperature: {temp}°F - Classification: {classification} {additional_message}")

#####################################
# Kafka Consumer
#####################################

def main():
    """
    Main entry point for the Kafka consumer.
    
    - Connects to the Kafka topic.
    - Listens for incoming messages.
    - Processes each message.
    """
    topic = get_kafka_topic()
    bootstrap_servers = get_kafka_bootstrap_servers()
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    
    logger.info(f"Listening for messages on topic: {topic}")
    
    try:
        for message in consumer:
            process_message(message.value)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()
