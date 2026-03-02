"""
Kafka Producer — simulates real-time Adobe Analytics hit data.

Reads hit_data.tab row by row and publishes each hit as a JSON message
to the MSK Serverless topic 'skp-hits'.

How it connects to MSK Serverless:
  MSK Serverless uses IAM authentication (no username/password).
  The producer uses the aws-msk-iam-auth library which automatically
  signs each Kafka request with the local AWS credentials/role.

Usage:
  pip install kafka-python aws-msk-iam-sasl-signer-python
  python producer.py --broker <MSK_BOOTSTRAP_URL> --delay 1.0

What each argument does:
  --broker  : MSK Serverless bootstrap URL (from AWS console)
  --input   : path to the tab-delimited hit data file
  --topic   : Kafka topic name to publish to (default: skp-hits)
  --delay   : seconds to wait between messages (simulates real-time cadence)
  --repeat  : loop through the file N times (default: 1)
"""
import argparse
import csv
import json
import time
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

TOPIC = 'skp-hits'
AWS_REGION = 'us-east-1'


def get_msk_token(config):
    """
    Called by KafkaProducer every time it needs to authenticate.
    MSKAuthTokenProvider generates a short-lived signed token from
    the current IAM role — no hardcoded credentials needed.
    """
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
    return token, expiry_ms


def build_producer(broker_url: str) -> KafkaProducer:
    """
    Creates a KafkaProducer configured for MSK Serverless IAM auth.

    Key settings explained:
      bootstrap_servers   : the MSK Serverless endpoint (from AWS console)
      security_protocol   : SASL_SSL — encrypted + authenticated
      sasl_mechanism      : OAUTHBEARER — MSK IAM uses OAuth-style tokens
      sasl_oauth_token_provider : our get_msk_token function above
      value_serializer    : converts Python dict → JSON bytes for the wire
    """
    return KafkaProducer(
        bootstrap_servers=broker_url,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=get_msk_token,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',           # wait for all replicas to confirm — no data loss
        retries=3,
        request_timeout_ms=30000,
    )


def stream_hits(producer: KafkaProducer, input_file: str,
                topic: str, delay: float, repeat: int):
    """
    Reads the hit data file and sends each row to Kafka.

    Why we use ip as the message key:
      Kafka partitions messages by key. Using IP ensures all hits from
      the same visitor land on the same partition and are processed
      in order — important for first-touch attribution.
    """
    sent = 0
    errors = 0

    for run in range(repeat):
        logger.info("Pass %d/%d — reading %s", run + 1, repeat, input_file)

        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                # Clean up None values from empty tab columns
                message = {k: (v if v else None) for k, v in row.items()}
                key = message.get('ip')

                try:
                    future = producer.send(topic, key=key, value=message)
                    future.get(timeout=10)   # block until broker confirms
                    sent += 1
                    logger.info("[%d sent] ip=%s referrer=%s",
                                sent, key, message.get('referrer', '')[:50])
                except KafkaError as e:
                    errors += 1
                    logger.error("Failed to send message: %s", e)

                time.sleep(delay)

    producer.flush()
    logger.info("Done. Sent: %d | Errors: %d", sent, errors)


def main():
    parser = argparse.ArgumentParser(description='SKP Kafka Producer')
    parser.add_argument('--broker', required=True,
                        help='MSK Serverless bootstrap URL')
    parser.add_argument('--input', default='tests/data/hit_data.tab',
                        help='Path to tab-delimited hit data file')
    parser.add_argument('--topic', default=TOPIC,
                        help='Kafka topic to publish to')
    parser.add_argument('--delay', type=float, default=1.0,
                        help='Seconds between messages (simulates real-time)')
    parser.add_argument('--repeat', type=int, default=1,
                        help='Number of times to loop through the file')
    args = parser.parse_args()

    logger.info("Connecting to MSK broker: %s", args.broker)
    logger.info("Publishing to topic: %s | delay: %ss", args.topic, args.delay)

    producer = build_producer(args.broker)
    stream_hits(producer, args.input, args.topic, args.delay, args.repeat)


if __name__ == '__main__':
    main()
