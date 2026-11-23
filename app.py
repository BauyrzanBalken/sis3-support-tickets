import os
import json
import time
import uuid
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger()

REGION = os.getenv("AWS_REGION", "us-east-1")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "")
QUEUE_URL = os.getenv("QUEUE_URL", "")
TABLE_NAME = os.getenv("TABLE_NAME", "")

sns = boto3.client("sns", region_name=REGION)
sqs = boto3.client("sqs", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)


class Handler(BaseHTTPRequestHandler):
    def send_json(self, status, data):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            return self.send_json(200, {"status": "ok"})
        return self.send_json(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/tickets":
            return self.send_json(404, {"error": "Not found"})

        try:
            length = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(length))
            title = data.get("title", "").strip()

            if not title:
                return self.send_json(400, {"error": "title is required"})

            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": "TicketCreated",
                "title": title,
                "created_at": int(time.time())
            }

            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=json.dumps(event),
                MessageAttributes={
                    "event_type": {
                        "DataType": "String",
                        "StringValue": "TicketCreated"
                    }
                }
            )

            log.info("Published TicketCreated event_id=%s", event["event_id"])
            return self.send_json(202, event)

        except Exception:
            log.exception("Ticket publishing failed")
            return self.send_json(500, {"error": "Internal server error"})

    def log_message(self, fmt, *args):
        log.info(fmt, *args)


def run_api():
    port = int(os.getenv("PORT", "8080"))
    log.info("Starting API on port %s", port)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()


def run_worker():
    table = dynamodb.Table(TABLE_NAME)
    log.info("Worker started. Queue: %s", QUEUE_URL)

    while True:
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            VisibilityTimeout=60
        )

        for message in response.get("Messages", []):
            event_id = "unknown"
            event_id = "unknown"
            try:
                body = json.loads(message["Body"])

                # SNS wraps the original event in the Message field.
                event = json.loads(body["Message"]) if "Message" in body else body
                event_id = event["event_id"]

                # Conditional write makes processing idempotent.
                table.put_item(
                    Item={
                        "event_id": event_id,
                        "title": event["title"],
                        "event_type": event["event_type"],
                        "created_at": event["created_at"]
                    },
                    ConditionExpression="attribute_not_exists(event_id)"
                )

                log.info("processed event_id=%s", event_id)

            except ClientError as error:
                if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    log.info("duplicate_skipped event_id=%s", event_id)
                else:
                    log.exception("DynamoDB processing failed")
                    continue

            except Exception:
                log.exception("Message processing failed")
                continue

            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message["ReceiptHandle"]
            )


if __name__ == "__main__":
    mode = os.getenv("APP_MODE", "api")
    if mode == "worker":
        run_worker()
    else:
        run_api()
