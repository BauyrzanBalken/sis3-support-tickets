# SIS-3: Support Tickets Application

## Architecture
- Python API running on ECS Fargate
- Application Load Balancer
- Amazon SNS for TicketCreated events
- Amazon SQS with Dead Letter Queue
- ECS Worker consuming messages
- DynamoDB for ticket storage
- Amazon ECR with immutable image tags
- CloudWatch logs with 7-day retention

## Idempotency
The worker uses a conditional DynamoDB PutItem operation with attribute_not_exists(event_id). Duplicate events are skipped.

## Deployment
Prerequisites: AWS CLI, Docker, and an image pushed to ECR.

Run:

    aws cloudformation deploy --template-file template.yaml --stack-name sis3-main --region us-east-1 --parameter-overrides Environment=dev ImageTag=v1 DesiredCount=1 --capabilities CAPABILITY_IAM

## Cleanup

    aws cloudformation delete-stack --stack-name sis3-main --region us-east-1
