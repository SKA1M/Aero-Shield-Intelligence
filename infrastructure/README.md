# Infrastructure Deployment

AWS SAM template for deploying the Aero Shield pipeline.

## Prerequisites

- AWS CLI configured with credentials (`aws configure`)
- AWS SAM CLI installed (`brew install aws-sam-cli` on macOS, or see [docs](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html))
- Python 3.12+

## Deploy

From the project root:

```bash
# Build the Lambda deployment package
sam build --template-file infrastructure/template.yaml

# First-time deployment (interactive)
sam deploy --guided \
    --template-file infrastructure/template.yaml \
    --stack-name aero-shield-prod \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        EnvironmentName=prod \
        OpenAQApiKey=$OPENAQ_API_KEY \
        WeatherApiKey=$WEATHER_API_KEY \
        TomTomApiKey=$TOMTOM_API_KEY \
        NotificationEmail=your.email@example.com
```

## Subsequent deploys

```bash
sam deploy --template-file infrastructure/template.yaml
```

## Teardown

```bash
aws cloudformation delete-stack --stack-name aero-shield-prod
```

**Note**: Empty the S3 buckets before stack deletion, or they'll block the teardown.

## What gets provisioned

- Two S3 buckets (raw + curated zones) with encryption and lifecycle rules
- Lambda function with hourly EventBridge schedule (6 AM – 8 PM)
- Glue job (G.1X × 3 workers, Glue 4.0)
- IAM roles with least-privilege S3 access
- SNS topic for pipeline failure alerts
- CloudWatch alarm on Lambda errors

## Security notes

- API keys in production should be stored in AWS Secrets Manager or SSM Parameter Store, not passed as CloudFormation parameters. This template supports both approaches — swap `!Ref OpenAQApiKey` for `!Sub '{{resolve:secretsmanager:aero-shield/openaq:SecretString:api_key}}'` for the Secrets Manager pattern.
- The Lambda IAM role uses `S3WritePolicy` (SAM managed policy) scoped to the raw bucket only — no read access, no access to other buckets.
