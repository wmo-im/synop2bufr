# Using synop2bufr on AWS Lambda 

## Overview

AWS Lambda is a service from Amazon that enables publishing code which is executed as on demand functions. 

This directory contains the Dockerfile and example lambda function code that will run the synop2bufr-transformation on files received in S3.

## lambda container

The Dockerfile in this directory will build the container image that can be used to run synop2bufr on Lambda.

# build and deploy
```bash
docker build -t synop2bufr-lambda .
```

Once built, you need to deploy to ECR. 
Depending on environment permissions, you may need to create a ECR repo with appropriate policies first.

```bash
aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin <aws-account-id>.dkr.ecr.us-east-1.amazonaws.com
docker tag synop2bufr-lambda:latest <ECR repo url>:latest
docker push <ECR repo url>:latest
```

In the AWS console you can then create an AWS Lambda function using the URI for this container image. Setup your lambda function to be triggered by the S3 bucket where your synop files are stored.

The example lambda-function will run the synop2bufr-transformation on the file stored in S3 and write the output to the 'wis2box-public' bucket.