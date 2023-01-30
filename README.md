# Uploading Audiences created in AWS to Meta Business Marketing Platform



## Getting started

This repo hosts AWS Python CDK stack for Meta Upolader data pipeline.

Use this application along with Guidance for Uploading Audiences created in AWS Clean Rooms to Meta Business Marketing Platform in [AWS Solutions Library](https://aws.amazon.com/solutions/advertising-marketing/)

## Name
Meta Business Marketing Platform Uploader AWS CDK stack (SO9074)

## Description
The CDK stack deploys resources on customer AWS account that enables them to upload audiences on AWS Meta Business Marketing Platform. Python AWS CDK is used in this project for deployment. 

Deployment uses below AWS services
1. Amazon Simple Storage Service(S3)
2. Amazon Identity and Access Management(IAM)
3. AWS Key Management Service(KMS)
4. AWS Glue
5. AWS EventBridge
6. AWS Lambda
7. AWS Secrets Manager
8. AWS Simple Queue Service(SQS)

In the example lambda code provided, data stored in S3 is picked up and sent to facebook conversions API.

## Badges
TBD

## Visuals
Refer Guidance for Uploading Audience Segments on Meta Business Marketing Platform in [AWS Solutions Library](https://aws.amazon.com/solutions/advertising-marketing/)

## Installation
### CDK
The project code uses the Python version of the AWS CDK ([Cloud Development Kit](https://aws.amazon.com/cdk/)). To execute the project code, please ensure that you have fulfilled the [AWS CDK Prerequisites for Python](https://docs.aws.amazon.com/cdk/latest/guide/work-with-cdk-python.html).

The project code requires that the AWS account is [bootstrapped](https://docs.aws.amazon.com/de_de/cdk/latest/guide/bootstrapping.html) in order to allow the deployment of the CDK stack.

#### Manual step - Export environment variables
```
export CDK_DEFAULT_ACCOUNT="<>"
export CDK_DEFAULT_REGION="us-east-1"
```
Either do this in the current shell or modify profile file and add these to the env file. These are needed for cdk to work
#### Manual step - Updated CDK context parameters

Update the cdk.context.json
1. The stack suffixes account and region to the bucket names make the S3 URI unique
2. The source bucket for glue job should typically exist and should be the output bucket of the cleanroom collaboration. Use the flag attribute accordingly
3. Cleanroom output folder name is the query id in the cleanroom collaboration. Obtain that from the cleanroom collaboration
4. Glue jobs assumes that output of cleanroom collaboration query is a csv.

```
{
  "kms_key_alias": "meta-uploads",
  "cdk_asset_bucket_name": "meta-act-cdk-asset-bucket",
  "glue_source_bucket_exist_flag": "Y",
  "glue_source_bucket_name": "cleanroom-query-output",
  "glue_target_bucket_name": "meta-act-tgt-bucket",
  "glue_source_table_name": "<query-id>",
  "glue_target_table_name": "meta_upload_audience",
  "glue_catalog_target_db_name": "meta-upload",
  "glue_catalog_target_table_name": "meta_upload_audience",
  "lambda_script_dir": "meta_conversions",
  "glue_job_script": "cleanroom-activation-meta-normalize-scriptonly.py",
  "glue_job_name": "meta-normalize-conversions-data",
  "lambda_script_name": "send_conversion_events",
  "acknowledged-issue-numbers": [
    21902
  ]
}

```

#### Helper script

A helper bash script is available that automates below steps
```
# navigate to project directory cdk directory
cd uploading-audiences-to-meta-business-marketing-platform/cdk
# execute bash script
./cdk_deploy.bash
```

#### CDK Deployment steps in the helper script explained
```
# navigate to project directory cdk directory
cd uploading-audiences-to-meta-business-marketing-platform/cdk

# install and activate a Python Virtual Environment
python3 -m venv ~/.virtualenvs/metauploads_cdk
source ~/.virtualenvs/metauploads_cdk/bin/activate

# install dependant libraries
python -m pip install -r requirements.txt

```

#### Bootstrap the account to setup CDK deployments in the region

```
cdk bootstrap 
```
Upon successful completion of `cdk bootstrap`, the project is ready to be deployed.

python dependencies in zip format
```
# create lambda layers
../assets/lambda/create_layers.bash
```
Lambda layer code files are copied in to s3 during stack deployment. Run below bash script to install 
```
cdk deploy 
```

#### Manual step - Update API token in AWS Secrets Manager entry created by the stack

1. On the Secrets Manager console, choose Store a new secret.
2. For Secret type, select “Other”
3. Enter your key as credentials and the value as the base64-encoded
string.
4. Leave the rest of the options at their default.
5. Choose Next.
6. Give a name to the secret following a URI format to make it easier to find
it among multiple secrets in the /dev/cleanroom-
uploads/meta/conversions/access_token.
7. Follow through the rest of the steps to store the secret.

#### Cleanup

When you’re finished experimenting with this solution, clean up your resources by running the command:

```
cdk destroy 
```

This command deletes resources deploying through the solution. S3 buckets containing the call recordings and CloudWatch log groups are retained after the stack is deleted.

### Manual implementation without CDK
* Clone repo

* Follow steps in the Guidance for Activating Audience Segments on Meta Business Marketing Platform in [AWS Solutions Library](https://aws.amazon.com/solutions/advertising-marketing/)
    * Copy relevant code from assets directory to respective AWS service console IDE's (Glue, Lambda)

## Local Setup to test lambda code alone
Refer below steps if you are not using CDK and would like to setup and test the lambda code locally

1. Setup AWS CLI
2. Setup virtual env
3. Install dependencies using requirements file
4. Use python CLI to test 
```
python3 assets/lambda/send_conversion_events.py
```

## No code alternative to glue data prep step
AWS Glue DataBrew service can be used as an alternative to the glue job that generates transformed data needed for Meta upload. Use the [sample Glue DataBrew recipe](/assets/databrew/octank-collab-meta-activation-prep-recipe.json)  available in the repo as a starting point to setup a AWS Glue DataBrew Job that generates output files which inturn triggers the lambda function for sending data to Meta Business API. 

Refer [AWS Glue DataBrew documentation](https://docs.aws.amazon.com/databrew/latest/dg/getting-started.html) on how to get started with this service

## A note on multiple requirements files
1. [All encompasing requirements](requirements-lambda+cdk+sec-frozen.txt) -> Contains all version locked requirements for the lambda, CDK and security scan modules. 
2. [Lambda specific requirements](requirements-lambda.txt) -> Non version locked dependencies for the lambda code alone. Use this for local lamdba code testing
3. [Lambda specific frozen requirements](requirements-lambda-frozen.txt) -> Version locked dependencies for the lambda code alone. Use this for local lamdba code testing. Use this to reproduce the dev env as is. This is preferred over unfrozen version.
4. [CDK requirements](cdk/requirements.txt) -> Dependencies for the CDK code to run on local machine
5. [CDK dev requirements](cdk/requirements-dev.txt) -> Dependencies to setup dev env for cdk code + security scan tools (cdk-nag, bandit, pip-audit)

## Contributing
[CONTRIBUTING](CONTRIBUTING.md)

***

## Authors and acknowledgment
* Ranjith Krishnamoorthy
* Brian Mcguire

## License
[LICENSE](LICENSE)
## Project status
Active

## Known Issues
1. Manual Creation of Customer managed key and secrets upfront are needed. References of these needs to be given as input in [cdk.context.json](cdk/cdk.context.json)
2. Glue security configurations are not working. Root cause unknown

##Fixed Issues
1. Customer managed KMS key encryption is either not deploying properly or after deployment is unable to use in services. These could be because of IAM permission issues. Workaround is to use AWS managed keys for respective services (S3, KMS, SSM etc)
2. Facebook_business layer is not getting resolved in lambda when deployed through CDK. Folder structure issues could be the root cause. Workaround is to manually create the layer and add to lambda
3. Glue job may fail on second run complaining about 'Col8' undefined. Root cause unknown, workaround is to change sql transform to native transform. A new job cloned from the first one succeedes
