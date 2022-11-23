#!/usr/bin/env python3
import aws_cdk as cdk
import os
from cdk.cdk_stack import CdkStack
from cdk_nag import AwsSolutionsChecks, NagSuppressions

# set the environment here
dev_env = cdk.Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"])
app = cdk.App()
mystack = CdkStack(app, "dev-meta-activation-stack",
        description="(SO9074) Meta Business Marketing Platform Activation AWS CDK stack: The CDK stack deploys resources on customer AWS account that enables them to activate audiences on Meta Business Marketing Platform.",
        env=dev_env
        )
cdk.Tags.of(mystack).add("project", "cleanroom-meta-activation")

# adding cdk-nag suppressions
NagSuppressions.add_stack_suppressions(
    mystack,
    [
        {
            "id": "AwsSolutions-IAM5",
            "reason": "AWS managed policies are allowed which sometimes uses * in the resources like - AWSGlueServiceRole has aws-glue-* . AWS Managed IAM policies have been allowed to maintain secured access with the ease of operational maintenance - however for more granular control the custom IAM policies can be used instead of AWS managed policies",
        },
        {
            "id": "AwsSolutions-IAM4",
            "reason": "AWS Managed IAM policies have been allowed to maintain secured access with the ease of operational maintenance - however for more granular control the custom IAM policies can be used instead of AWS managed policies",
        },
        {
            "id": "AwsSolutions-S1",
            "reason": "S3 Access Logs are enabled for all data buckets. This stack creates a access log bucket which doesnt have its own access log enabled.",
        },
        {
            "id": "AwsSolutions-SQS3",
            "reason": "SQS queue used in the CDC is a DLQ.",
        },
        {
            "id": "AwsSolutions-SMG4",
            "reason": "Rotation is disabled in the sample code. Customers are encouraged to rotate thirdparty(Meta) api tokens",
        },
        # {
        #     'id': 'AwsSolutions-KMS5',
        #     'reason': 'SQS KMS key properties are not accessible from cdk',
        # }
    ],
)


# Simple rule informational messaged
cdk.Aspects.of(app).add(AwsSolutionsChecks())

app.synth()
