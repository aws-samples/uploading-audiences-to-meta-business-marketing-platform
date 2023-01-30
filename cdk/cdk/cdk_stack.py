from email import policy
from multiprocessing import set_forkserver_preload
from aws_cdk import (
    CfnOutput,
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    aws_s3_deployment as s3_deploy,
    Duration,
    aws_glue as glue,
    aws_kms as kms,
    aws_secretsmanager as secretsmanager,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_ssm as ssm,
    aws_lambda_destinations as destinations,
    Aspects,
    CfnTag as tag
)
from constructs import Construct
import json
from os import path
import cdk_nag

class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "CdkQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )

        # sets variables
        # takes the existing kms key alias as input and builds the kms object
        # user expected to create the key in advance manually to keep the code secure
        self.stack_tag = tag(key="project", value="octank-collab")
        self.kms_key_alias = self.node.try_get_context("kms_key_alias")
        self.cdk_asset_bucket_name = self.node.try_get_context("cdk_asset_bucket_name")
        self.glue_source_bucket_exist_flag = self.node.try_get_context("glue_source_bucket_exist_flag")
        self.glue_source_bucket_name = self.node.try_get_context("glue_source_bucket_name")
        self.glue_target_bucket_name = self.node.try_get_context("glue_target_bucket_name")
        self.glue_source_table_name = self.node.try_get_context("glue_source_table_name")
        # target table name is also used in the eventbridge rule
        self.glue_target_table_name = self.node.try_get_context("glue_target_table_name")
        self.glue_catalog_target_db_name = self.node.try_get_context("glue_catalog_target_db_name")
        self.glue_catalog_target_table_name = self.node.try_get_context("glue_catalog_target_table_name")
        self.lambda_script = self.node.try_get_context("lambda_script_dir")
        self.lambda_script_name = self.node.try_get_context("lambda_script_name")
        self.glue_job_script = self.node.try_get_context("glue_job_script")
        self.glue_job_name = self.node.try_get_context("glue_job_name")
        # Sets a customer managed key as best practise. Customer managed keys comes with higher costs compared to AWS managed.
        self.set_kms_key()
        self.role_name = "cleanroom_meta_upload_role"
        self.glue_script_bucket_key = "glue"
        self.lambda_script_bucket_key = "lambda"
        self.sample_data_bucket_key = "sample-data"

        self.config_prefix = "meta-conversions"
        self.parameter_prefix = "dev/cleanroom-uploads/meta"
        self.asset_dir = path.join(path.curdir, "../assets")
        # update run time as needed
        self.lambda_runtime = _lambda.Runtime.PYTHON_3_9
        print(f"Stack is in account:{self.account} and region:{self.region}")

        self.kms_secret_arn = f"arn:aws:kms:{self.region}:{self.account}:key/{self.parameter_prefix}/*"
        self.ssm_parameter_arn = f"arn:aws:ssm:{self.region}:{self.account}:parameter/{self.parameter_prefix}/*"
        
        # for associating resource to policy for kms
        self.glue_log_group_arn = f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws-glue/jobs"
        
        # default KMS key ARN
        self.aws_managed_kms_key_arn = f"arn:aws:kms:{self.region}:{self.account}:alias/aws/s3"
        # for s3 access logs
        self.s3_access_log_bucket_name=f"s3-access-log-{self.account}-{self.region}"
        self.add_s3_access_log_bucket()
        
        # deploys components of the stack
        self.add_s3_buckets()

        # Iam role restricts access to bucket hence bucket needs to be created first
        self.add_iam_role()

        # both secret manager and system manager parameter store can be used to store secrets
        # in this example secrets manager is used
        # A dummy secret will be created, update this manually with real api token
        self.add_secret()

        # # use this method and add logic if parameters needs to be managed centrally
        # # self.add_config()

        # deploy glue script
        self.deploy_s3_asset(
            "glue-script-deployment",
            path.abspath('../assets/glue'),
            self.cdk_asset_bucket,
            self.glue_script_bucket_key,
        )
        
        # deploy lambda script, layers
        self.deploy_s3_asset(
            "lambda-script-deployment",
            "../assets/lambda",
            self.cdk_asset_bucket,
            self.lambda_script_bucket_key,
        )

        # deploy sample data
        self.deploy_s3_asset(
            "sample-data-deployment",
            "../assets/data",
            self.cdk_asset_bucket,
            self.sample_data_bucket_key,
        )

        self.add_glue_jobs()
        
        # layers needs to be added first before adding function
        self.add_lambda_layers()
        self.add_lambda_function()

        # lambda function needs to be created before creating event setup
        self.add_event_framework()

        # update bucket policies as enforce_ssl=True in s3 bucket creates a bucket policy that denies access
        # role and bucket needs to exist first
        # TODO refactor code to avoid below complexity
        self.update_bucket_policy(self.cdk_asset_bucket)
        self.update_bucket_policy(self.glue_source_bucket)
        self.update_bucket_policy(self.glue_target_bucket)
        self.update_bucket_policy(self.glue_asset_bucket)

        # run cdk nag
        Aspects.of(self).add(cdk_nag.AwsSolutionsChecks())
    
    def set_kms_key(self) -> None:
        """
        Retrieves key from the alias
        """
        # self.kms_key = kms.Alias.from_alias_name(self, "myKMSKey", f"alias/{self.kms_key_alias}")
        self.kms_key = kms.Key.from_lookup(self, "MyKeyLookup",
            alias_name=f"alias/{self.kms_key_alias}"
            )
        CfnOutput(self, "KMS arn used", value=self.kms_key.key_arn)        
    
    def update_bucket_policy(self, bucket: s3.Bucket) -> None:
        """
        Update bucket policy to overcome the effects of ssl enable in buckets
        """
        bucket_policy_statement = iam.PolicyStatement(
            sid="metaUploadsBucketPolicy",
            actions=[
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject",
                "s3:GetBucketLocation",
                "s3:ListMultipartUploadParts"
                ],
            resources=[
                bucket.bucket_arn,
                f"{bucket.bucket_arn}/*"
                ],
            principals=[
                iam.ArnPrincipal(self.role.role_arn)
                ],
            effect=iam.Effect.ALLOW
        )
        bucket.add_to_resource_policy(bucket_policy_statement)
    
    def add_iam_role(self) -> None:
        """
        Creates IAM role needed for the stack services and components to use
        """
        self.s3_policy_statement = iam.PolicyStatement(
            sid="metaUploadsS3Access",
            actions=[
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:DeleteObject",
                "s3:GetBucketLocation",
                "s3:ListMultipartUploadParts",
                ],
            resources=[
                self.cdk_asset_bucket.bucket_arn, 
                self.glue_asset_bucket.bucket_arn, 
                self.glue_source_bucket.bucket_arn, 
                self.glue_target_bucket.bucket_arn,
                f"{self.glue_target_bucket.bucket_arn}/*"
                ],
        )

        self.kms_policy_statement = iam.PolicyStatement(
            sid="metaUploadsKMSAccess",
            actions=[
                "kms:Describe*",
                "kms:Get*",
                "kms:List*",
                "kms:Decrypt",
                "kms:Encrypt",
                "kms:GenerateDataKey"
            ],
            resources=[self.kms_key.key_arn]
        )

        self.ssm_policy_statement = iam.PolicyStatement(
            sid="metaUploadsSSMAccess",
            actions=[
                "ssm:GetParameter*"
            ],
            resources=[self.ssm_parameter_arn]
        )

        self.loggroup_policy_statement = iam.PolicyStatement(
            sid="metaUploadsLogGroupAccess",
            actions=[
                "logs:AssociateKmsKey"
            ],
            resources=[
                f"{self.glue_log_group_arn}/error:*",
                f"{self.glue_log_group_arn}/logs-v2:*",
                f"{self.glue_log_group_arn}/output:*"
                ],
        )

        inline_policy_doc = iam.PolicyDocument(statements=[self.s3_policy_statement, self.kms_policy_statement, self.ssm_policy_statement, self.loggroup_policy_statement])
        
        self.role = iam.Role(
            self,
            self.role_name,
            assumed_by=iam.CompositePrincipal(iam.ServicePrincipal(f"logs.{self.region}.amazonaws.com"),
                iam.ServicePrincipal("lambda.amazonaws.com"),
                iam.ServicePrincipal("glue.amazonaws.com")
            ),
            inline_policies={"customPolicies": inline_policy_doc},

        )
        # self.role.grant_assume_role(iam.ServicePrincipal("lambda.amazonaws.com"))
        # self.role.grant_assume_role(iam.ServicePrincipal(f"logs.{self.region}.amazonaws.com"))
        # self.role.add_to_policy(s3_policy_statement)
        # self.role.add_to_policy(kms_policy_statement)
        # self.role.add_to_policy(ssm_policy_statement)

        # add managed roles
        self.role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))
        self.role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))
        CfnOutput(self, "stack_role_name", value=self.role_name)

    def add_secret(self) -> None:
        """
        Creates a AWS Secrets manager entry. User needs to update secret manually in console after depolyment
        Path here assumes that conversions api is being used.
        """
        self.meta_conversion_api_secret = secretsmanager.Secret(self, 
            f"{self.config_prefix}-api-token",
            secret_name=f"{self.parameter_prefix}/conversions/access_token",
            description="API access token for conversions api",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"access_token": ""}),
                generate_string_key="access_token"
            ),
            encryption_key=self.kms_key
        )
        CfnOutput(self, "secret_manager_entry", value=self.meta_conversion_api_secret.secret_name)
    
    def add_config(self) -> None:
        """
        Creates System manager parameter store entry for storing application configurations.
        If secrets needs to be created, User needs to update secret manually in console after depolyment
        """
        pass

    def get_s3_bucket(self, bucket_name:str) -> s3.Bucket:
        """
        Helper method to get the standard s3 bucket construct
        Needs the access log s3 and kms key already defined
        Implements cdk-nag rules
        """
        s3_bucket = s3.Bucket(
            self,
            bucket_name,
            bucket_name=f"{bucket_name}-{self.account}-{self.region}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            server_access_logs_bucket=self.s3_access_log_bucket,
            server_access_logs_prefix=bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            # FIXED Customer managed key deployment fails, woraround is to use AWS managed Key
            # need to input the iKey construct in iam. iAlias wont work
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            bucket_key_enabled=True,
            # encryption=s3.BucketEncryption.KMS_MANAGED,
            enforce_ssl=True,
            event_bridge_enabled=True,
        )
        return s3_bucket

    def add_s3_access_log_bucket(self) -> None:
        """
        Creates a bucket to store access logs of other buckets
        This implements cdk-nag rule AwsSolutions-S1
        """
        self.s3_access_log_bucket = s3.Bucket(
            self,
            self.s3_access_log_bucket_name,
            bucket_name=self.s3_access_log_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            enforce_ssl=True,
        )

    def add_s3_buckets(self) -> None:
        """
        Creates S3 buckets needed for the pipeline
        """
        self.cdk_asset_bucket = self.get_s3_bucket(self.cdk_asset_bucket_name)
        # create source bucket only if user denotes that it doesnt exist
        # if it exists lookup the bucket for the cfnoutput
        # cleanroom output bucket should exist and that should be the source of glue job
        if self.glue_source_bucket_exist_flag.lower() != "y":
            self.glue_source_bucket = self.get_s3_bucket(self.glue_source_bucket_name)
        else:
            self.glue_source_bucket = s3.Bucket.from_bucket_name(self, self.glue_source_bucket_name, self.glue_source_bucket_name)

        self.glue_target_bucket = self.get_s3_bucket(self.glue_target_bucket_name)
        # if the glue asset bucket exist, refer that instead of creating. 
        # Create will fail if bucket exist
        # self.glue_asset_bucket = self.get_s3_bucket(f"aws-glue-assets-logs-{self.account}-{self.region}")
        self.glue_asset_bucket = s3.Bucket.from_bucket_name(self, f"aws-glue-assets-logs-{self.account}-{self.region}", f"aws-glue-assets-logs-{self.account}-{self.region}")
        CfnOutput(self, "glue_source_bucket_name", value=self.glue_source_bucket.bucket_name)
        CfnOutput(self, "glue_target_bucket_name", value=self.glue_target_bucket.bucket_name)
        CfnOutput(self, "glue_asset_bucket_name", value=self.glue_asset_bucket.bucket_name)       

    def deploy_s3_asset(self, id:str, asset_path: str, bucket: s3.Bucket, bucket_key: str) -> None:
        """
        Generic S3 deploy method
        """
        s3_deploy.BucketDeployment(
            self,
            id,
            sources=[s3_deploy.Source.asset(asset_path)],
            destination_bucket=bucket,
            destination_key_prefix=bucket_key,
            # prune=False,
            # server_side_encryption=s3_deploy.ServerSideEncryption.AWS_KMS,
            # server_side_encryption_aws_kms_key_id=self.kms_key.key_id,
        )

    def add_glue_jobs(self) -> None:
        """
        Creates sample glue job with data normalization code
        """
        # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
        arguments = {
            "--class":	"GlueApp",
            "--job-language":	"python",
            "--job-bookmark-option":	"job-bookmark-enable",
            "--TempDir":	f"s3://{self.glue_asset_bucket.bucket_name}/temporary/",
            "--enable-metrics":	"true",
            "--enable-continuous-cloudwatch-log":	"true",
            "--enable-spark-ui":	"true",
            "--enable-auto-scaling":	"true",
            "--spark-event-logs-path":	f"s3://{self.glue_asset_bucket.bucket_name}/sparkHistoryLogs/",
            "--enable-glue-datacatalog":	"true",
            "--enable-job-insights":	"true",
            "--sourcebucket":	self.glue_source_bucket.bucket_name,
            "--targetbucket": self.glue_target_bucket.bucket_name,
            "--sourcetable": self.glue_source_table_name,
            "--targettable": self.glue_target_table_name,
            "--targetcatalogdb": self.glue_catalog_target_db_name,
            "--targetcatalogtable": self.glue_catalog_target_table_name
        }
        # add security configuration to meet cdk-nag bar
        glue_sec_config = glue.CfnSecurityConfiguration(
            self,
            "GlueJobSecurityConfiguration",
            encryption_configuration=glue.CfnSecurityConfiguration.EncryptionConfigurationProperty(
                cloud_watch_encryption=glue.CfnSecurityConfiguration.CloudWatchEncryptionProperty(
                    cloud_watch_encryption_mode="SSE-KMS",
                    kms_key_arn=self.kms_key.key_arn
                ),
                job_bookmarks_encryption=glue.CfnSecurityConfiguration.JobBookmarksEncryptionProperty(
                    job_bookmarks_encryption_mode="CSE-KMS",
                    kms_key_arn=self.kms_key.key_arn
                ),
                s3_encryptions=[
                    glue.CfnSecurityConfiguration.S3EncryptionProperty(
                        kms_key_arn=self.kms_key.key_arn,
                        s3_encryption_mode="SSE-KMS"
                    )   
                ]
            ),
            name=f"{self.glue_job_name}SecConfig",
        )

        glue_job = glue.CfnJob(
            self,
            self.glue_job_name,
            name=self.glue_job_name,
            role=self.role.role_arn,
            allocated_capacity=10,
            command=glue.CfnJob.JobCommandProperty(
                name=f"glueetl",
                python_version="3",
                script_location=f"s3://{self.cdk_asset_bucket.bucket_name}/glue/{self.glue_job_script}",
            ),
            # security_configuration=f"{self.glue_job_name}SecConfig",
            glue_version="3.0",
            default_arguments=arguments,
        )

        CfnOutput(self, "Glue_Job", value=glue_job.name)
    
    def get_deny_non_ssl_policy(self, queue_arn):
        """
        Helper method that returns a IAM policy statement that denies non SSL calls to SQS queue
        To be used in SSL queue creation
        """
        return iam.PolicyStatement(
            sid='Enforce TLS for all principals',
            effect=iam.Effect.DENY,
            principals=[
                iam.AnyPrincipal(),
            ],
            actions=[
                'sqs:*',
            ],
            resources=[queue_arn],
            conditions={
                'Bool': {'aws:SecureTransport': 'false'},
            },
        )
    
    def add_lambda_layers(self) -> None:
        """
        Creates facebook and aws wrangler lambda layers
        All custom layer like the facebook one should be created in sync with 
        other libraries in the main lambda function. In other words, along with 
        the virtual environment setup, the zip file needs to be re-created
        Update compatible runtimes as needed
        """
        # FIXED layer classes are not getting detected when deployed through CDK, workaround is to manually create layers in console
        # Needed to zip the content under python folder
        self.fb_lambda_layer = _lambda.LayerVersion(self, "FacebookBusiness14.0.0-Python39",
            removal_policy=RemovalPolicy.RETAIN,
            code=_lambda.Code.from_asset(path.join(self.asset_dir, "lambda/layer.zip")),
            # As part of s3 deployment, these assets are already uploaded to s3
            # TODO referencing s3 instead of local asset
            # code=_lambda.Code.from_bucket(self.cdk_asset_bucket, "lambda/layer.zip",),
            compatible_architectures=[_lambda.Architecture.X86_64, _lambda.Architecture.ARM_64],
            compatible_runtimes=[self.lambda_runtime]
        )

        self.wrangler_layer =_lambda.LayerVersion.from_layer_version_attributes(self, "AWSSDKPandas-Python39",
           layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSDataWrangler-Python39:1")
    
    def add_lambda_function(self) -> None:
        """
        Creates the sample lambda function with facebook sdk and aws wrangler layers
        """
        # create DLQ
        dead_letter_queue = sqs.Queue(
            self, 
            "metaConversionsLambdaDLQ",
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=self.kms_key,
            )
        # Deny non SSL traffic
        dead_letter_queue.add_to_resource_policy(self.get_deny_non_ssl_policy(dead_letter_queue.queue_arn))

        # create lambda
        self.meta_converstions_lambda = _lambda.Function(
            self, 
            "metaConversionsPublish",
            function_name="metaConversionsPublish",
            runtime=self.lambda_runtime,
            handler=f"{self.lambda_script_name}.lambda_handler",
            # code=_lambda.Code.from_bucket(bucket=self.cdk_asset_bucket, key=f"{self.lambda_script_bucket_key}/{self.lambda_script}"),
            code=_lambda.Code.from_asset(path.join(self.asset_dir, f"{self.lambda_script_bucket_key}/{self.lambda_script}/")),
            layers=[self.fb_lambda_layer, self.wrangler_layer],
            on_failure=destinations.SqsDestination(dead_letter_queue),
            max_event_age=Duration.hours(2),  # Optional: set the maxEventAge retry policy
            retry_attempts=2,
            timeout=Duration.minutes(15),
            role=self.role
        )
        CfnOutput(self, "Lambda_Function", value=self.meta_converstions_lambda.function_arn)

    def add_event_framework(self) -> None:
        """
        Creates AWS eventbridge components to route and archive events and DLQ
        Change event pattern json as needed
        """
        event_pattern_detail = {
                            "bucket": {
                                "name": [f"{self.glue_target_bucket.bucket_name}"]
                                },
                            "object": {
                                "key": [{
                                    "prefix": self.glue_target_table_name
                                }],
                                "key": [{
                                    "suffix": ".csv"
                                }]
                            }
                        }
        rule = events.Rule(self, "meta_upload_s3_object_create",
                    event_pattern=events.EventPattern(
                        source=["aws.s3"],
                        detail_type=["Object Created"],
                        detail=event_pattern_detail
                    )
                )
        
        rule.add_target(targets.LambdaFunction(self.meta_converstions_lambda))
        CfnOutput(self, "Event_Bridge_Rule", value=rule.rule_arn)
