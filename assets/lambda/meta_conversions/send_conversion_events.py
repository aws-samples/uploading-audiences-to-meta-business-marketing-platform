"""
Meta Facebook Marketing API integration Lambda code. Uses an AWS Eventbridge event as a source
Reads configuration from AWS System Manager parameter store that includes api token
Optional code to pull from AWS Secrets manager
Uses Facebook Business Manager SDK to build and send payload to Conversions api
Can be used as template to send data to other apis as well
Author: Ranjith Krishnamoorthy
"""
from array import array
from decimal import InvalidOperation
import time
import datetime as dt
from facebook_business.adobjects.serverside.action_source import ActionSource
from facebook_business.adobjects.serverside.content import Content
from facebook_business.adobjects.serverside.custom_data import CustomData
from facebook_business.adobjects.serverside.delivery_category import DeliveryCategory
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.api import FacebookAdsApi
from facebook_business.api import FacebookRequest as request
import base64
from botocore.exceptions import ClientError
import traceback, json, configparser, boto3
import awswrangler as wr
from pandas import DataFrame

# Initialize boto3 client at global scope for connection reuse
client = boto3.client('ssm')

# location for AWS System Manager Parameter Store parameter entry
env = 'dev'
app_config_path = 'cleanroom-activations/meta'
full_config_path = f'/{env}/{app_config_path}/'

# Initialize app at global scope for reuse across invocations
app = None

class MetaAWSAMTConnector:
    """
    Meta connector with S3 and EventBridge integration
    """
    def __init__(self, config):
        """
        Construct new MetaAWSAMTConnector with configuration
        :param config: application configuration
        """
        self.config = config
        self.source_file_uri = None
        self.df_terator = None

    @staticmethod
    def get_secret_from_secret_manager(name, region) -> json:
        """
        Returns the AWS secret manager stored secret value json based on the key name and region parameters
        """

        secret_name = name
        region_name = region

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                return secret
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                return decoded_binary_secret
   
    @staticmethod
    def get_content() -> Content:
        """
        Builds fb sdk content object
        """
        content = Content(
            product_id='product123',
            quantity=1,
            delivery_category=DeliveryCategory.HOME_DELIVERY,
        )
        return content

    @staticmethod
    def get_custom_data(content:Content) -> CustomData:
        """
        Builds fb sdk custom data object
        """
        custom_data = CustomData(
            contents=[content],
            currency='usd',
            value=123.45,
        )
        return custom_data
    
    @staticmethod
    def get_events_data(user_data: UserData, custom_data: CustomData, event_id: int) -> Event:
        """
        Builds fb sdk event object
        """
        event = Event(
            event_name='Purchase',
            event_time=int(time.time()),
            user_data=user_data,
            custom_data=custom_data,
            event_source_url='http://jaspers-market.com/product/123',
            action_source=ActionSource.WEBSITE,
            event_id= event_id
        )
        return event

    @staticmethod
    def get_event_request(events: array, pixel_id: str) -> EventRequest:
        """
        Builds event request
        """
        event_request = EventRequest(
            events=events,
            pixel_id=pixel_id,
            test_event_code='TEST72284'
        )
        return event_request

    @staticmethod
    def get_needed_cols_df_chunk(df_chunk):
        """
        returns a needed attributes from the full file with data frame as input
        """
        # use below for joined ss_store_sales data
        # needed_cols_df_chunk = df_chunk.iloc[:,[1,8,9,11,12,13,16]]
        # use below for raw customer data
        needed_cols_df_chunk = df_chunk
        return needed_cols_df_chunk
                   
    @staticmethod
    def format_dob_digits(inpvalue: float, type: str) -> str:
        """
        Formats input float value in to two charecter or four charecter string with prefix 0 
        for sending date parts to meta api
        """
        if type == 'd':
            output = dt.datetime.strptime(f'{int(inpvalue)}', '%d').strftime('%d')
        elif type == 'm':
            output = dt.datetime.strptime(f'{int(inpvalue)}', '%m').strftime('%m')
        elif type == 'y':
            output = dt.datetime.strptime(f'{int(inpvalue)}', '%Y').strftime('%Y')
        else:
            raise InvalidOperation
        return output

    def get_config(self):
        """
        Returns entire config object
        """
        return self.config
    
    def get_config_value(self, section, key):
        """
        Returns value of a config key for given section and key from the configuration object
        """
        return self.config[section][key]
    
    def set_s3_source_file_uri(self, event) -> str:
        """
        Reads EventBridge event payload, extracts the s3 object URI and set it to object variable
        """
        bucket = event['detail']['bucket']['name']
        folder_path = event['detail']['object']['key']
        self.source_file_uri = f's3://{bucket}/{folder_path}'
        print(f"Reading {self.source_file_uri}")

    def get_user_data(self, row_tuple) -> UserData:
        """
        Builds fb sdk user data object
        """
        # remove formatting of DOB values if input values are already formatted
        # print(row_tuple)
        user_data = UserData(
            external_id=row_tuple[1],
            first_name=str(row_tuple[2]),
            last_name=str(row_tuple[3]),
            dobd=self.format_dob_digits(row_tuple[4], 'd'),
            dobm=self.format_dob_digits(row_tuple[5], 'm'),
            doby=self.format_dob_digits(row_tuple[6], 'y'),
            email=row_tuple[7],
            # phones=['12345678901', '14251234567'],
            # It is recommended to send Client IP and User Agent for Conversions API Events.
            client_ip_address= '1.1.1.1',
            client_user_agent= 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Mobile/15E148 Safari/604.1',
            fbc='fb.1.1554763741205.AbCdEfGhIjKlMnOpQrStUvWxYz1234567890',
            fbp='fb.1.1558571054389.1098115397',
        )

        return user_data
    
    def set_df_iterator(self, limit_rows: int=None, chunksize: int=100, delimeter: str=',', encoding: str='utf8') -> iter:
        """
        Reads data from s3 file object in chunks and saves in the iterator class object
        """
        self.df_terator = wr.s3.read_csv(path=self.source_file_uri, chunksize=chunksize, sep=delimeter, 
            na_values=['null', 'none'], encoding=encoding, nrows=limit_rows)
        print("created dataframe iterator")
    
    def send_conversion_data(self, chunk_id: int, df_chunk: DataFrame):
        """
        Sends sample payload to meta facebook marketing conversions api
        """
        if (df_chunk.empty):
            print("***************")
            print("Empty dataframe detected. Exiting")
            print("***************")
            exit(2)
        # gets connection configuration
        access_token = self.get_config_value('conversions', 'access_token')
        pixel_id = self.get_config_value('conversions', 'pixel_id')
        
        # intiates connection
        FacebookAdsApi.init(access_token=access_token)

        events = []
        # print(df_chunk.head(2))
        # creates one chunk of events to be send in one request
        print ("Adding chunk of data to one request")
        for row in df_chunk.itertuples():
            user_data = self.get_user_data(row)
            # print(user_data)
            content = self.get_content()
            custom_data = self.get_custom_data(content)
            #generate dummy event id
            event_id = time.monotonic_ns() + chunk_id
            events.append(self.get_events_data(user_data, custom_data, event_id))
        
        event_request = self.get_event_request(events, pixel_id)
        print ("Sending chunk of data to Meta Conversions API")
        event_response = event_request.execute()
        response_dict = event_response.to_dict()
        print(json.dumps(response_dict, indent=4))
        return response_dict

    def iterate_conversion_data_chunks(self) -> dict:
        """
        iterate through chunks of df iterator object, extracts required cols to be sent
        """
        event_response_dict = {"responses":[]}
        for i, df_chunk in enumerate(self.df_terator):
            # optional if input file has more columns than that is needed in the request to api
            print(f"processing chunk {i}")
            needed_cols_df_chunk = self.get_needed_cols_df_chunk(df_chunk)
            event_response_dict['responses'].append(self.send_conversion_data(i,needed_cols_df_chunk))
        return event_response_dict

def load_config(ssm_parameter_path):
    """
    Load configparser from config stored in SSM Parameter Store
    :param ssm_parameter_path: Path to app config in SSM Parameter Store
    :return: ConfigParser holding loaded config
    """
    #FIXED unable to use customer KMS key. Permission issues.Workaround use AWS/SSM key.
    #Need to give Ikey instead of IAlias object in IAM permissions
    configuration = configparser.ConfigParser()
    try:
        # Get all parameters for this app
        param_details = client.get_parameters_by_path(
            Path=ssm_parameter_path,
            Recursive=False,
            WithDecryption=True
        )
        # Loop through the returned parameters and populate the ConfigParser
        if 'Parameters' in param_details and len(param_details.get('Parameters')) > 0:
            for param in param_details.get('Parameters'):
                param_path_array = param.get('Name').split("/")
                section_position = len(param_path_array) - 1
                section_name = param_path_array[section_position]
                config_values = json.loads(param.get('Value'))
                config_dict = {section_name: config_values}
                # print(config_dict)
                configuration.read_dict(config_dict)

    except:
        print("Encountered an error loading config from SSM.")
        traceback.print_exc()
    finally:
        return configuration

def get_sample_event():
    """
    returns sample payload for testing purposes
    """
    payload = {
        "version": "0",
        "id": "2d4eba74-fd51-3966-4bfa-b013c9da8ff1",
        "detail-type": "Object Created",
        "source": "aws.s3",
        "account": "123456789012",
        "time": "2021-11-13T00:00:59Z",
        "region": "us-west-2",
        "resources": [
            "<>"
        ],
        "detail": {
            "version": "0",
            "bucket": {
            "name": "<>"
            },
            "object": {
            "key": "<>",
            "size": 99797,
            "etag": "7a72374e1238761aca7778318b363232",
            "version-id": "a7diKodKIlW3mHIvhGvVphz5N_ZcL3RG",
            "sequencer": "00618F003B7286F496"
            },
            "request-id": "4Z2S00BKW2P1AQK8",
            "requester": "348414629041",
            "source-ip-address": "72.21.198.68",
            "reason": "PutObject"
        }
    }
    return payload

def lambda_handler(event, context):

    print("Loading config and creating new MyApp...")
    config = load_config(full_config_path)
    app = MetaAWSAMTConnector(config)
    print("getting event and identifying object name that got uploaded")
    app.set_s3_source_file_uri(event)
    print("read s3 object data and set the chunk iterator object")
    # use below for limited testing
    # app.set_df_iterator(limit_rows=50, chunksize=5, delimeter=',', encoding='iso8859-1')
    # use below for production
    app.set_df_iterator(chunksize=1000)
    print("Itrate each chunks")
    response = app.iterate_conversion_data_chunks()
    return response

# if __name__ == "__main__":
#     response = lambda_handler(get_sample_event(), None)
    