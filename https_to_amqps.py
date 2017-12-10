"""
Lambda function to connect https to amqps
"""
import json
import os

import boto3
import pika

print('Loading function')

CREDENTIALS = None


class MissingEnvVarsException(Exception):
    pass


def get_credentials(bucket: str, path: str, encryption_context: str) -> dict:
    """
    Gets RabbitMq credentials from s3
    :param bucket:Configuration Bucket
    :param path: Path to encrypted configuration file
    :param encryption_context: Encryption context used during encryption
    :return: Credentials to access Rabbitmq

    """
    global CREDENTIALS
    if CREDENTIALS is None:
        client = boto3.client("s3")
        file = client.get_object(Bucket=bucket, Key=path)

        client = boto3.client("kms")

        response = client.decrypt(
            CiphertextBlob=file['Body'].read(),
            EncryptionContext=encryption_context)
        CREDENTIALS = json.loads(response["Plaintext"])

    return CREDENTIALS


def respond(err: dict, res: dict = None) -> dict:
    """
    Generates an API Gateway response with elements to for the api caller
    :param err: (object) With error information
    :param res: (dict) Containing the response to return to the user
    :return: (dict) Response in API Gateway format
    """
    return {
        'statusCode': '400' if err else '200',
        'body': json.dumps(err) if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


def direct_message_to_rabbitmq(credentials: dict, host: str, vhost: str, body: dict) -> dict:
    """
    Receives a message from API Gateway and forwards it to RabbitMQ
    :param credentials: (dict) containing the username and password to accesss rabbit
    :param host: host address to access RabbitMq
    :param vhost: rabbitmq vhost
    :param body: Message to forward to RabbitMq
    :return: Returns the message forwarded to RabbitMq
    """
    print("Forward to RabbitMq")
    uri = "amqps://{}:{}@{}/{}".format(
        credentials["username"],
        credentials["password"],
        host,
        vhost)
    connection = pika.connection.URLParameters(uri)
    connection = pika.BlockingConnection(connection)
    channel = connection.channel()
    properties = pika.spec.BasicProperties(priority=int(body["priority"]))
    channel.basic_publish(exchange=body["exchange"],
                          body=json.dumps(body),
                          properties=properties,
                          routing_key="")
    return body


def lambda_handler(event: dict, context: object) -> dict:
    """
    Receives a message from API Gateway and forwards it to RabbitMQ
    :param event: (dict) with the users request to the API Gateway
    :param context: (object)
    :return: (dict) Response in API Gateway format
    """

    print("Set env variables")
    host = os.getenv("ADDRESS")
    vhost = os.getenv("VHOST")
    app_name = os.getenv("APPNAME")
    encryption_context = os.getenv("ENCRYPTION_CONTEXT")
    config_bucket = os.getenv("CONFIG_BUCKET")
    config_path = os.getenv("CONFIG_PATH")

    if not host and not vhost and not app_name and not config_path and not config_bucket:
        raise MissingEnvVarsException

    print("Set up operation behaviour")
    operation = event["context"]["http-method"]
    operations = {
        'POST': direct_message_to_rabbitmq
    }

    credentials = get_credentials(config_bucket, config_path, encryption_context)

    print("Get payload")
    body = event['body-json']

    if operation in operations:
        return respond(
            None, operations[operation](
                credentials, host, vhost, body))

    return respond({"message": 'Unsupported method "{}"'.format(operation)})
