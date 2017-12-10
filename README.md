### Description

Https to AMQPS is a AWS lambda function which forwards https messages from the API Gateway to a RabbitMQ Server.

### Dependencies

* Python 3.6
* Boto3
* pika

### Workflow

1. Receive the https request
2. Get RabbitMQ credentials from S3
3. Decrypt credentials
4. Send to RabbitMQ

### TODO

* Add tests
* Add CF templates

### Maintainers
Mikhael Santos