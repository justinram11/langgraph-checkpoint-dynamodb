# langgraph-checkpoint-dynamodb

Implementation of a LangGraph CheckpointSaver that uses a AWS's DynamoDB

## Inspiration

Based on: https://github.com/researchwiseai/langgraphjs-checkpoint-dynamodb

## Required DynamoDB Tables

To be able to use this checkpointer, two DynamoDB table's are needed, one to store
checkpoints and the other to store writes. Below are some examples of how you
can create the required tables.

### Terraform

```hcl
# Variables for table names
variable "checkpoints_table_name" {
  type = string
}

variable "writes_table_name" {
  type = string
}

# Checkpoints Table
resource "aws_dynamodb_table" "checkpoints_table" {
  name         = var.checkpoints_table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "thread_id"
  range_key = "checkpoint_id"

  attribute {
    name = "thread_id"
    type = "S"
  }

  attribute {
    name = "checkpoint_id"
    type = "S"
  }
}

# Writes Table
resource "aws_dynamodb_table" "writes_table" {
  name         = var.writes_table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "thread_id_checkpoint_id_checkpoint_ns"
  range_key = "task_id_idx"

  attribute {
    name = "thread_id_checkpoint_id_checkpoint_ns"
    type = "S"
  }

  attribute {
    name = "task_id_idx"
    type = "S"
  }
}
```

### AWS CDK

```python
from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
)
from constructs import Construct

class DynamoDbStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        checkpoints_table_name = 'YourCheckpointsTableName'
        writes_table_name = 'YourWritesTableName'

        # Checkpoints Table
        dynamodb.Table(
            self,
            'CheckpointsTable',
            table_name=checkpoints_table_name,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            partition_key=dynamodb.Attribute(
                name='thread_id',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='checkpoint_id',
                type=dynamodb.AttributeType.STRING,
            ),
        )

        # Writes Table
        dynamodb.Table(
            self,
            'WritesTable',
            table_name=writes_table_name,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            partition_key=dynamodb.Attribute(
                name='thread_id_checkpoint_id_checkpoint_ns',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='task_id_idx',
                type=dynamodb.AttributeType.STRING,
            ),
        )
```

## Using the Checkpoint Saver

### Default

To use the DynamoDB checkpoint saver, you only need to specify the names of
the checkpoints and writes tables. In this scenario the DynamoDB client will
be instantiated with the default configuration, great for running on AWS Lambda.

```python
from langgraph_checkpoint_dynamodb import DynamoDBSaver
...
checkpoints_table_name = 'YourCheckpointsTableName'
writes_table_name = 'YourWritesTableName'

memory = DynamoDBSaver(
    checkpoints_table_name=checkpoints_table_name,
    writes_table_name=writes_table_name,
)

graph = workflow.compile(checkpointer=memory)
```

### Providing Client Configuration

If you need to provide custom configuration to the DynamoDB client, you can
pass in an object with the configuration options. Below is an example of how
you can provide custom configuration.

```python
memory = DynamoDBSaver(
    checkpoints_table_name=checkpoints_table_name,
    writes_table_name=writes_table_name,
    client_config={
        'region': 'us-west-2',
        'accessKeyId': 'your-access-key-id',
        'secretAccessKey': 'your-secret-access-key',
    }
)
```
