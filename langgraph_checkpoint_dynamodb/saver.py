import boto3
from boto3.dynamodb.conditions import Key, Attr
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple, Checkpoint, CheckpointMetadata, \
    ChannelVersions
from langgraph.checkpoint.serde.base import SerializerProtocol

from langgraph_checkpoint_dynamodb.write import Write


class DynamoDBSaver(BaseCheckpointSaver):
    def __init__(
        self,
        *,
        client_config: Optional[Dict[str, Any]] = None,
        serde: Optional[SerializerProtocol] = None,
        checkpoints_table_name: str,
        writes_table_name: str,
    ) -> None:
        super().__init__(serde=serde)
        self.client = boto3.client("dynamodb", **(client_config or {}))
        self.dynamodb = boto3.resource("dynamodb", **(client_config or {}))
        self.checkpoints_table_name = checkpoints_table_name
        self.writes_table_name = writes_table_name
        self.checkpoints_table = self.dynamodb.Table(self.checkpoints_table_name)
        self.writes_table = self.dynamodb.Table(self.writes_table_name)

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        configurable = self.validate_configurable(config.get("configurable"))
        item = self._get_item(configurable)
        if not item:
            return None

        checkpoint = self.serde.loads_typed([item["type"], item["checkpoint"].value])
        metadata = self.serde.loads_typed([item["type"], item["metadata"].value])

        # Fetch pending writes
        partition_key = self.get_write_partition_key(item)
        response = self.writes_table.query(
            KeyConditionExpression=Key(
                "thread_id_checkpoint_id_checkpoint_ns"
            ).eq(partition_key)
        )
        pending_writes = []
        items = response.get("Items", [])
        for write_item in items:
            write = Write.from_dynamodb_item(write_item)
            value = self.serde.loads_typed([write.type, write.value.value])
            pending_writes.append((write.task_id, write.channel, value))

        config = {
            "configurable": {
                "thread_id": item["thread_id"],
                "checkpoint_ns": item.get("checkpoint_ns", ""),
                "checkpoint_id": item["checkpoint_id"],
            }
        }
        parent_config = None
        if item.get("parent_checkpoint_id"):
            parent_config = {
                "configurable": {
                    "thread_id": item["thread_id"],
                    "checkpoint_ns": item.get("checkpoint_ns", ""),
                    "checkpoint_id": item["parent_checkpoint_id"],
                }
            }

        checkpoint_tuple = CheckpointTuple(
            config=config,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes,
        )
        return checkpoint_tuple

    def _get_item(self, configurable):
        if configurable["checkpoint_id"] is not None:
            # Use get_item
            response = self.checkpoints_table.get_item(
                Key={
                    "thread_id": configurable["thread_id"],
                    "checkpoint_id": configurable["checkpoint_id"],
                }
            )
            return response.get("Item")
        else:
            # Use query
            key_condition_expression = Key("thread_id").eq(configurable["thread_id"])
            args = dict()
            if configurable["checkpoint_ns"]:
                args['FilterExpression'] = Attr("checkpoint_ns").eq(
                    configurable["checkpoint_ns"]
                )

            response = self.checkpoints_table.query(
                KeyConditionExpression=key_condition_expression,
                Limit=1,
                ConsistentRead=True,
                ScanIndexForward=False,
                *args,
            )
            items = response.get("Items", [])
            return items[0] if items else None

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[Dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        configurable = config.get("configurable", {})
        thread_id = configurable.get("thread_id")

        key_condition_expression = Key("thread_id").eq(thread_id)
        if before and before.get("configurable") and before["configurable"].get(
            "checkpoint_id"
        ):
            key_condition_expression &= Key("checkpoint_id").lt(
                before["configurable"]["checkpoint_id"]
            )

        response = self.checkpoints_table.query(
            KeyConditionExpression=key_condition_expression,
            Limit=limit,
            ScanIndexForward=False,
        )

        for item in response.get("Items", []):
            checkpoint = self.serde.loads_typed(item["type"], item["checkpoint"])
            metadata = self.serde.loads_typed(item["type"], item["metadata"])
            config = {
                "configurable": {
                    "thread_id": item["thread_id"],
                    "checkpoint_ns": item.get("checkpoint_ns", ""),
                    "checkpoint_id": item["checkpoint_id"],
                }
            }
            parent_config = None
            if item.get("parent_checkpoint_id"):
                parent_config = {
                    "configurable": {
                        "thread_id": item["thread_id"],
                        "checkpoint_ns": item.get("checkpoint_ns", ""),
                        "checkpoint_id": item["parent_checkpoint_id"],
                    }
                }
            yield CheckpointTuple(
                config=config,
                checkpoint=checkpoint,
                metadata=metadata,
                parent_config=parent_config,
            )

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        configurable = self.validate_configurable(config.get("configurable"))
        thread_id = configurable["thread_id"]
        type1, serialized_checkpoint = self.serde.dumps_typed(checkpoint)
        type2, serialized_metadata = self.serde.dumps_typed(metadata)
        if type1 != type2:
            raise ValueError(
                "Failed to serialize checkpoint and metadata to the same type."
            )
        item = {
            "thread_id": thread_id,
            "checkpoint_ns": config.get("configurable", {}).get("checkpoint_ns", ""),
            "checkpoint_id": checkpoint.get("id"),
            "parent_checkpoint_id": config.get("configurable", {}).get("checkpoint_id"),
            "type": type1,
            "checkpoint": serialized_checkpoint,
            "metadata": serialized_metadata,
        }
        self.checkpoints_table.put_item(Item=item)
        return {
            "configurable": {
                "thread_id": item["thread_id"],
                "checkpoint_ns": item["checkpoint_ns"],
                "checkpoint_id": item["checkpoint_id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[Tuple[str, Any]],
        task_id: str,
    ) -> None:
        configurable = self.validate_configurable(config.get("configurable"))
        thread_id = configurable["thread_id"]
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = configurable.get("checkpoint_id")
        if checkpoint_id is None:
            raise ValueError("Missing checkpoint_id")
        write_items = []
        for idx, write in enumerate(writes):
            channel, value = write
            type_, serialized_value = self.serde.dumps_typed(value)
            item = Write(
                thread_id=thread_id,
                checkpoint_ns=checkpoint_ns,
                checkpoint_id=checkpoint_id,
                task_id=task_id,
                idx=idx,
                channel=channel,
                type=type_,
                value=serialized_value,
            )
            write_items.append({"PutRequest": {"Item": item.to_dynamodb_item()}})
        # Batch write items in batches of 25
        for i in range(0, len(write_items), 25):
            batch = write_items[i : i + 25]
            request_items = {self.writes_table_name: batch}
            self.client.batch_write_item(RequestItems=request_items)

    def get_write_partition_key(self, item):
        return Write.get_partition_key(item)

    def get_write_sort_key(self, item):
        return Write.get_sort_key(item)

    def validate_configurable(self, configurable):
        if not configurable:
            raise ValueError("Missing configurable")
        thread_id = configurable.get("thread_id")
        checkpoint_ns = configurable.get("checkpoint_ns", "")
        checkpoint_id = configurable.get("checkpoint_id")
        if not isinstance(thread_id, str):
            raise ValueError("Invalid thread_id")
        if not (isinstance(checkpoint_ns, str) or checkpoint_ns is None):
            raise ValueError("Invalid checkpoint_ns")
        if not (isinstance(checkpoint_id, str) or checkpoint_id is None):
            raise ValueError("Invalid checkpoint_id")
        return {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns or "",
            "checkpoint_id": checkpoint_id,
        }
