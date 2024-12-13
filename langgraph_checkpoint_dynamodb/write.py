from typing import Any

class Write:
    separator = ":::"

    def __init__(
        self,
        *,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
        channel: str,
        type: str,
        value: Any,
    ):
        self.thread_id = thread_id
        self.checkpoint_ns = checkpoint_ns
        self.checkpoint_id = checkpoint_id
        self.task_id = task_id
        self.idx = idx
        self.channel = channel
        self.type = type
        self.value = value

    def to_dynamodb_item(self):
        return {
            "thread_id_checkpoint_id_checkpoint_ns": {
                'S': self.get_partition_key(
                    {
                        "thread_id": self.thread_id,
                        "checkpoint_id": self.checkpoint_id,
                        "checkpoint_ns": self.checkpoint_ns,
                    }
                )
            },
            "task_id_idx": {
                'S': self.get_sort_key(
                    {"task_id": self.task_id, "idx": self.idx}
                )
            },
            "channel": {'S': self.channel},
            "type": {'S': self.type},
            "value": {'B': self.value},  # Assuming self.value is binary data
        }

    @classmethod
    def from_dynamodb_item(cls, item):
        thread_id_checkpoint_id_checkpoint_ns = item[
            "thread_id_checkpoint_id_checkpoint_ns"
        ]
        task_id_idx = item["task_id_idx"]
        channel = item["channel"]
        type_ = item["type"]
        value = item["value"]
        thread_id, checkpoint_id, checkpoint_ns = thread_id_checkpoint_id_checkpoint_ns.split(
            cls.separator
        )
        task_id, idx = task_id_idx.split(cls.separator)
        return cls(
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
            task_id=task_id,
            idx=int(idx),
            channel=channel,
            type=type_,
            value=value,
        )

    @staticmethod
    def get_partition_key(item):
        return Write.separator.join(
            [item["thread_id"], item["checkpoint_id"], item.get("checkpoint_ns", "")]
        )

    @staticmethod
    def get_sort_key(item):
        return Write.separator.join([item["task_id"], str(item["idx"])])
