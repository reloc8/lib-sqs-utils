import boto3
import hashlib
import logging

from dataclasses import dataclass
from typing import Any, AnyStr, List, Callable, Union, Tuple
from botocore.exceptions import ClientError


@dataclass
class SqsClient:

    boto_client: Any
    boto_resource: Any


@dataclass(init=False)
class SqsUtils:

    logger: logging.Logger

    def __init__(self, logger: logging.Logger = logging.getLogger()):

        self.logger = logger
        self.client = SqsClient(boto_client=boto3.client('sqs'), boto_resource=boto3.resource('sqs'))

    def send_batch(self,
                   batch: List[AnyStr], queue_name: AnyStr,
                   group_id: AnyStr = 'default',
                   fun_identify: Callable[[AnyStr], AnyStr] = lambda s: hashlib.sha1(s).hexdigest(),
                   fun_deduplicate: Callable[[AnyStr], AnyStr] = lambda s: hashlib.sha1(s).hexdigest()) -> bool:
        """Sends a batch of messages to a queue

        :param batch:           Batch of messages to send
        :param queue_name:      Name of the queue
        :param group_id:        SQS group-id - messages within the same group are strictly ordered
        :param fun_identify:    Callable that returns a message id within this batch
        :param fun_deduplicate: Callable that returns a message id within a deduplication interval
        :return:                True if entire batch was sent
        """

        successful = True

        if len(batch) > 0:

            fifo = queue_name.lower().endswith('.fifo')

            queue = self.client.boto_resource.get_queue_by_name(QueueName=queue_name)
            entries = [
                dict(
                    MessageBody=message,
                    MessageGroupId=group_id,
                    Id=fun_identify(message.encode('utf-8')),
                    MessageDeduplicationId=fun_deduplicate(message.encode('utf-8'))
                ) for message in batch
            ]
            if not fifo:
                for entry in entries:
                    entry.pop('MessageDeduplicationId')
                    entry.pop('MessageGroupId')

            successful_writes = queue.send_messages(Entries=entries).get('Successful')
            successful = successful_writes is not None and len(successful_writes) == len(batch)

        return successful

    def receive_one(self,
                    queue_name: AnyStr,
                    hide_for_seconds: int = 60 * 60,
                    poll_for_seconds: int = 20) -> Union[AnyStr, None]:
        """Receives a single message from a queue

        :param queue_name:          Name of the queue
        :param hide_for_seconds:    SQS visibility-timeout - how long the message will be hidden from subsequent request
        :param poll_for_seconds:    SQS wait-time-seconds - maximum time to wait for new messages
        :return:                    The body of the message if present, None otherwise
        """

        batch = self.receive_many(
            queue_name=queue_name,
            hide_for_seconds=hide_for_seconds,
            poll_for_seconds=poll_for_seconds,
            max_batch_size=1
        )

        if len(batch) > 0:
            return batch[0]
        else:
            return None

    def receive_many(self,
                     queue_name: AnyStr,
                     hide_for_seconds: int = 60 * 60,
                     poll_for_seconds: int = 20,
                     max_batch_size: int = 10,
                     with_receipt: bool = False) -> List[AnyStr]:
        """Receives a batch of messages from a queue

        :param queue_name:          Name of the queue
        :param hide_for_seconds:    SQS visibility-timeout - how long the message will be hidden from subsequent request
        :param poll_for_seconds:    SQS wait-time-seconds - maximum time to wait for new messages
        :param max_batch_size:      Max number of messages to receive
        :param with_receipt:        If True each element is a couple (body, receipt)
        :return:                    The list of message bodies if any message is present, an empty list otherwise
        """

        messages = self.__receive_many(
            queue_name=queue_name, hide_for_seconds=hide_for_seconds, poll_for_seconds=poll_for_seconds,
            max_batch_size=max_batch_size
        )

        if with_receipt:
            return messages
        else:
            return [message[0] for message in messages]

    def remove_batch(self,
                     queue_name: AnyStr,
                     receipts: List[AnyStr]) -> bool:
        """Removes a batch of messages from a queue given their receipts

        :param queue_name:  Name of the queue
        :param receipts:    List of message receipts
        :return:            True if all messages have been removed
        """

        if len(receipts) == 0:
            return True

        try:
            response = len(
                self.client.boto_client.delete_message_batch(
                    QueueUrl=self.client.boto_client.get_queue_url(QueueName=queue_name)['QueueUrl'],
                    Entries=[
                        dict(
                            Id=hashlib.sha1(receipt.encode('utf-8')).hexdigest(),
                            ReceiptHandle=receipt
                        ) for receipt in receipts
                    ]
                )
            ) == len(receipts)
        except ClientError as error:
            if error.response['Error']['Code'] == 'ReceiptHandleIsInvalid':
                response = False
            else:
                raise

        return response

    def __receive_many(self,
                       queue_name: AnyStr,
                       hide_for_seconds: int = 60 * 60,
                       poll_for_seconds: int = 20,
                       max_batch_size: int = 10) -> List[Tuple[AnyStr, AnyStr]]:
        """Receives a batch of messages and their corresponding receipts from a queue

        :param queue_name:          Name of the queue
        :param hide_for_seconds:    SQS visibility-timeout - how long the message will be hidden from subsequent request
        :param poll_for_seconds:    SQS wait-time-seconds - maximum time to wait for new messages
        :param max_batch_size:      Max number of messages to receive
        :return:                    List of couples (body, receipt) if any message is present, an empty list otherwise
        """

        response = self.client.boto_client.receive_message(
            QueueUrl=self.client.boto_client.get_queue_url(QueueName=queue_name)['QueueUrl'],
            MaxNumberOfMessages=max_batch_size,
            VisibilityTimeout=hide_for_seconds,
            WaitTimeSeconds=poll_for_seconds
        )

        messages = response.get('Messages')

        if messages is not None and len(messages) > 0:
            messages = [(message['Body'], message['ReceiptHandle']) for message in messages]
        else:
            messages = []

        return messages
