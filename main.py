import asyncio
import json
import logging
import sys
from json import JSONDecodeError

import aio_pika
import cfg_load
import riprova as riprova
from pydantic import ValidationError

from router import dispatch
from schemas import IncomingMessageSchema

LOG_FORMAT = '%(asctime)s %(levelname)-6s %(funcName)s (on line %(lineno)-4d) : %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def _get_message(d: dict):
    try:

        return {'id': d['id'],
                'body': d['body'],
                "fromMe": False,
                "self": 0,
                "isForwarded": d["isForwarded"],
                'author': d['sender']['id']['_serialized'],
                'time': d['t'],
                'chatId': d['chatId']['_serialized'],
                'type': d['type'],
                'senderName': d['sender']['name'] or d['chat']['contact']['formattedName'] or '',
                'caption': d['caption'],
                'quotedMsgId': d['quotedMsg']
                }
    except Exception as e:
        logger.info(f"ERROR :: {e.__class__.__name__} :: {str(e)} :: {d}")
        return None


class WhatsAPI:

    def __init__(self, event_loop):
        self.loop = event_loop
        self.exchange = None

        _rmq = confg['rabbitmq']
        self.amqp_url = self.url = f"amqp://{_rmq['username']}:{_rmq['password']}@{_rmq['host']}:{_rmq['port']}"

        self.retrier = riprova.AsyncRetrier(
            timeout=1000,
            backoff=riprova.ExponentialBackOff(
                factor=1, interval=1, max_elapsed=retry_conf['timeout'], multiplier=retry_conf['multiplier']),
            on_retry=self._on_retry
        )

    @classmethod
    async def _on_retry(cls, err, next_try):
        logger.exception(err)
        logger.info('RETRYING AGAIN IN {} seconds'.format(next_try))

    async def publish(self, payload):
        message = aio_pika.Message(
            json.dumps(payload, default=str).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )

        try:
            if self.exchange is not None:
                await self.exchange.publish(message, routing_key=confg['rabbitmq']['outgoing_routing_key'])
            else:
                logger.error('Rabbitmq exchange is empty or None')
                raise
        except Exception as e:
            logger.info('ERROR PUBLISHING MESSAGE...')
            logger.exception(str(e))
            raise

    async def _process(self, message: aio_pika.IncomingMessage):

        try:
            json_body = json.loads(message.body.decode('utf-8'))

            m = _get_message(json_body)
            if m is None:
                return

            if json_body['chat'] is not None:
                logger.info('')
                logger.info('')
                logger.info('------------------------------ -----------------')
                logger.info('------ RECEIVED CHAT MESSAGE FROM QUEUE --------')
                logger.info('-------------------------------- ---------------')
                logger.info(f"sender: {m['senderName']}")
                logger.info(f"message: {m['body']}")

                resp = dispatch(IncomingMessageSchema(**m), cfg=confg)
                if resp:
                    logger.debug(f"response: {resp}")
                    await self.publish({
                        'chat_id': m['chatId'],
                        'message': resp,
                        'type': 'chat'}
                    )

        except JSONDecodeError as e:
            logger.info("ERROR ON INPUT")
            logger.error(f'[JSONDecodeError]: {e}')
            error_message = {
                'type': 'bundles',
                'request': message.body.decode('utf-8'),
                'http_code': None,
                'result_code': None,
                'result_desc': 'JSONDecodeError',
                'result_details': str(e)
            }
            logger.warning(error_message)
        except ValidationError as e:
            logger.info("ERROR ON INPUT")
            logger.error(f"[ValidationError]: {e}")
            error_message = {
                'type': 'bundles',
                'request': json_body,
                'http_code': None,
                'result_code': None,
                'result_desc': "ValidationError",
                'result_details': str(e).replace('\n', " '")
            }
            logger.warning(error_message)

        except KeyError as e:
            logger.exception(str(e))
            logger.info('PROBABLY INVALID CONFIG?')
            raise

    async def message_callback(self, message: aio_pika.IncomingMessage):

        async with message.process(requeue=True):
            # running retry with context manager
            async with self.retrier as retry:
                await retry.run(self._process, message)

    async def main(self):

        logger.info("Connecting to rabbitmq..")
        connection = await aio_pika.connect(self.amqp_url, loop=self.loop)

        logger.info("Creating channel")
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=confg['rabbitmq']['prefetch'])

        self.exchange = await channel.declare_exchange(
            confg['rabbitmq']['exchange'], aio_pika.ExchangeType.TOPIC, durable=True,
        )
        logger.info("creating queues...")
        incoming_messages = await channel.declare_queue(
            confg['rabbitmq']['request_queue'],
            durable=True,
            arguments={
                'x-message-ttl': confg['rabbitmq']['ttl'],
                'x-dead-letter-exchange': confg['rabbitmq']['exchange'],
                'x-dead-letter-routing-key': confg['rabbitmq']['dlx_exchange_key'],
            }

        )
        await incoming_messages.bind(self.exchange, routing_key=confg['rabbitmq']['incoming_routing_key'])

        # create outgoing responses queue
        outgoing_messages = await channel.declare_queue(
            confg['rabbitmq']['response_queue'],
            durable=True,
            arguments={
                'x-message-ttl': confg['rabbitmq']['ttl'],
                'x-dead-letter-exchange': confg['rabbitmq']['exchange'],
                'x-dead-letter-routing-key': confg['rabbitmq']['dlx_exchange_key'],
            }
        )
        await outgoing_messages.bind(self.exchange, routing_key=confg['rabbitmq']['outgoing_routing_key'])

        logger.info(f"[{confg['rabbitmq']['request_queue']}] waiting for messages..")
        await incoming_messages.consume(self.message_callback)

        return connection


if __name__ == '__main__':
    args = sys.argv
    config_path = args[1] if len(args) > 1 else 'config.yaml'
    try:
        confg = cfg_load.load(config_path)
    except FileNotFoundError:
        raise NotImplementedError(
            "You have not specified a config file path. Default config name is 'config.yaml' and cannot be found on "
            "current directory")

    # setting retry variables
    retry_conf = confg['retry']

    loop = asyncio.get_event_loop()

    handler = WhatsAPI(loop)
    conn = loop.run_until_complete(handler.main())

    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(conn.close())  # closing amqp connection
