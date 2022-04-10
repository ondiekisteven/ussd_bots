import logging

import redis

from schemas import IncomingMessageSchema
from services import bridgecap

r = redis.Redis(host='localhost', port=6379, charset="utf-8", decode_responses=True, db=2)
logger = logging.getLogger(__name__)


def welcome():
    return 'CON Welcome back. Choose a service:\n\n1. BridgeCap Insurance'


def dispatch(message: IncomingMessageSchema, cfg):
    redis_key = message.chatId.split('@')[0]

    if message.body.lower() == 'bridgecap':
        r.hset(redis_key, 'app', 'bridgecap')

    app = r.hget(redis_key, 'app') or 'welcome'

    if message.body.lower() in 'join bot' or app == 'welcome':
        if message.body.lower() == 'join bot':
            r.hset(redis_key, 'app', 'welcome')
            resp = welcome()
            return resp
        elif message.body.lower() == '1':
            r.hset(redis_key, 'app', 'bridgecap')
        else:
            return print(f'Invalid Input! Try again\n\n{welcome()}')

        if message.body.lower() == '1':
            r.hset(redis_key, 'bridgecap')

    elif message.body.lower() == 'bridgecap' or app == 'bridgecap':
        r.hset(redis_key, 'app', 'bridgecap')
        if message.body.lower() == 'bridgecap':
            message.body = 'start'
            session_id = r.hget(redis_key, 'session_id')
            logger.info(f"{message.chatId.split('@')[0]} : {session_id}")
            if session_id is None:
                r.hset(redis_key, 'session_id', '100000001')
            r.hincrby(redis_key, 'session_id', '1')
        resp = bridgecap.ussd_handler(message, r.hget(redis_key, 'session_id'), cfg)

        return resp[4:]
