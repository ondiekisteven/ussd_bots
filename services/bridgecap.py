import logging

import requests

from schemas import IncomingMessageSchema


logger = logging.getLogger(__name__)


def ussd_handler(message: IncomingMessageSchema, session_id, cfg=None):
    if message.body.lower() == 'start':
        logger.info("new session starting...")
        body = ''
    else:
        body = message.body

    if 'c.us' in message.chatId:
        if '254742976667' in message.chatId:
            print("modifying url...")
            url = f'http://34.247.24.244:5002/ussd?MSISDN=0712345678&session_id={session_id}&ussd_string={body}'
        else:
            url = f'http://34.247.24.244:5002/ussd?MSISDN={message.chatId.replace("@c.us", "")}&session_id={session_id}&ussd_string={body}'
        resp = requests.get(url)
        logger.info(f"{resp.status_code}  ::  {resp.text}")
        if resp.status_code == 200:
            return resp.text
        else:

            return 'CON error retrieving response'
    else:
        return None

