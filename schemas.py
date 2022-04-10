from typing import Optional, Union

from pydantic import BaseModel


class IncomingMessageSchema(BaseModel):
    id: str
    body: str
    fromMe: Optional[bool] = False
    self: Optional[int] = 0
    isForwarded: Optional[bool] = False
    author: str
    time: int
    chatId: str
    type: str
    senderName: str
    caption: Union[str, None] = None
    quotedMsgId: Union[str, int, None] = None
