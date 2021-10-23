from pydantic import BaseModel


class Author(BaseModel):
    first_name: str
    last_name: str


class Message(BaseModel):
    data: str
    author: Author
    created_ts: int
