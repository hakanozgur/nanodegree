from __future__ import annotations

from typing import Any, List

from pydantic import BaseModel


class Topic(BaseModel):
    MatchedCount: int
    TopicId: int
    FullCount: int
    Title: str


class Data(BaseModel):
    Topics: List[Topic]
    PageCount: int
    PageSize: int
    PageIndex: int


class PopularTopics(BaseModel):
    Success: bool
    Message: Any
    Data: Data
