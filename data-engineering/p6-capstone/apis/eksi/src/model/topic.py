from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class AuthorSummary(BaseModel):
    Nick: str
    Id: int
    ProcessStatus: Optional[int]

    def __hash__(self):
        return hash((self.Nick, self.Id, self.ProcessStatus))


class TopicSummary(BaseModel):
    TopicId: Optional[int]
    TopicTitle: Optional[str]
    ProcessStatus: Optional[int]


class Entry(BaseModel):
    Id: int
    Content: str
    Author: AuthorSummary
    Created: str
    LastUpdated: Optional[str]
    IsFavorite: bool
    FavoriteCount: int
    Hidden: bool
    Active: bool
    CommentCount: int
    CommentSummary: Optional[dict]
    AvatarUrl: Optional[str]
    TopicId: Optional[int]
    TopicTitle: Optional[str]


class EntryCounts(BaseModel):
    BeforeFirstEntry: int
    AfterLastEntry: int
    Buddy: int
    Total: int


class Video(BaseModel):
    DisplayInfo: Optional[dict]
    InTopicVideo: bool


class Data(BaseModel):
    Id: int
    Title: str
    Entries: List[Entry]
    PageCount: int
    PageSize: int
    PageIndex: int
    PinnedEntry: Optional[dict]
    EntryCounts: EntryCounts
    DraftEntry: Optional[str]
    IsTracked: bool
    IsTrackable: bool
    Slug: str
    Video: Optional[Video]
    Disambiguations: List
    IsAmaTopic: bool
    MatterCount: int


class Topic(BaseModel):
    Success: bool
    Message: Optional[str]
    Data: Data
