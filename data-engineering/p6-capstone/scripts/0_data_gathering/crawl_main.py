import uuid
from enum import Enum
from time import sleep

import requests
from author_entries import AuthorEntries
from author_model import Author
from pydantic import parse_obj_as
from pymongo import MongoClient
from topic_model import Topic, TopicSummary


class ProcessStatus(Enum):
    NOT_PROCESSED = 0
    PENDING = 1
    PROCESSED = 2


MIN_ENTRY_COUNT = 10  
SLEEP_TIMER = 0
host = "https://api.----------.com"
session = requests.session()
client = MongoClient("mongodb://mongoadmin:--------/")  # todo remove
db = client["-------"]
db_entries = db["entries"]
db_entries.create_index("Id", unique=True)
db_authors = db["authors"]
db_authors.create_index("Id", unique=True)
db_author_detail = db["author_detail"]
db_author_detail.create_index("UserId", unique=True)
db_topics = db["topics"]
db_topics.create_index("TopicId", unique=True)


def setup_session():
    headers = {
        "Host": "api.------.com",
        "content-type": "application/x-www-form-urlencoded",
        "accept-encoding": "gzip",
        "user-agent": "okhttp/3.12.1"
    }

    session.headers = headers

    client_secret = str(uuid.uuid4())
    client_unique_id = str(uuid.uuid4())
    api_secret = "-----------------"  # todo remove

    data = "Platform=g&Version=2.0.1&Build=52" \
           "&Api-Secret=" + api_secret + \
           "&Client-Secret=" + client_secret + \
           "&ClientUniqueId=" + client_unique_id

    anonymous_token = "/v2/account/anonymoustoken"
    anonymous_token_request = session.post(host + anonymous_token, data=data)

    session.headers['authorization'] = "Bearer " + anonymous_token_request.json()['Data']['access_token']
    session.headers['client-secret'] = client_secret


def crawl_topic(topic_id: int, page: int):
    topic_url = "/v2/topic/" + str(topic_id) + "/?p=" + str(page)
    topic_request = session.get(host + topic_url)
    topic = parse_obj_as(Topic, topic_request.json())

    if topic.Success is None:
        print("Error - ", str(topic_id))
        return

    for entry in topic.Data.Entries:

        # Add entries to db
        entry.TopicId = topic.Data.Id
        entry.TopicTitle = topic.Data.Title
        try:
            insert_result = db_entries.insert_one(entry.dict())
        except Exception as e:
            pass

        # Add author to db
        author = entry.Author
        author.ProcessStatus = ProcessStatus.NOT_PROCESSED.value
        try:
            insert_result = db_authors.insert_one(author.dict())
        except Exception as e:
            pass

    # Process the next page
    print("remaining entries: " + str(topic.Data.EntryCounts.AfterLastEntry))
    if topic.Data.EntryCounts.AfterLastEntry > 0:
        next_page_index = topic.Data.PageIndex + 1
        sleep(SLEEP_TIMER)
        return topic.Data.Id, next_page_index
    else:
        print("topic id = {}, completed".format(str(topic_id)))
        return topic.Data.Id, 0


def crawl_author_entries(author_name: str, page: int):
    author_entries_url = "/v2/user/{}/entries?p={}".format(author_name, str(page))
    author_entries_request = session.get(host + author_entries_url)
    author_entries_request.json()
    author_entries = parse_obj_as(AuthorEntries, author_entries_request.json())
    if author_entries.Success is None:
        print("Error - ", author_entries)
        return author_name, 0

    for entry_root in author_entries.Data.Entries:
        entry = entry_root.Entry
        entry.TopicId = entry_root.TopicId.Id
        entry.TopicTitle = entry_root.TopicId.TopicTitle.Title
        try:
            insert_result = db_entries.insert_one(entry.dict())
        except Exception as e:
            pass

        try:
            topic_summary = TopicSummary()
            topic_summary.TopicId = entry.TopicId
            topic_summary.TopicTitle = entry.TopicTitle
            topic_summary.ProcessStatus = ProcessStatus.NOT_PROCESSED.value
            insert_result = db_topics.insert_one(topic_summary.dict())
        except Exception as e:
            pass
    print("author processing {}/{}".format(str(author_entries.Data.PageIndex), str(author_entries.Data.PageCount)))
    if author_entries.Data.PageCount > author_entries.Data.PageIndex:
        next_page_index = author_entries.Data.PageIndex + 1
        sleep(SLEEP_TIMER)
        return author_name, next_page_index
    else:
        return author_name, 0


def crawl_author_favorite_entries(author_name: str, page: int, favorite_entries: list = []):
    author_favorite_entries_url = "/v2/user/{}/favorites?p={}".format(author_name, str(page))
    author_favorite_entries_request = session.get(host + author_favorite_entries_url)
    author_favorite_entries_request.json()

    author_favorite_entries = parse_obj_as(AuthorEntries, author_favorite_entries_request.json())

    for entry_root in author_favorite_entries.Data.Entries:
        entry = entry_root.Entry
        entry.TopicId = entry_root.TopicId.Id
        entry.TopicTitle = entry_root.TopicId.TopicTitle.Title
        favorite_entries.append(entry.Id)
        try:
            insert_result = db_entries.insert_one(entry.dict())
        except Exception as e:
            pass

        try:
            topic_summary = TopicSummary()
            topic_summary.TopicId = entry.TopicId
            topic_summary.TopicTitle = entry.TopicTitle
            topic_summary.ProcessStatus = ProcessStatus.NOT_PROCESSED.value
            insert_result = db_topics.insert_one(topic_summary.dict())
        except Exception as e:
            pass

    print("author favorite processing {}/{}".format(str(author_favorite_entries.Data.PageIndex),
                                                    str(author_favorite_entries.Data.PageCount)))

    if author_favorite_entries.Data.PageIndex > 900:
        return favorite_entries

    if author_favorite_entries.Data.PageCount > author_favorite_entries.Data.PageIndex:
        next_page_index = author_favorite_entries.Data.PageIndex + 1
        sleep(SLEEP_TIMER)
        return crawl_author_favorite_entries(author_name, next_page_index, favorite_entries)
    else:
        return favorite_entries


def crawl_author_favorited_entries(author_name: str, page: int, favorited_entries: list = []):
    author_favorite_entries_url = "/v2/user/{}/favorited?p={}".format(author_name, str(page))
    author_favorite_entries_request = session.get(host + author_favorite_entries_url)
    author_favorite_entries_request.json()

    author_favorite_entries = parse_obj_as(AuthorEntries, author_favorite_entries_request.json())

    for entry_root in author_favorite_entries.Data.Entries:
        entry = entry_root.Entry
        favorited_entries.append(entry.Id)

    print("author favorited processing {}/{}".format(str(author_favorite_entries.Data.PageIndex),
                                                     str(author_favorite_entries.Data.PageCount)))
    if author_favorite_entries.Data.PageIndex > 900:
        return favorited_entries
    if author_favorite_entries.Data.PageCount > author_favorite_entries.Data.PageIndex:
        next_page_index = author_favorite_entries.Data.PageIndex + 1
        sleep(SLEEP_TIMER)
        return crawl_author_favorited_entries(author_name, next_page_index, favorited_entries)
    else:
        return favorited_entries


def crawl_author(author_name: str):
    print("processing author: {}".format(author_name))
    author_url = "/v2/user/{}/".format(author_name)
    author_request = session.get(host + author_url)

    author = parse_obj_as(Author, author_request.json())

    if author.Success is None:
        print("Error - ", author_name)
        return

    author.Data.UserId = author.Data.UserInfo.UserIdentifier.Id

    try:
        insert_result = db_authors.insert_one(author.Data.UserInfo.UserIdentifier.dict())
    except Exception as e:
        pass

    if author.Data.UserInfo.EntryCounts.Total > MIN_ENTRY_COUNT:
        index = 1
        while index > 0:
            author_name, index = crawl_author_entries(author_name, page=index)

    favorite_entries = crawl_author_favorite_entries(author_name, page=1)
    author.Data.FavoriteEntries = ', '.join(map(str, favorite_entries))

    favorited_entries = crawl_author_favorited_entries(author_name, page=1)
    author.Data.FavoritedEntries = ', '.join(map(str, favorited_entries))

    try:
        insert_result = db_author_detail.insert_one(author.Data.dict())
    except Exception as e:
        pass


def crawl_author_from_db():
    selected_author = db_authors.find_one({"ProcessStatus": 0})
    if selected_author is None:
        return -1
    try:
        db_authors.update_one({
            '_id': selected_author['_id']
        }, {
            '$set': {
                'ProcessStatus': 1
            }
        }, upsert=False)

        crawl_author(selected_author['Nick'])

        db_authors.update_one({
            '_id': selected_author['_id']
        }, {
            '$set': {
                'ProcessStatus': 2
            }
        }, upsert=False)
        return 0
    except Exception as ex:
        print("crawl_author_from_db ", ex)
        return -1


# setup_session()


# crawl_author("cilekli sut")

# is_there_more_authors = 0
# while is_there_more_authors == 0:
#    try:
#        is_there_more_authors = crawl_author_from_db()
#    except Exception as ex:
#        print("Unknown ex: ", ex)


def crawl_topic_from_db():
    selected_topic = db_topics.find_one({"ProcessStatus": 0})
    if selected_topic is None:
        print(selected_topic)
        return -1
    try:
        db_topics.update_one({
            '_id': selected_topic['_id']
        }, {
            '$set': {
                'ProcessStatus': 1
            }
        }, upsert=False)

        index = 1
        total_count = 0
        while index > 0:
            topic_id, index, total_count = crawl_topic(selected_topic['TopicId'], index)

        db_topics.update_one({
            '_id': selected_topic['_id']
        }, {
            '$set': {
                'ProcessStatus': 2,
                'EntryCount': total_count,
            }
        }, upsert=True)
        return 0
    except Exception as ex:
        print("crawl_author_from_db ", ex)
        return -1


setup_session()

more_topic = 0
while more_topic == 0:
    more_topic = crawl_topic_from_db()
