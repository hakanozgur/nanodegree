import json
from typing import Optional

import requests
from fastapi import BackgroundTasks, FastAPI
from pydantic import BaseModel

from src.crawler.api.api_helper import get_popular_topics, setup_session, get_todays_entries_from_topic
from src.helpers.preprocess import preprocess_entries, preprocess_topics

app = FastAPI()

PATH_TODAYS_TOPICS = "../data/todays_topics.json"
PATH_TODAYS_ENTRIES = "../data/todays_entries.json"
PATH_PROCESSED_TODAYS_ENTRIES = "../data/procesed_todays_entries.json"
PATH_PROCESSED_TODAYS_TOPICS = "../data/procesed_todays_topics.json"


class BaseResponse(BaseModel):
    success: bool
    data: Optional[dict] = None
    error: Optional[dict] = None


@app.get("/")
def read_root():
    """
    root path of the api

    :return: hello mesage
    """
    return BaseResponse(success=True, data={"message": "hello crawler"})


@app.get("/get_todays_topics")
def get_daily():
    """
    parses topics that received entries today.
    writes the topics into the local file

    :return: base response with success of the parse
    """
    try:
        session = setup_session()
        topics = get_popular_topics(session=session, page=1, topics=[])

        with open(PATH_TODAYS_TOPICS, 'w', encoding='utf8') as write_file:
            for topic in topics:
                write_file.write(json.dumps(topic.dict(), ensure_ascii=False) + "\n")

    except Exception as ex:
        return BaseResponse(success=False, error={"exception": str(ex)})
    return BaseResponse(success=True, data={"topic_count": len(topics)})


def get_entries(session: requests.Session):
    """
    async task to get entries
    parses local topics file, crawls each topic to get entries
    and writes the entries to a local file

    :param session: session to the data source
    """
    try:
        todays_topic_ids = []
        with open(PATH_TODAYS_TOPICS, 'r', encoding='utf8') as write_file:
            for item in write_file:
                topic = json.loads(item)
                todays_topic_ids.append(topic['TopicId'])
        with open(PATH_TODAYS_ENTRIES, 'a', encoding='utf8') as write_file:
            for topic_id in todays_topic_ids:
                entries, topic = get_todays_entries_from_topic(session=session, topic_id=topic_id, page=1, entries=[])
                for entry in entries:
                    write_file.write(json.dumps(entry.dict(), ensure_ascii=False) + "\n")
    except Exception as ex:
        print(ex)


@app.get("/get_entries_from_todays_topics")
async def get_todays_entries(background_tasks: BackgroundTasks):
    """
    starts a background task to parse todays entries

    :param background_tasks: supplied by the FastAPI framework
    :return: success of the background task start
    """
    try:
        # Clean entries file
        open(PATH_TODAYS_ENTRIES, 'w').close()

        # Start background task
        session = setup_session()
        background_tasks.add_task(get_entries, session)
    except Exception as ex:
        return BaseResponse(success=False, error={"exception": str(ex)})
    return BaseResponse(success=True, data={"message": "parsing todays entries"})


@app.get("/check_parse_complete")
def check_parse_complete():
    """
    check the running background task if the parse operation is successful

    :return: success if parsed entry count is => than to expected count
    """
    try:
        expected_entry_count = 0
        with open(PATH_TODAYS_TOPICS, 'r', encoding='utf8') as topics_file:
            for item in topics_file:
                topic = json.loads(item)
                expected_entry_count += topic['MatchedCount']

        actual_entry_count = 0
        with open(PATH_TODAYS_ENTRIES, 'r', encoding='utf8') as entries_file:
            for item in entries_file:
                actual_entry_count += 1

        # while parsing new entries might be added, some might have been removed
        deviation = 20
        if actual_entry_count + deviation >= expected_entry_count:
            return BaseResponse(success=True, data={"entry_count": actual_entry_count})
        else:
            return BaseResponse(success=False,
                                data={"entry entry_count": actual_entry_count, "expected_count": expected_entry_count})
    except Exception as ex:
        return BaseResponse(success=False, error={"exception": str(ex)})


@app.get("/preprocess_topics")
def get_preprocess_topics():
    """
    preprocesses downloaded topics to expected format

    :return: base response depending on the success of the case
    """
    try:
        input_file = PATH_TODAYS_TOPICS
        output_file = PATH_PROCESSED_TODAYS_TOPICS

        result = preprocess_topics(input_file, output_file)
        return BaseResponse(success=result)
    except Exception as ex:
        return BaseResponse(success=False, error={"exception": str(ex)})


@app.get("/preprocess_entries")
def get_preprocess_entries():
    """
    preprocesses downloaded entries to expected format

    :return: base response depending on the success of the case
    """
    try:
        input_file = PATH_TODAYS_ENTRIES
        output_file = PATH_PROCESSED_TODAYS_ENTRIES

        result = preprocess_entries(input_file, output_file)
        return BaseResponse(success=result)
    except Exception as ex:
        return BaseResponse(success=False, error={"exception": str(ex)})
