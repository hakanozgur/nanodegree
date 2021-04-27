import uuid

import requests
from pydantic import parse_obj_as
from src.model.popular_topics import PopularTopics
from src.model.topic import Topic

HOST = "---------------------" # todo

SLEEP_TIMER = 0.01


def setup_session() -> requests.Session:
    """
    authenticates to the api, creates client id

    :return: authenticated session
    """
    session = requests.session()

    headers = {
        "Host": "----------------", # todo
        "content-type": "application/x-www-form-urlencoded",
        "accept-encoding": "gzip",
        "user-agent": "okhttp/3.12.1"
    }

    session.headers = headers

    client_secret = str(uuid.uuid4())
    client_unique_id = str(uuid.uuid4())
    api_secret = "------------------------------" # todo

    data = "Platform=g&Version=2.0.1&Build=52" \
           "&Api-Secret=" + api_secret + \
           "&Client-Secret=" + client_secret + \
           "&ClientUniqueId=" + client_unique_id

    anonymous_token = "/v2/account/anonymoustoken"
    anonymous_token_request = session.post(HOST + anonymous_token, data=data)

    session.headers['authorization'] = "Bearer " + anonymous_token_request.json()['Data']['access_token']
    session.headers['client-secret'] = client_secret
    return session


def get_popular_topics(session: requests.Session, page: int, topics=[]):
    """
    recursively get the topics in paginated api

    :param session: authenticated session
    :param page: current page
    :param topics: used for recursive calls
    :return: array of topics
    """
    popular_url = f"/v2/index/popular?p={page}"
    popular_request = session.post(HOST + popular_url)
    popular_topics = parse_obj_as(PopularTopics, popular_request.json())

    if not popular_topics.Success:
        print('parse topic failed')
        return topics

    for topic in popular_topics.Data.Topics:
        topics.append(topic)

    if popular_topics.Data.PageCount > popular_topics.Data.PageIndex:
        return get_popular_topics(session=session, page=page + 1, topics=topics)
    else:
        return topics


def get_entries_from_topic(session: requests.Session, topic_id: int, page: int, entries=[]):
    """
    recursively gets the entries in a given topic

    :param session: authenticated session
    :param topic_id:
    :param page: current page
    :param entries: used for recursive calls
    :return: array of entries
    """
    topic_url = "/v2/topic/" + str(topic_id) + "/?p=" + str(page)
    topic_request = session.get(HOST + topic_url)
    topic = parse_obj_as(Topic, topic_request.json())

    if topic.Success is None or topic.Success is False:
        print("Error - ", str(topic_id))
        return entries, topic

    for entry in topic.Data.Entries:
        entry.TopicId = topic.Data.Id
        entry.TopicTitle = topic.Data.Title
        entries.append(entry)

    # Process the next page
    if topic.Data.EntryCounts.AfterLastEntry > 0:
        next_page_index = topic.Data.PageIndex + 1
        # sleep(SLEEP_TIMER)
        return get_entries_from_topic(session, topic_id, next_page_index, entries)
    else:
        return entries, topic


def get_todays_entries_from_topic(session: requests.Session, topic_id: int, page: int, entries=[]):
    """
    similar to @get_entries_from_topic method gets all the entries from a topic but filters them by todays entries

    :param session: authenticated session
    :param topic_id:
    :param page: current page
    :param entries: used for recursive calls
    :return: array of entries
    """
    topic_url = "/v2/topic/" + str(topic_id) + '/popular' + "?p=" + str(page)
    topic_request = session.get(HOST + topic_url)
    topic = parse_obj_as(Topic, topic_request.json())

    if topic.Success is None or topic.Success is False:
        print("Error - ", str(topic_id))
        return entries, topic

    for entry in topic.Data.Entries:
        entry.TopicId = topic.Data.Id
        entry.TopicTitle = topic.Data.Title
        entries.append(entry)

    # Process the next page
    if topic.Data.EntryCounts.AfterLastEntry > 0:
        next_page_index = page + 1
        # sleep(SLEEP_TIMER)
        return get_todays_entries_from_topic(session, topic_id, next_page_index, entries)
    else:
        return entries, topic
