import json


def preprocess_topics(input_file, output_file):
    """
    change the topic file format from input file and writes to output
    changed format is used for staging database tables

    :param input_file:
    :param output_file:
    :return: success of the case
    """
    try:
        with open(output_file, 'w', encoding='utf8') as write_file:
            with open(input_file, 'r', encoding='utf8') as topics_file:
                for item in topics_file:
                    topic = json.loads(item)
                    new_json = {
                        'topic_id': topic['TopicId'],
                        'topic_title': topic['Title'],
                        'entry_count': -1 if 'FullCount' not in topic else topic['FullCount'],
                        'created': '',
                        'slug': ''
                    }
                    write_file.write(json.dumps(new_json, ensure_ascii=False) + "\n")
        return True
    except Exception as ex:
        print(f'preprocess topic exception {str(ex)}')
        return False


def preprocess_entries(input_file, output_file):
    """
    change the entry file format from input file and writes to output
    changed format is used for staging database tables

    :param input_file:
    :param output_file:
    :return: success of the case
    """
    special_nl = 'Ƕ'
    try:
        with open(output_file, 'w', encoding='utf8') as write_file:
            with open(input_file, 'r', encoding='utf8') as entries_file:
                for item in entries_file:
                    entry = json.loads(item)

                    if entry['CommentSummary'] and 'Content' in entry['CommentSummary']:
                        entry['CommentSummary']['Content'] = entry['CommentSummary']['Content'].replace('\r',
                                                                                                        '').replace(
                            "\n", special_nl)

                    new_json = {
                        "entry_id": entry['Id'],
                        "author_nick": entry['Author']['Nick'],
                        "author_id": entry['Author']['Id'],
                        "created": entry['Created'],
                        "last_updated": entry['LastUpdated'],
                        "is_favorite": entry['IsFavorite'],
                        "favorite_count": entry['FavoriteCount'],
                        "hidden": entry['Hidden'],
                        "active": entry['Active'],
                        "comment_count": entry['CommentCount'],
                        "comment_summary": entry['CommentSummary'],
                        "avatar_url": entry['AvatarUrl'],
                        "topic_id": entry['TopicId'],
                        "topic_title": entry['TopicTitle'],
                        "content": entry['Content'].replace('\r', '').replace("\n", special_nl),
                    }
                    write_file.write(json.dumps(new_json, ensure_ascii=False) + "\n")
        return True
    except Exception as ex:
        print(f'preprocess entry exception {str(ex)}')
        return False
