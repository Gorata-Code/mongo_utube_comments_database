import googleapiclient.discovery
from mongoDB_u_tube_api_comments_storage_helper import crud, constants as const


def thread_fetch(video_id: str, user_api_key: str) -> None:

    """
    Connects to the YouTube API and accesses the comments & other metadata from the given video url
    :param video_id: The video for which to access the comment data (of)
    :param user_api_key: The user's API Key
    :return: None
    """

    fetch = googleapiclient.discovery.build(const.API_SERVICE_NAME, const.API_VERSION, developerKey=user_api_key)

    cargo = fetch.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=65
    )

    PRODUCT = cargo.execute()
    RESPONSE = PRODUCT

    ALL_COMMENTS: [] = []

    print('\n\t\t\t**** COMMENTS SECTION ****\n')

    for key, _ in RESPONSE.items():
        if key == 'items':
            COMMENTS_COLLECTION = RESPONSE[key]  # We get the value here. The "value" being the comment response i.e.
            # a dict containing all the comments and associated data of the video in question.

            for comment_bundle in COMMENTS_COLLECTION:  # Iterating to retrieve each comment & associated metrics.
                author = comment_bundle['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comment = comment_bundle['snippet']['topLevelComment']['snippet']['textDisplay']
                comment_date_time = comment_bundle['snippet']['topLevelComment']['snippet']['publishedAt']
                formatted_comment_date = f'{comment_date_time.split("T")[0]}'
                formatted_comment_time = f'{comment_date_time.split("T")[1].replace("Z", "")}'
                comment_likes = comment_bundle['snippet']['topLevelComment']['snippet']['likeCount']

                print(f'{author} comments:\n\t"{comment}"\nDATE: {formatted_comment_date}\nTIME: '
                      f'{formatted_comment_time}\nLIKES: {comment_likes}\n')

                ALL_COMMENTS.append({"AUTHOR": author, "COMMENT": comment, "DATE": formatted_comment_date,
                                     "TIME": formatted_comment_time, "LIKES": comment_likes})

    database_tether_point(ALL_COMMENTS)


def database_tether_point(comment_bundle: [{}]) -> None:

    """
    Links to the database CRUD operations file
    :param comment_bundle: The comment metadata which will be used to populate our database
    :return: None
    """
    crud.ping_deployment()  # Is our database server actually available
    crud.db_ops_centre(comment_bundle)
