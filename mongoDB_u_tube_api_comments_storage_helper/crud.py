import sys
import pytz
import datetime
from tzlocal import get_localzone
from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConfigurationError, ConnectionFailure


def db_set_up(server_api=None) -> MongoClient:

    """
    We create a new client and connect to the server
    :param server_api: Our server config for testing server connections
    :return: Our client-cluster object
    """

    cluster_connection_string: str = input('\nPaste your database connection string here & Press Enter: ').strip()

    try:
        client = MongoClient(cluster_connection_string, server_api=server_api)
        return client
    except Exception or ConfigurationError or ConnectionFailure:
        if ConfigurationError:
            print('\n*** Config Error: Please make sure all your settings are properly configured. ***')
        elif ConnectionFailure:
            print('\n*** Unable to connect to the database, please make sure your password and username are correct. '
                  '***')
        else:
            raise


def ping_deployment() -> None:
    """
    Checking for server availability
    :return: None
    """

    client: MongoClient = db_set_up(ServerApi('1'))

    try:
        client.admin.command('ping')
        print("\n\tYou have successfully connected to MongoDB!")
    except Exception or ServerSelectionTimeoutError:
        if ServerSelectionTimeoutError:
            print('\n*** Unable to connect to any MongoDB Server within the expected timeframe ***')
            input('\nPress Enter to Exit & Try Again.')
        else:
            raise


def db_ops_centre(payload, insitu: bool = True) -> None:

    """
    Our Mother function. Invokes the database connection function and all the CRUD functions
    :param payload: An array of dictionary objects each containing a comment's metadata
    :param insitu: Boolean guard for writing to the database and printing the user menu only once per session
    :return: None
    """

    connect: MongoClient = db_set_up()

    comments_db = connect['utube_comments_db']  # we connect to our database inside our cluster

    the_comments_collection = comments_db.the_comments_collection  # we access our collection for all CRUD operations

    if insitu:

        if the_comments_collection.count_documents({}) == 0:  # Populate the db if it is empty
            the_comments_collection.insert_many(payload)  # Saving all our comments to the database

        print(f'\nThe Comments Database currently contains {the_comments_collection.count_documents({})} comments.\n')

        user_query: str = input(
            '\tType "ONE" to view a specific comment by commenter\'s name or comment keyword or comment date: '
            '\n\tType "ALL" to view all available comments: \n\tType "ADD" to Add a comment to the database: \n\tType '
            '"EDIT" to edit a specific comment in the database: \n\tType "DELETE" to delete a specific comment from the'
            ' database: \n\tType "X" to Exit: ').strip().upper()

    else:
        user_query: str = input('\n\t>>What would you like to do: ').strip().upper()

    if user_query not in ["ONE", "ALL", "ADD", "EDIT", "DELETE", "X"]:
        print(f'\n"{user_query}" is not an option.')
        db_ops_centre([], False)
    elif user_query in ['ONE', "ALL"]:
        read_comments(the_comments_collection, user_query)
    elif user_query == "ADD":
        add_comment(the_comments_collection)
    elif user_query == "EDIT":
        edit_comment(the_comments_collection)
    elif user_query == "DELETE":
        delete_comment(the_comments_collection)
    elif user_query == "X":
        print('\nThank You Bye....')
        sys.exit()

    # We loop here
    query_or_quit: str = input('\nType "Q" to Continue or "X" to Exit: ').strip().upper()

    if query_or_quit == "Q":
        db_ops_centre([], False)
    elif query_or_quit == "X":
        print('\nThank You Bye...')
        sys.exit()
    else:
        print('\n\t***Invalid input***')

    connect.close()


def validate_document(db_conn, target_record) -> int:
    """
    We run a pre-check for the availability of the record(s) the user wants to Read or Edit or Delete
    :param db_conn: Our database collection object
    :param target_record: The record the user is querying
    :return: Integer value which determines the presence or absence of the record in question
    """

    print('\nRecord Validating 1')
    here_or_nah = db_conn.find(target_record)
    print('Record Validating 2')

    if len(list(here_or_nah)) > 0:  # Such a record as has been queried actually exists, so we can operate on it
        print('Record Validating 3')

        return 1
    else:
        print('Record Validating 4')
        return 0


def add_comment(db_collection) -> None:

    """
    Write a comment to the database
    :param db_collection: The database collection object
    :return: None
    """

    author: str = input('\nYour Username: ').strip()
    comment: str = input('\nYour Comment: ').strip().title()

    while author == '':
        print('\nEvery comment has an Author.')
        author: str = input('\nYour Username: ').strip()
    while comment == '':
        print('\nYou have to comment to comment')
        comment: str = input('\nYour Comment: ').strip().title()

    users_local_time: datetime = datetime.datetime.now(tz=pytz.timezone(f'{get_localzone()}'))

    comment_bundle: {} = {'AUTHOR': author, 'COMMENT': comment, 'DATE': users_local_time.strftime('%Y-%m-%d'),
                          'TIME': users_local_time.strftime('%H:%M:%S')}

    db_collection.insert_one(comment_bundle)
    print(f'\n\tYou have successfully added a comment to the database!')


def read_comments(db_collection, reading_scope: str) -> None:
    """
    For reading all the available comments in the database
    :param db_collection: Database collection object
    :param reading_scope: Read a specific record or all the records
    :return: None
    """

    if reading_scope == "ONE":
        document_attribute: str = input('\nView by "Author" or "Comment" (e.g. Thank you) or "Date" (YYYY-MM-DD) or '
                                        '"Likes" (e.g. 2)?: ').upper().strip()

        lookup_tag = input(f'\nState the {document_attribute} would you like to view?: ').strip()

        if document_attribute not in ["AUTHOR", "COMMENT", "DATE", "LIKES"]:
            print(f'\n"{document_attribute}" is not an option.')
        else:
            if document_attribute in ["DATE", "LIKES"]:
                if document_attribute == "LIKES":
                    try:
                        lookup_tag = int(lookup_tag)
                    except Exception or TypeError:
                        if TypeError:
                            print('\n"Likes" must be whole numbers only!')
                            sys.exit(1)
                        else:
                            raise

                query_filter: {} = {f"{document_attribute}": lookup_tag}

                if validate_document(db_collection, query_filter) == 1:
                    the_comments = db_collection.find(query_filter)
                    print('\nTHE COMMENT(s)\n')
                    [print('\t', comment) for comment in the_comments]
                else:
                    print(f'\nWe have no comment associated with the {document_attribute.title()} "{lookup_tag}" in '
                          f'the database.')

            else:  # Because when querying for words we can use regular expressions, especially  if the user provides
                # partial information unlike with Date and Likes which can only be numbers (of the same pattern)

                query_filter: {} = {f"{document_attribute}": {'$regex': lookup_tag, '$options': 'i'}}

                if validate_document(db_collection, query_filter) == 1:  # There is something query-worthy
                    the_comments = db_collection.find(query_filter)
                    print('\nTHE COMMENTS\n')
                    [print('\t', comment) for comment in the_comments]
                else:
                    print(f'\nWe have no comment associated with the {document_attribute.title()} "{lookup_tag}" in '
                          f'the database.')

    elif reading_scope == "ALL":
        if validate_document(db_collection, None) == 1:
            the_comments = db_collection.find()
            print(f'\nALL THE COMMENTS ({db_collection.count_documents({})})\n')
            [print('\t', comment) for comment in the_comments]
        else:
            print('\nYour database is currently empty.')


def edit_comment(db_collection) -> None:
    """
    For editing some comment metadata
    :param db_collection: The database collection object
    :return: None
    """

    commenter: str = input('\nWhose comment would you like to edit: ').strip()

    document_attribute: str = input('\nWhat do you want to edit? "Author" or "Comment" or "Likes"?: ').upper().strip()

    the_edit = input(f'\nWhat do you want to edit the {document_attribute.title()} to?: ').strip()

    if document_attribute not in ["AUTHOR", "COMMENT", "LIKES"]:
        print(f'\nEditing "{document_attribute}" is not an option.')
    else:
        query_filter: {} = {"AUTHOR": {"$regex": commenter, "$options": "i"}}

        if validate_document(db_collection, query_filter) == 1:
            if document_attribute == "LIKES":
                try:
                    the_edit = int(the_edit)
                except Exception or TypeError:
                    if TypeError:
                        print('\n"Likes" must be whole numbers only!')
                        sys.exit(2)
                    else:
                        raise

            db_collection.update_one(query_filter, {"$set": {document_attribute: the_edit}})
            print(f'\n\tYou have successfully edited {commenter.title()}\'s comment!')
        else:
            print(f'\nSorry, we do not have a commenter by the name "{commenter}".')


def delete_comment(db_collection) -> None:
    """
    For deleting a record or clearing the entire database
    :param db_collection: Our collection object
    :return: None
    """

    delete_scope: str = input('\nWould you like to delete "ONE" comment or "ALL" the comments?: ').strip().upper()

    if delete_scope not in ["ONE", "ALL"]:
        if delete_scope == '':
            print('\nYou\'ve chosen not to delete any comment.')
        else:
            print(f'\n"{delete_scope}" is not an option.')
    else:
        if delete_scope == 'ONE':

            commenter: str = input('\nWhose comment would you like to delete?: ').strip()

            query_filter: {} = {"AUTHOR": {'$regex': commenter, '$options': 'i'}}

            if validate_document(db_collection, query_filter) == 1:
                db_collection.delete_one(query_filter)
                print(f'\nSuccess! You have deleted {commenter}\'s comment from the database!')
            else:
                print(f'\nSorry, we do not have a commenter named "{commenter}".')

        elif delete_scope == "ALL":
            if validate_document(db_collection, None) == 1:
                db_collection.delete_many({})
                print('\nYou have successfully deleted ALL comments from your database.')
            else:
                print('\nYour database is already empty.')
