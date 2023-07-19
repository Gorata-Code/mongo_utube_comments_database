import sys
from mongoDB_u_tube_api_comments_storage_helper.comments_bank import thread_fetch


def script_summary() -> None:
    print('''
               ***----------------------------------------------------------------------------------------***
         \t***------------------------ DUMELANG means GREETINGS! ~ G-CODE -----------------------***
                     \t***------------------------------------------------------------------------***\n
              
        \t"U-TUBE-API-COMMENT-DATABASE" Version 1.0.0\n
        
        This bot will help you collect the viewers\' comments from any YouTube video
        of your choosing. All you need to do is provide a YouTube video link and the script
        will fetch the top 65 comments for you, as well as the authors\' names, the number
        of likes for each comment and the date each comment was published. The bot will then save
        these details to your Mongo Database Atlas account. You can then query this database in the
        command prompt / terminal.
        
        Cheers!
    ''')


def comment_bot(video_id: str, user_api_key: str) -> None:
    try:

        thread_fetch(video_id, user_api_key)

    except Exception as exp:

        if 'INTERNET' in str(exp):

            print(''''

                    Please make sure you are connected to the internet and Try again.

                    Cheers!

                            ''')

            input('\nPress Enter To Exit.\n')

        elif 'Timed out receiving message from renderer' or 'cannot determine loading status' in str(exp):
            print('\nIt appears you\'re experiencing internet connectivity issues. Please try again.')

        elif 'ERR_NAME_NOT_RESOLVED' or 'ERR_CONNECTION_CLOSED' or 'unexpected command response' in str(exp):
            print('\nYour internet connection may have been interrupted.')
            print('Please make sure you\'re still connected to the internet and try again.')

        else:
            raise

        input('\nPress Enter to Exit & Try Again.')
        sys.exit(1)

    input('\nPress Enter to Exit.')
    sys.exit(0)


def main() -> None:
    script_summary()

    video_id: str = 'LXkO4HdQXdo'
    user_api_key: str = input('\nPlease paste your api_key_here: ').strip()

    if len(user_api_key) >= 1:
        comment_bot(video_id, user_api_key)
    elif len(user_api_key) < 1:
        print('\nPlease provide the required information.')
        input('\nPress Enter to Exit: ')
        sys.exit(1)


if __name__ == '__main__':
    main()
