import stweet as st
from db import get_twitter_usernames, update_user, get_recent_users
import json
import time
import psycopg2
from dotenv import load_dotenv
import os

production = True
# load .env file
load_dotenv()
# set DB_USER to the value of the environment variable DB_USER
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
if production:
    # production
    DB_PASSWORD = os.getenv("PROD_PASSWORD")
    DB_HOST = os.getenv("PROD_HOST")
else:
    # local
    DB_PASSWORD = os.getenv("LOCAL_PASSWORD")
    DB_HOST = os.getenv("LOCAL_HOST")


def try_user_scrape(username):
    user_task = st.GetUsersTask([username])
    output_json = st.CollectorRawOutput()
    st.GetUsersRunner(get_user_task=user_task, raw_data_outputs=[output_json]).run()
    output = output_json.get_raw_list()[0]
    data = output.to_json_line()
    data = json.loads(data)
    username = data['raw_value']['legacy']['screen_name']
    follower_count = data['raw_value']['legacy']['followers_count']
    following_count = data['raw_value']['legacy']['friends_count']
    tweet_count = data['raw_value']['legacy']['statuses_count']
    user_data = {
        'username': username,
        'follower_count': follower_count,
        'following_count': following_count,
        'tweet_count': tweet_count
    }
    return user_data


if __name__ == '__main__':
    mode = 'forward'
    try:
        connection = psycopg2.connect(user=DB_USER,
                                      password=DB_PASSWORD,
                                      host=DB_HOST,
                                      port=DB_PORT,
                                      database=DB_NAME)
        cursor = connection.cursor()
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgresSQL", error)

    if mode == 'recent':
        while(True):
            for userName, userId in get_recent_users(cursor):
                try:
                    user_info = try_user_scrape(userName)
                    user_info['user_id'] = userId
                except Exception as e:
                    print(e)
                    continue
                update_user(user_info)
                print(user_info)
                time.sleep(3)
    elif mode == 'forward':
        for userName, userId in get_twitter_usernames(cursor):
            try:
                user_info = try_user_scrape(userName)
                user_info['user_id'] = userId
            except Exception as e:
                print(e)
                continue
            update_user(user_info)
            print(user_info)
            time.sleep(3)
    elif mode == 'backwards':
        for userName, userId in reversed(get_twitter_usernames()):
            try:
                user_info = try_user_scrape(userName)
                user_info['user_id'] = userId
            except Exception as e:
                print(e)
                continue
            update_user(user_info)
            print(user_info)
            time.sleep(3)