import psycopg2
import os
from dotenv import load_dotenv

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


def get_twitter_usernames():
    twitter_usernames = []
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(user=DB_USER,
                                      password=DB_PASSWORD,
                                      host=DB_HOST,
                                      port=DB_PORT,
                                      database=DB_NAME)
        cursor = connection.cursor()
        select_query = "select * from users"

        cursor.execute(select_query)
        metadata_records = cursor.fetchall()

        count = 0
        for row in metadata_records:
            if row[12]:
                count += 1
            else:
                twitter_usernames.append((row[2], row[0]))
        print(f"total users with data: {count}")
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgresSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
        return twitter_usernames


def update_user(user_info):
    connection = None
    cursor = None
    user_id = user_info['user_id']
    follower_count = user_info['follower_count']
    following_count = user_info['following_count']
    tweet_count = user_info['tweet_count']
    try:
        connection = psycopg2.connect(user=DB_USER,
                                      password=DB_PASSWORD,
                                      host=DB_HOST,
                                      port=DB_PORT,
                                      database=DB_NAME)
        cursor = connection.cursor()
        update_query = """
                UPDATE users 
                SET "followerCount" = %s, "followingCount" = %s, "tweetCount" = %s 
                WHERE id = %s;
                """

        cursor.execute(update_query, (follower_count, following_count, tweet_count, user_id))
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
