import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime
import csv

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


def get_twitter_usernames(cursor=None):
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
        select_query = """select * from users where "twitterUsername" is null"""

        cursor.execute(select_query)
        metadata_records = cursor.fetchall()

        print(f"total users without data: {len(metadata_records)}")
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgresSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
        return twitter_usernames


def get_recent_users(cursor=None):
    twitter_usernames = []


    try:
        if cursor is None:
            connection = psycopg2.connect(user=DB_USER,
                                          password=DB_PASSWORD,
                                          host=DB_HOST,
                                          port=DB_PORT,
                                          database=DB_NAME)
            cursor = connection.cursor()
        select_query = """
        select * from users 
        where "lastUpdated" > current_timestamp - interval '1 minute'
        and "twitterUsername" is null
        order by "lastUpdated" desc
        """

        cursor.execute(select_query)
        metadata_records = cursor.fetchall()

        count = 0
        count2 = 0
        for row in metadata_records:
            print(row[2] + " " + str(row[17]))
            if row[12]:
                count += 1
            else:
                count2 += 1
                twitter_usernames.append((row[2], row[0]))
        print(f"total users with data: {count}")
        print(f"total users without data: {count2}")
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgresSQL", error)

    finally:
        # # closing database connection.
        # if connection:
        #     cursor.close()
        #     connection.close()
        return twitter_usernames


def update_user(user_info, cursor=None):

    user_id = user_info['user_id']
    follower_count = user_info['follower_count']
    following_count = user_info['following_count']
    tweet_count = user_info['tweet_count']
    try:
        if cursor is None:
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

    # finally:
    #     # # closing database connection.
    #     # if connection:
    #     #     cursor.close()
    #     #     connection.close()


def get_user_events(cursor, username):
    print("getting user events")
    try:
        if cursor is None:
            connection = psycopg2.connect(user=DB_USER,
                                          password=DB_PASSWORD,
                                          host=DB_HOST,
                                          port=DB_PORT,
                                          database=DB_NAME)
            cursor = connection.cursor()
        select_query = f"""
        select * from events
        where "trader" = '0x5ddfdb36ba0c4179f1fc42d4b796867bed02a15f'
        """

        print(f'select query: {select_query}')

        cursor.execute(select_query)

        user_records = cursor.fetchall()
        # write user_records to a csv with columns: id, block number, trader, subject, isBuy, shareAmount, ethAmount, protocolEthAmount, subjectEthAmount, timestamp
        with open('user_records.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['id', 'block_number', 'trader', 'subject', 'isBuy', 'shareAmount', 'ethAmount', 'protocolEthAmount', 'subjectEthAmount', 'timestamp'])
            for row in user_records:
                writer.writerow(row)



        print(f'User records: {user_records}')
        for row in user_records:
            print(row)
    except Exception as e:
        print(e)
