import stweet as st
from db import get_twitter_usernames, update_user
import json
import time

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
    for userName, userId in get_twitter_usernames():
        try:
            user_info = try_user_scrape(userName)
            user_info['user_id'] = userId
        except Exception as e:
            print(e)
            continue
        update_user(user_info)
        print(user_info)
        time.sleep(5)
