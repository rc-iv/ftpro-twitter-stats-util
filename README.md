# Twitter Scraper using Stweet

This script uses the `stweet` library to scrape the unofficial Twitter API and updates a PostgreSQL database with user details such as follower count, following count, and tweet count.

## Requirements

- Python 3.x
- Required libraries: `stweet`, `psycopg2`, `os`, `dotenv`, `json`, `time`

You can install these libraries using pip:

```
pip install stweet psycopg2 python-dotenv
```

## Setup

1. Create a `.env` file in the root directory of the project.
2. Fill in the required environment variables:

```
PROD_PASSWORD=your_prod_password
PROD_HOST=your_prod_host
DB_USER=your_db_user
DB_PORT=your_db_port
DB_NAME=your_db_name
LOCAL_PASSWORD=your_local_password
LOCAL_HOST=your_local_host
```

Replace the placeholders with your actual database credentials.

3. By default, the script is set to use the production database. If you want to use the local database, set the `production` variable in `db.py` to `False`.

## How to Use

1. Run the main script:

```
python main.py
```

The script will fetch Twitter usernames from the database, scrape the user details from Twitter, and then update the database with the fetched details.

## Functions

### main.py

- `try_user_scrape(username)`: Scrapes the Twitter user details for the given username and returns a dictionary with the user details.

### db.py

- `get_twitter_usernames()`: Fetches the Twitter usernames from the database and returns a list of tuples containing the username and user ID.
- `update_user(user_info)`: Updates the user details in the database using the provided user information.

## Note

Ensure that you keep your database credentials private and do not expose them in any public repositories or platforms. Always use environment variables or other secure methods to store sensitive information.
