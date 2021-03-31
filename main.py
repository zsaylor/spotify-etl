import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "xe5yGtYvRwOpNBsJnSRQcg"
TOKEN = "BQAuasQehCsutypvVa3mck160284dlgkiQR9qhxxqpn2W4U7kpoUZfEi11JJEZBpvdJATkGgIEODXAxNKONoQcJ0a52pMgPGNszBEHKYPbRs9kOZdoUC1y1IT6ycwOlwp5e0LXL-epZ-OEYdzN8kkA"

# Note: Token expires every 15 minutes.
# Generate new token here: https://developer.spotify.com/console/get-recently-played/


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if DataFrame is empty
    if df.empty:
        print("No songs downloaded. Finishing execution.")
        return False

    # Primary Key check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated.")

    # Check for null values
    if df.isnull().values.any():
        raise Exception("Null value found.")

    # Check that all timestamps are from last 24 hours
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception(
                "At least 1 returned song was not played in the last 24 hours."
            )

    return True


if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN),
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(
            time=yesterday_unix_timestamp
        ),
        headers=headers,
    )

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps,
    }

    song_df = pd.DataFrame(
        song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"]
    )

    print(song_df)

    # Validate
    if check_if_valid_data(song_df):
        print("Data valid, proceeding to Load stage...")

    # Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("my_played_tracks.sqlite")
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Database succesfully opened...")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
    except:
        print("Data already in database...")

    conn.close()
    print("Database successfully closed...")