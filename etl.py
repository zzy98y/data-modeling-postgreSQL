import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    # open song file
    df = pd.read_json(filepath,lines = True)

    # insert song record
    for i in range(df.shape[0]):
        columns = df[['song_id','title','artist_id','year','duration']].values
        song_data = columns[i].tolist()
        cur.execute(song_table_insert, song_data)
    
    # insert artist record
    for i in range(df.shape[0]):
        columns = df[['artist_id','artist_name','artist_location',\
                 'artist_latitude','artist_longitude']].values
        artist_data = columns[i].tolist()
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the user information in order to store it into the users table.
    Then it extracts the time information in order to store it into the time table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open log file
    df = pd.read_json(filepath,lines = True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    time_df = pd.DataFrame(data = {
    column_labels[0]:time_data[0],
    column_labels[1]:time_data[1],
    column_labels[2]:time_data[2],
    column_labels[3]:time_data[3],
    column_labels[4]:time_data[4],
    column_labels[5]:time_data[5],
    column_labels[6]:time_data[6]
})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (time_df['start_time'][index], row.userId, row.level,
                         songid, artistid, row.sessionId, row.location,
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure processes different function with json file in the directory and ensure all the file is     being processed in the directory

    INPUTS: 
    * cur the cursor variable, conn the connection, filepath is the path to both song and log file, func is the function we run to process the ETL 
    * filepath the file path to the log file and song file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This procedure make sure we connect to the database and proceed with the two function we created in this pipeline.

    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()