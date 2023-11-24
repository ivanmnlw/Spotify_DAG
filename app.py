from dotenv import load_dotenv
from flask import Flask, request, redirect, url_for, session
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import requests
import json
import os
import time
import pandas as pd
import numpy as np
from connection import cur, conn

import spotipy
from spotipy.oauth2 import SpotifyOAuth

app = Flask(__name__)
load_dotenv()

app.secret_key = "sddf8322348gfdsf"
app.config['SESSION_COOKIE_NAME'] = 'Ivans Cookie'
TOKEN_INFO = "token_info"

AUTH_URL = 'https://accounts.spotify.com/authorize'
TOKEN_URL = 'https://accounts.spotify.com/api/token'
REDIRECT_URI = 'http://localhost:5000/callback' 
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SCOPE = [
    "user-read-recently-played",
    "playlist-read-collaborative",
    "user-library-read"
]

@app.route('/')
def login():
    sp_oauth = create_spotify_oauth()
    auth_url = sp_oauth.get_authorize_url()
    return redirect(auth_url)

@app.route('/callback')
def callback():
    sp_oauth = create_spotify_oauth()
    session.clear()
    code = request.args.get('code')
    token_info = sp_oauth.get_access_token(code)
    session[TOKEN_INFO] = token_info
    return redirect(url_for('getTracks', _external=True))

@app.route('/getTracks')
def getTracks():
    try:
        token_info = get_token()
    except:
        print('User not logged in')    
        return redirect('/')

    sp = spotipy.Spotify(auth=token_info['access_token'])
    # items = sp.current_user_saved_tracks(limit=50, offset=0)['items']
    now = datetime.now()
    yest = now - timedelta(days=1)
    yest_unix_timestamp = int(yest.timestamp()) * 1000
    items = sp.current_user_recently_played(limit=50, after=yest_unix_timestamp)['items']
        
    song_names = []
    artist_names = []
    played_at = []

    for item in (items):
        tracks = item['track']['name']
        artist = item['track']['artists'][0]['name']
        played_att = item['played_at']
        song_names.append(tracks)
        artist_names.append(artist)
        played_at.append(played_att)
    
    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at
    }
    
    df = pd.DataFrame(song_dict, columns=['song_name', "artist_name", "played_at"])
    if check_valid(df):
        print("Data valid, proceed to load stage")
        print(df)
        df.to_sql('myplaylist', conn, index = False, if_exists = 'append')
        conn.close()
        print("Data in the database")
    return items

def get_token():
    token_info = session.get(TOKEN_INFO, None)
    if not token_info:
        raise "exception"
    now = int(time.time())
    is_expired = token_info['expires_at'] - now < 60
    if(is_expired):
        sp_oauth = create_spotify_oauth()
        token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])['items'][0]
    return token_info

def create_spotify_oauth():
    return SpotifyOAuth(
            client_id = CLIENT_ID,
            client_secret = CLIENT_SECRET,
            redirect_uri = url_for('callback', _external=True),
            scope = SCOPE
    )

def check_valid(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing Execution")
        return False
    else :
        pass
    # Primary Key Check
    if pd.Series(['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key check is violated")
    # Check if null
    if df.isnull().values.any():
        raise Exception("Null valued detected")
    # Check if all data is yesterday
    timestamp_format = '%Y-%m-%d'
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    timestamp = yesterday.strftime(timestamp_format)
    transform = pd.to_datetime(df['played_at'])
    transform1 = transform.dt.date
    df['date_stamp'] = transform1
    list_df = df['date_stamp'].tolist()
    for list in list_df:
        list = list.strftime(timestamp_format)
        if list != timestamp:
            raise Exception ("The songs must be at least come from 24 hours age")
        else :
            return True

if __name__ == '__main__':
    app.run(port=5000,debug=True)