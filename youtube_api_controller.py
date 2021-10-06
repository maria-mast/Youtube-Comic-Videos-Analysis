import settings
import google_auth_oauthlib.flow
import googleapiclient.discovery as discovery
import googleapiclient.errors
import pandas as pd

api_service_name = "youtube"
api_version = "v3"
API_KEY = settings.API_KEY
youtube = discovery.build(api_service_name, api_version, developerKey=API_KEY)

def get_youtube_videos(keywords):
    request = youtube.search().list(part="snippet",q=f"{keywords} cartoon english subtitles", maxResults=1000)
    response = request.execute()
    
    videos = pd.json_normalize(response['items']) #apo json se dataframe
    videos.drop(videos.columns.difference(['id.videoId','snippet.title']), 1, inplace=True)
    videos.rename(columns={'id.videoId':'id', 'snippet.title': 'title'}, inplace=True)
    videos.dropna(inplace=True)
    
    #takes all playlists from the response
    playlists = pd.json_normalize(response['items'])
    playlists.drop(playlists.columns.difference(['id.playlistId','snippet.title']), 1, inplace=True)
    playlists.rename(columns={'id.playlistId':'id', 'snippet.title': 'title'}, inplace=True)
    playlists.dropna(inplace=True)
    
    for playlist_id in playlists.id:
        playlist_request = youtube.playlistItems().list(part="snippet", playlistId=playlist_id)
        playlist_response = playlist_request.execute()
        playlist_videos = pd.json_normalize(playlist_response['items']) #apo json se dataframe
        playlist_videos.drop(playlist_videos.columns.difference(['id','snippet.title']), 1, inplace=True)
        playlist_videos.rename(columns={'snippet.title': 'title'}, inplace=True)
        playlist_videos.dropna(inplace=True)
        videos = videos.append(playlist_videos)
    print(f'Videos count: {videos.shape[0]}')
    return videos

def get_videos_duration(video_ids):
    # TODO: NA FTIAXW 50ades kai na kanw osa request xreiazetai!!!
    request = youtube.videos().list(part='snippet, contentDetails, statistics', id=','.join(video_ids[:50]), maxResults=1000)
    response = request.execute()
    videos_with_duration = pd.json_normalize(response['items']) #apo json se dataframe
    videos_with_duration.drop(videos_with_duration.columns.difference(['id', 'snippet.title','contentDetails.duration','statistics.viewCount','statistics.likeCount','statistics.dislikeCount']), 1, inplace=True)
    videos_with_duration.rename(columns={'snippet.title': 'title', 'contentDetails.duration':'duration', 'statistics.viewCount': 'views', 'statistics.likeCount': 'likes', 'statistics.dislikeCount':'dislikes'}, inplace=True)
    videos_with_duration.dropna(inplace=True)
    videos_with_duration.title = videos_with_duration.title.str.lower()
    return videos_with_duration

def convert_duration_to_seconds(duration):
   day_time = duration.split('T')
   day_duration = day_time[0].replace('P', '')
   day_list = day_duration.split('D')
   if len(day_list) == 2:
      day = int(day_list[0]) * 60 * 60 * 24
      day_list = day_list[1]
   else:
      day = 0
      day_list = day_list[0]
   hour_list = day_time[1].split('H')
   if len(hour_list) == 2:
      hour = int(hour_list[0]) * 60 * 60
      hour_list = hour_list[1]
   else:
      hour = 0
      hour_list = hour_list[0]
   minute_list = hour_list.split('M')
   if len(minute_list) == 2:
      minute = int(minute_list[0]) * 60
      minute_list = minute_list[1]
   else:
      minute = 0
      minute_list = minute_list[0]
   second_list = minute_list.split('S')
   if len(second_list) == 2:
      second = int(second_list[0])
   else:
      second = 0
   return day + hour + minute + second