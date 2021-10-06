import pandas as pd
import googleapiclient.discovery as discovery

api_service_name = "youtube"
api_version = "v3"
API_KEY = 'AIzaSyAYOEb9pgaJ807aPI95qSuoSFdQvwt7geg'
youtube = discovery.build(api_service_name, api_version, developerKey=API_KEY)

def fetch_youtube_items(keywords):
    items = []
    response = None
    next_token = None
    for i in range(0, 2):
        if i > 0:
            if response is not None:
                next_token = response['nextPageToken']
        request = youtube.search().list(part="snippet", q=f"{keywords} cartoon english subtitles",
                                        pageToken=next_token, maxResults=1000)
        response = request.execute()
        items += response['items']
    return items

def get_youtube_videos(keywords):
    items = fetch_youtube_items(keywords)
    request = youtube.search().list(part="snippet",q=f"{keywords} cartoon english subtitles", maxResults=1000)
    response = request.execute()
    
    videos = pd.json_normalize(items) #apo json se dataframe
    videos.drop(videos.columns.difference(['id.videoId','snippet.title']), 1, inplace=True)
    videos.rename(columns={'id.videoId':'id', 'snippet.title': 'title'}, inplace=True)
    videos['url'] = "https://www.youtube.com/watch?v=" + videos['id']
    videos.dropna(inplace=True)
    
   #  #takes all playlists from the response
   #  playlists = pd.json_normalize(items)
   #  playlists.drop(playlists.columns.difference(['id.playlistId','snippet.title']), 1, inplace=True)
   #  playlists.rename(columns={'id.playlistId':'id', 'snippet.title': 'title'}, inplace=True)
   #  playlists.dropna(inplace=True)
    
   #  for playlist_id in playlists.id:
   #      playlist_request = youtube.playlistItems().list(part="snippet", playlistId=playlist_id)
   #      playlist_response = playlist_request.execute()
   #      playlist_videos = pd.json_normalize(playlist_response['items']) #apo json se dataframe
   #      playlist_videos.drop(playlist_videos.columns.difference(['id','snippet.title']), 1, inplace=True)
   #      playlist_videos.rename(columns={'snippet.title': 'title'}, inplace=True)
   #      playlist_videos['url'] = "https://www.youtube.com/watch?v=" + playlist_videos['id']
   #      playlist_videos.dropna(inplace=True)
   #      videos = videos.append(playlist_videos)

    print(f'Videos count: {videos.shape[0]}')
    return videos

def get_videos_duration(video_ids):
    # TODO: NA FTIAXW 50ades kai na kanw osa request xreiazetai!!!
    request = youtube.videos().list(part='snippet, contentDetails, statistics', id=','.join(video_ids[:50]), maxResults=1000)
    response = request.execute()
    videos_with_duration = pd.json_normalize(response['items']) #apo json se dataframe
    videos_with_duration.drop(videos_with_duration.columns.difference(['id', 'snippet.title','contentDetails.duration','statistics.viewCount','statistics.likeCount','statistics.dislikeCount']), 1, inplace=True)
    videos_with_duration.rename(columns={'snippet.title': 'title', 'contentDetails.duration':'duration', 'statistics.viewCount': 'views', 'statistics.likeCount': 'likes', 'statistics.dislikeCount':'dislikes'}, inplace=True)
    videos_with_duration['url'] = "https://www.youtube.com/watch?v=" + videos_with_duration['id']
    videos_with_duration.dropna(inplace=True)
    videos_with_duration.title = videos_with_duration.title.str.lower()
    return videos_with_duration


