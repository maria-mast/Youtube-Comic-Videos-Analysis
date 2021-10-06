import numpy as np
import pandas as pd
import math

def filter_by_duration(videos):
    #convert the video duration from Youtube format to seconds
    videos.duration = videos.duration.apply(lambda x: convert_duration_to_seconds(x))

    duration_threshold = get_duration_threshold(videos)
    print(duration_threshold)
    
    videos = videos.duration < duration_threshold
    return videos

def filter_by_excluded_words(videos):
    excl_keywords = "compilat episodes"
    videos = ~videos.title.str.contains('|'.join(excl_keywords.split()))
    return videos

def filter_by_keywords(keywords, videos):
    seperated_keyword = keywords.split()

    videos['title'] = videos['title'].str.lower()

    #keep videos that contain at least on of the keywords
    videos = videos[videos['title'].str.contains('|'.join(seperated_keyword))]
    return videos


def get_duration_threshold(videos_with_duration):
   average_duration = videos_with_duration['duration'].mean()
   
   max_duration = videos_with_duration['duration'].max()
   
   av_and_max = [average_duration, max_duration]
   
   return np.mean(av_and_max)

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

def analyze_subs(subs_json):
    df = pd.DataFrame(columns=['id', 'duration_good', 'duration_bad', 'subs'])
    videos_with_subs = subs_json[0]
    videos_without_subs = subs_json[1]
    # print(subs[1])
    curr_subs = []
    for v_id, s in videos_with_subs.items():
        # Export subtitles to json file
        # with open(f'subs/{v_id}_subs.json', 'w') as outfile:
            # json.dump(s, outfile)
        good_dur = 0
        bad_dur = 0
        for subt in s:
            if ("(" in subt['text']) or (")" in subt['text']) \
                    or ("[" in subt['text']) or ("]" in subt['text']):
                bad_dur += subt['duration']
            else:
                good_dur += subt['duration']
                # Remove special characters
                clean_t = subt['text'].replace('\n', ' ')
                curr_subs.append(clean_t)
        # print([v_id, good_dur, bad_dur, curr_subs])
        df = df.append({'id': v_id, 'duration_good': good_dur, 'duration_bad': bad_dur, 'subs': ' '.join(curr_subs)}, ignore_index=True)
    return df

def split_text(text, n=10):
    '''Takes in a string of text and splits into n equal parts, with a default of 10 equal parts.'''

    # Calculate length of text, the size of each chunk of text and the starting points of each chunk of text
    length = len(text)
    size = math.floor(length / n)
    start = np.arange(0, length, size)
    
    # Pull out equally sized pieces of text and put it into a list
    split_list = []
    for piece in range(n):
        split_list.append(text[start[piece]:start[piece]+size])
    return split_list

def get_subs_pieces(videos):
    list_pieces = []
    for t in videos.subs:
        split = split_text(t)
        list_pieces.append(split)
    return list_pieces