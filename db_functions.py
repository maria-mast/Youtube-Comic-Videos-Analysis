import mysql.connector
from datetime import datetime, timedelta
import time
import pandas as pd
import youtube_handler as yt
import random


class MyFunctions:

    def __init__(self, db_name):
        self.db_name = db_name

    def connect_mysql(self, db=None):
        """ Connect to DB """
        conn = mysql.connector.connect(host='localhost', user='root', password='xmas2020', db=db)
        return conn


    def create_db(self):
        """ Create DB """
        conn = self.connect_mysql()
        cursor = conn.cursor()
        cursor.execute("drop database if exists %s" % self.db_name)
        conn.commit()
        cursor.execute("create database if not exists %s" % self.db_name)
        conn.commit()
        cursor.close()
        conn.close()

    # def create_tables(self):
    #     """ Create DB tables """
    #     conn = self.connect_mysql(self.db_name)
    #     cursor = conn.cursor()
    #     table1 = 'videos'
    #     table1_field1 = 'id'
    #     table1_field2 = 'title'
    #     table1_field3 = 'duration'
    #     cursor.execute(
    #         """create table if not exists %s
    #             (
    #             %s varchar(20) primary key not null,
    #             %s varchar(1000),
    #             %s float
    #             )""" % (table1, table1_field1, table1_field2, table1_field3))

    #     table2 = 'video_details'
    #     table2_field1 = 'id'
    #     table2_field2 = 'timestamp'
    #     table2_field3 = 'views'
    #     table2_field4 = 'likes'
    #     table2_field5 = 'dislikes'
    #     cursor.execute(
    #         """create table if not exists %s
    #             (
    #             %s varchar(20) not null,
    #             %s datetime,
    #             %s int default 0,
    #             %s int default 0,
    #             %s int default 0
    #             )""" % (table2, table2_field1, table2_field2, table2_field3, table2_field4, table2_field5))
    #     conn.commit()
    #     cursor.close()
    #     conn.close()

    def create_table(self, table_name, *sql_fields):
        sql_fields_query = ''

        for i in range(len(sql_fields)):
            sql_fields_query += (f"{sql_fields[i].name } {sql_fields[i].type}")
            if i != len(sql_fields)-1:
                sql_fields_query += ','
      
        
        query = f"create table if not exists {table_name} ( {sql_fields_query} )"
        
        conn = self.connect_mysql(self.db_name)
        cursor = conn.cursor()

        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()


    def insert_youtube_videos(self, videos_df):
        """ Create DB and tables """
        conn = self.connect_mysql(self.db_name)
        cursor = conn.cursor()
        rows_inserted = 0
        table = 'youtube_videos'
        field1 = 'id'
        field2 = 'title'
        field3 = 'url'
        field4 = 'duration'
        field5 = 'duration_good'
        field6 = 'duration_bad'
        field7 = 'subs'
        field8 = 'percentage_of_good_subs'
        field9 = 'polarityPA'
        field10 = 'subjectivityPA'
        field11 = 'polarityNB'
        field12 = 'polarity_mean'

        for row in videos_df.iterrows():
            query = """INSERT INTO %s (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) VALUES ("%s","%s","%s",%s,%s,%s, %s, %s, %s ,%s, %s);""" % (
                table, field1, field2, field3, field4, field5, field6, field8, field9, field10, field11, field12, row[1][field1], row[1][field2], row[1][field3], row[1][field4], row[1][field5] ,row[1][field6], row[1][field8] ,row[1]['polarity(PatternAnalyzer)'] ,row[1]['subjectivity(PatternAnalyzer)'], row[1]['polarity(NaiveBayes)'], row[1]['polarity_mean'])
        
            print(query)
            cursor.execute(query)
            rows_inserted += cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()
        return rows_inserted

    def insert_youtube_indicators(self, videos_df):
        """ Create DB and tables """
        conn = self.connect_mysql(self.db_name)
        cursor = conn.cursor()
        rows_inserted = 0
        table = 'youtube_video_indicators'
        field1 = 'id'
        field2 = 'zscore_p'
        field3 = 'zscore_r'
        field4 = 'zscore_VPD'
        field5 = 'zscore_LPV'
        field6 = 'zscore_DPV'
        field7 = 'zscore_polarity_mean_ci'

        for row in videos_df.iterrows():
            query = """INSERT INTO %s (%s,%s,%s,%s,%s,%s,%s) VALUES ("%s",%s,%s,%s,%s,%s,%s);""" % (
                table, field1, field2, field3, field4, field5, field6, field7, row[1][field1], row[1][field2], row[1][field3], row[1][field4], row[1][field5] ,row[1][field6], row[1]['zscore_polarity_mean(ci)'])
        
            print(query)
            cursor.execute(query)
            rows_inserted += cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()
        return rows_inserted

    def get_video_ids(self):
        """ Get video IDs from DB """
        conn = self.connect_mysql(self.db_name)
        cursor = conn.cursor()
        table = 'youtube_videos'
        field1 = 'id'
        cursor.execute("select distinct(%s) from %s" % (field1, table))
        ids = [r[0] for r in cursor.fetchall()]
        cursor.close()
        conn.close()
        return ids

    def get_videos(self):
        """ Get video IDs from DB """
        conn = self.connect_mysql(self.db_name)
        cursor = conn.cursor()
        videos = pd.DataFrame(columns=['id', 'title', 'duration', 'url', 'duration_good', 'duration_bad', 'percentage_of_good_subs', 'polarity(PatternAnalyzer)', 'subjectivity(PatternAnalyzer)', 'polarity(NaiveBayes)', 'polarity_mean'])

        table = 'youtube_videos'
        field1 = 'id'
        field2 = 'title'
        field3 = 'url'
        field4 = 'duration'
        field5 = 'duration_good'
        field6 = 'duration_bad'
        field7 = 'percentage_of_good_subs'
        field8 = 'polarityPA'
        field9 = 'subjectivityPA'
        field10 = 'polarityNB'
        field11 = 'polarity_mean'

        cursor.execute("""select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s from %s""" % (field1, field2, field3, field4, 
        field5, field6, field7, field8, field9, field10, field11, table))
        
        for r in cursor.fetchall():
            videos = videos.append({'id': r[0], 'title': r[1], 'duration': r[2],
                            'url': r[3], 'duration_good': r[4], 'duration_bad': r[5], 'percentage_of_good_subs': r[6], 'polarity(PatternAnalyzer)' : r[7], 'subjectivity(PatternAnalyzer)'
                            : r[8], 'polarity(NaiveBayes)': r[9], 'polarity_mean' : r[10]} ,  ignore_index=True)

        cursor.close()
        conn.close()
        return videos

    def get_hourly_stats(self, num_times=48, interval=3600):
        """ Get hourly stats for each video and insert them into DB """
        # Get video ids from db
        video_ids = self.get_video_ids()
        conn = self.connect_mysql(self.db_name)
        cursor = conn.cursor()
        rows_inserted = 0
        if len(video_ids) > 0:
            for i in range(0, num_times):
                try:
                    video_stats_df = yt.get_videos_duration(video_ids)
                    video_stats_df = video_stats_df[['id', 'views', 'likes', 'dislikes']]
                    video_stats_df['timestamp'] = datetime.now()
                    table = 'youtube_video_stats'
                    field1 = 'id'
                    field2 = 'timestamp'
                    field3 = 'views'
                    field4 = 'likes'
                    field5 = 'dislikes'
                    for row in video_stats_df.iterrows():
                        cursor.execute("""INSERT INTO %s (%s,%s,%s, %s, %s) VALUES ("%s","%s",%s, %s, %s);""" % (
                            table, field1, field2, field3, field4, field5,
                            row[1][field1], datetime.now(), row[1][field3], row[1][field4], row[1][field5]))
                        rows_inserted += cursor.rowcount
                    print(f"Iteration: {i}. Datetime: {datetime.now()}. Rows inserted so far: {rows_inserted}")
                    conn.commit()
                    time.sleep(interval)
                except Exception as e:
                    print("type error: " + str(e))
                    print(f"Error during request, the time is: {datetime.now}")
            cursor.close()
            conn.close()
            return rows_inserted
        else:
            return 0

    def get_video_ids_with_stats(self):
        """ Get video IDs which have stats from DB """
        conn = self.connect_mysql(self.db_name)
        cursor = conn.cursor()
        table = 'youtube_video_stats'
        field1 = 'id'
        cursor.execute("select distinct(%s) from %s" % (field1, table))
        ids = [r[0] for r in cursor.fetchall()]
        cursor.close()
        conn.close()
        return ids

    def get_video_stats(self):
        """ Get video stats from DB """
        df = pd.DataFrame(columns=['id', 'timestamp', 'views', 'likes', 'dislikes'])
        conn = self.connect_mysql(self.db_name)
        cursor = conn.cursor()
        table = 'youtube_video_stats'
        field1 = 'id'
        field2 = 'timestamp'
        field3 = 'views'
        field4 = 'likes'
        field5 = 'dislikes'
       
        # Get stats for all videos
        cursor.execute("select %s, %s, %s, %s, %s from %s" % (
                field1, field2, field3, field4, field5, table))
        for r in cursor.fetchall():
            df = df.append({'id': r[0], 'timestamp': r[1], 'views': r[2],
                            'likes': r[3], 'dislikes': r[4]}, ignore_index=True)
        cursor.close()
        conn.close()
        return df



