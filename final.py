from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
import pymongo
import psycopg2
import streamlit as st
import time

# Mongodb connectivity
mongo_client=pymongo.MongoClient("mongodb://vinoth:vinothvk@ac-8om7l8w-shard-00-00.srdizaf.mongodb.net:27017,ac-8om7l8w-shard-00-01.srdizaf.mongodb.net:27017,ac-8om7l8w-shard-00-02.srdizaf.mongodb.net:27017/?ssl=true&replicaSet=atlas-1coxpo-shard-0&authSource=admin&retryWrites=true&w=majority")
db = mongo_client['youtube_data']
col=db["channel1"]

# CONNECTING WITH MYSQL DATABASE
sql = psycopg2.connect(host='localhost',user='postgres',password=54321,database='channel_1')
mycursor = sql.cursor()

# BUILDING CONNECTION WITH YOUTUBE API
api_key='AIzaSyDN-urSoTBSvFoYo12n27R6NLUBBIRgNa0'
channel_id ='UCRWbrHSHcFui6N9fh2JKjWA'
youtube = build('youtube', 'v3', developerKey=api_key)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_data(channel_id):
    channel_data = []
    request = youtube.channels().list(
        id=channel_id,
        part='snippet,statistics,contentDetails'
    )
    response = request.execute()

    for i in response["items"]:
        data = {"Channel_Name": i['snippet']['title'],
                "Channel_Id": i['id'],
                "Subscription_Count": i['statistics']['subscriberCount'],
                "Channel_Views": i['statistics']['viewCount'],
                "Channel_Description": i['snippet']['description'],
                "Playlist_Id": i['contentDetails']['relatedPlaylists']['uploads'],
                "Total_video_count": i['statistics']['videoCount']}
        channel_data.append(data)
        return (channel_data)

#get playlist id
playlist_Id=get_channel_data(channel_id)[0]['Playlist_Id']

#fuction to get playlist details
def get_playlist_data(channel_id):
    playlist = []
    next_page_token = None

    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )

        response = request.execute()

        for item in response['items']:
            playlist_details = {'Playlist_Id': item['id'],
                                'channel_id': item['snippet']['channelId'],
                                'Playlist_name': item['snippet']['title']}

            playlist.append(playlist_details)

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return playlist

#FUNCTION TO GET VIDEO IDS
def get_video_ids(playlist_Id):
    video_ids = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_Id,
            maxResults=50,
            pageToken=next_page_token)
        
        response = request.execute()
        playlist_items = response['items']

        for playlist_item in playlist_items:
            video_id = playlist_item['contentDetails']['videoId']
            video_ids.append(video_id)

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break
    return video_ids

video_ids=get_video_ids(playlist_Id)

#fuction to get video details
def get_video_details1(video_ids):
    video_data = []
    next_page_token = None
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50]),
            pageToken=next_page_token
        )
        response = request.execute()

        for video in response["items"]:
            published_date_str = video["snippet"]["publishedAt"]
            published_date = datetime.strptime(published_date_str, '%Y-%m-%dT%H:%M:%SZ')
            formatted_published_date = published_date.strftime('%Y-%m-%d %H:%M:%S')

            snippet = video.get("snippet", {})
            statistics = video.get("statistics", {})
            content_details = video.get("contentDetails", {})

            duration = content_details.get("duration", "")
            duration = duration[2:]  # Remove "PT" from the beginning 

            hours = 0
            minutes = 0
            seconds = 0
            
            if 'H' in duration:
                hours_index = duration.index('H')
                hours = int(duration[:hours_index])
                duration = duration[hours_index + 1:]

            if 'M' in duration:
                minutes_index = duration.index('M')
                minutes = int(duration[:minutes_index])
                duration = duration[minutes_index + 1:]

            if 'S' in duration:
                seconds_index = duration.index('S')
                seconds = int(duration[:seconds_index])

            duration_formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            video_info = {
                "Channel_name": video['snippet']['channelTitle'],
                "Video_id": video["id"],
                "Playlist_Id":playlist_Id,
                "video_title": snippet["title"],
                "description": snippet["description"],
                "publishedAt": formatted_published_date,
                "viewCount": int(statistics.get("viewCount", 0)),
                "likeCount":int (statistics.get("likeCount", 0)),
                "dislikeCount": int(statistics.get("dislikeCount", 0)),
                "favoriteCount": int(statistics.get("favoriteCount", 0)),
                "commentCount": int(statistics.get("commentCount", 0)),
                "duration": duration_formatted,
                "thumbnail_url": snippet["thumbnails"]["default"]["url"],
                "caption_status": content_details.get("caption", "")

            }
            video_data.append(video_info)
        next_page_token = response.get("nextPageToken")
    return video_data

#fuction to get comments details
def get_comment_data(video_ids):
    comments_data = []
    for ids in video_ids:
        try:
            video_data_request = youtube.commentThreads().list(
                part="snippet",
                videoId=ids,
                maxResults=50).execute()
            video_info = video_data_request['items']
            for comment in video_info:
                comment_info = {
                    'Video_id': comment['snippet']['videoId'],
                    'Comment_Id': comment['snippet']['topLevelComment']['id'],
                    'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published_At': comment['snippet']['topLevelComment']['snippet']['publishedAt']}
                comments_data.append(comment_info)
        except:
            comments = {"comments": None}
    return comments_data

#get_comment_data(video_ids)

channel_dct=get_channel_data(channel_id),
playlist_dct=get_playlist_data(channel_id),
video_dct=get_video_details1(video_ids),
comment_dct=get_comment_data(video_ids)

#Merging All Deatils
data={"channel_dct":get_channel_data(channel_id),
      "playlist_dct":get_playlist_data(channel_id),
      "video_dct":get_video_details1(video_ids),
      "comment_dct":get_comment_data(video_ids)}



# streamlit
#with st.sidebar:
add_radio = st.sidebar.radio(" youtube data harvesting and warehousing project",
        ("Home","Extract & Transfer","queries")
    )

# HOME PAGE
if add_radio == "Home":   
                        st.title(":blue[Project title]: youtube data harvesting and warehousing project")
                        st.title(":blue[Domain] : Social Media")
                        st.title(":blue[Technologies used] : Python,MongoDB, Youtube Data API, postgres, Streamlit")
                        st.title(":blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a postgres,then querying the data and displaying it in the Streamlit app")

# EXTRACT AND TRANSFORM PAGES
if add_radio == "Extract & Transfer":
    tab1,tab2 = st.tabs(["Extract","transfer"])

# EXTRACT TAB
    with tab1:
        st.write("### Enter YouTube Channel_ID  :")
        channel_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > ctrl+F>?channel_id>copy channel_id").split(',')

        if channel_id and st.button("Extract Data"):
             channel_dct=get_channel_data(channel_id)
             st.write(f'#### Extracted data from :blue["{channel_dct[0]["Channel_Name"]}"] channel')
             st.table(channel_dct)
             st.success("extracted successful !!",icon="✅")
             st.balloons()

        if st.button("Upload to MongoDB"):
           with st.spinner('Please Wait a sec...'):
                time.sleep(1)
                col.insert_one(data)
                st.success("Upload to MongoDB successful !!")
                st.balloons()        
                Ch_name=get_channel_data(channel_id)[0]['Channel_Name']

#TRANSFORM TAB
                with tab2:
                    st.markdown("#   ")
                    st.markdown("### Select a channel to begin Transformation to postgres")

                    option = st.selectbox("Select channel",(Ch_name,"others"))
                    st.write('You selected:', option)
                    if st.button("Submit"):
                        try:
                            st.success("Transformation to Post Successful !!",icon="✅")
                            st.balloons()
                        except:
                                st.error("Channel details already transformed !!",icon="🚨")

        def create_sqltables():
             mycursor.execute("create table channel (Channel_Name VARCHAR(255),Channel_Id VARCHAR(255),Subscription_Count INT,Channel_Views INT,Channel_Description TEXT,Playlist_Id VARCHAR(255),Total_video_count INT );")
             sql.commit()
             mycursor.execute("create table playlist (Playlist_ID VARCHAR(255),channel_id VARCHAR(255),Playlist_name VARCHAR(255));")
             sql.commit()
             mycursor.execute("create table comments (Video_id VARCHAR(255),Comment_Id VARCHAR(255),Comment_Text Text,Comment_Author VARCHAR(255),Comment_Published_At TIMESTAMP);")
             sql.commit()
             mycursor.execute("create table video (Channel_name VARCHAR(255),Video_id VARCHAR(255),Playlist_Id VARCHAR(255),video_title VARCHAR(255),description TEXT,publishedAt TIMESTAMP,viewCount INTEGER,likeCount INTEGER,dislikeCount INTEGER,favoriteCount INTEGER,commentCount INTEGER,duration INTERVAL,thumbnail_url VARCHAR(255),caption_status VARCHAR(255))")
             sql.commit()

        def insert_into_channel():
             video_data = tuple(data["channel_dct"][0].values())
             query = "insert into channel (Channel_Name,Channel_Id,Subscription_Count,Channel_Views,Channel_Description,Playlist_Id,Total_video_count) values(%s,%s,%s,%s,%s,%s,%s)"
             mycursor.execute(query,video_data)
             sql.commit()

        def insert_into_playlist():
             query = "insert into playlist(Playlist_ID,channel_id,Playlist_name) values(%s,%s,%s)"
             for i in playlist_dct[0]:
                  mycursor.execute(query,tuple(i.values()))
                  sql.commit()

        def insert_into_comments():
             comment_dit=pd.DataFrame(comment_dct)
             cmt_data=[tuple(row) for row in comment_dit.itertuples(index=False)]
             query = "insert into comments(Video_id,Comment_Id,Comment_Text,Comment_Author,Comment_Published_At) values(%s,%s,%s,%s,%s)"
             mycursor.executemany(query,cmt_data)
             sql.commit()

        def insert_into_table():
             query = "insert into video (Channel_name,Video_id,Playlist_Id,video_title,description,publishedAt,viewCount,likeCount,dislikeCount,favoriteCount,commentCount,duration,thumbnail_url,caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
             for i in video_dct[0]:
                 mycursor.execute(query,tuple(i.values()))
                 sql.commit()
        
       

# VIEW PAGE
if add_radio == "queries":
     
    st.write("## :blue[Select any question ]")
    questions = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
            query_1 = "select channel.channel_name  , video.video_title from channel inner join video on  channel.playlist_Id = video.playlist_Id order by channel.channel_name"
            mycursor.execute(query_1)
            result_1 = [i for i in mycursor.fetchall()]
            dataframe_1=pd.DataFrame(result_1, columns=["Channel Names", "Video Title"], index=range(1, len(result_1) + 1))
            st.write(dataframe_1)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            query_2 = "select Channel_Name,Total_video_count from channel order by Total_video_count desc"
            mycursor.execute(query_2)
            result_2 = [i for i in mycursor.fetchall()]
            dataframe_2 = pd.DataFrame(result_2, columns=["Channel Names", "Total Videos"], index=range(1, len(result_2) + 1))
            st.write(dataframe_2)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            query_3 = "select Channel_name ,video_title from video order by viewCount desc limit 10"
            mycursor.execute(query_3)
            result_3 = [i for i in mycursor.fetchall()]
            dataframe_3=pd.DataFrame(result_3, columns=['Channels', 'Video Title'], index=range(1, len(result_3) + 1))
            st.write(dataframe_3)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            query_4 = "select video_title ,commentCount from video  order by commentCount desc"
            mycursor.execute(query_4)
            result_4 = [i for i in mycursor.fetchall()]
            dataframe_4=pd.DataFrame(result_4, columns=["Video Title", "Comments count"], index=range(1, len(result_4) + 1))
            st.write(dataframe_4)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            query_5 = "select channel_name ,video_title,likeCount from video order by likeCount desc "
            mycursor.execute(query_5)
            result_5 = [i for i in mycursor.fetchall()]
            dataframe_5 = pd.DataFrame(result_5, columns=["Channel Names", "Video Title","likeCount"], index=range(1, len(result_5) + 1))
            st.write(dataframe_5)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            query_6 = "select video_title , likeCount, dislikeCount  from video  order by likeCount desc "
            mycursor.execute(query_6)
            result_6 = [i for i in mycursor.fetchall()]
            dataframe_6=pd.DataFrame(result_6, columns=["video_title", "likeCount", "dislikeCount"], index=range(1, len(result_6) + 1))
            st.write(dataframe_6)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            query_7 = "select channel_name , Channel_Views from channel order by Channel_Views desc"
            mycursor.execute(query_7)
            result_7 = [i for i in mycursor.fetchall()]
            dataframe_7 =pd.DataFrame(result_7, columns=["channel_name", "Channel_Views"], index=range(1, len(result_7) + 1))
            st.write(dataframe_7)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
            query_8 = "select channel_name , publishedAt from video where extract(year from publishedAt )= 2022 order by channel_name "
            mycursor.execute(query_8)
            result_8 = [i for i in mycursor.fetchall()]
            dataframe_8=pd.DataFrame(result_8, columns=["channel_name", "publishedAt"], index=range(1, len(result_8) + 1))
            st.write(dataframe_8)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            #query_9 = "select Channel_name,time_format(sec_to_time(avg(time_to_sec(time(duration)))),'%H:%i:%s') from video group by Channel_name order by 2 desc"
            #query_9 = "select Channel_name,from_unixtime(duration,'%h:%I:%s') from video group by Channel_name order by 2 desc"
            #mycursor.execute(query_9)
            #result_9 = mycursor.fetchall()
            #st.write(result_9)
            #dataframe_9=pd.DataFrame(result_9, columns=["Channel name", "average_duration"], index=range(1, len(result_9) + 1))
            #st.write(dataframe_9)
            mycursor.execute("""SELECT Channel_name, AVG(duration) AS average_duration FROM video GROUP BY Channel_name""")
            result_9 = mycursor.fetchall()
            result_9 = [(Channel_name, str(average_duration)) for Channel_name, average_duration in result_9]
            column_names = [desc.name for desc in mycursor.description]
            df = pd.DataFrame(result_9, columns=column_names, index=range(1, len(result_9) + 1))
            st.write(df)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            query_10 = "select Channel_name , video_title , commentCount from video order by commentCount desc"
            mycursor.execute(query_10)
            result_10 = [i for i in mycursor.fetchall()]
            dataframe_10=pd.DataFrame(result_10, columns=["Channel Names", "Video Title", "commentCount"], index=range(1, len(result_10) + 1))
            st.write(dataframe_10)








                    


