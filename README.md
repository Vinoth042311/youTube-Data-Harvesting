#YouTube Data- harvesting

##Introduction
This project is a YouTube API harvesting that allows users to retrieve and analyze data from YouTube
channels. It utilizes the YouTube Data API to fetch information such as channel statistics, video details,
comments, and more. The scrapper provides various functionalities to extract and process YouTube data
for further analysis and insights.

##Features
The YouTube Data harvesting offers a range of features to help you extract and analyze data from
YouTube. Some of the key features include:

Retrieve channel statistics: Get detailed information about YouTube channels, including subscriber
count, view count, video count, and other relevant metrics.

Fetch video details: Extract data such as video title, description, duration, view count, like count, dislike
count, and publish date for individual videos.

Analyze comments: Retrieve comments made on YouTube videos and perform analysis, such as
sentiment analysis or comment sentiment distribution.

Generate reports: Generate reports and visualizations based on the collected data, allowing users to
gain insights into channel performance, video engagement, and audience interaction.

Data storage: Store the collected YouTube data in a database for easy retrieval and future reference.

##Technologies Used
Python: The project is implemented using the Python programming language.

YouTube Data API: Utilizes the official YouTube Data API to interact with YouTube’s platform and
retrieve data.

Streamlit: The user interface and visualization are created using the Streamlit framework, providing a
seamless and interactive experience.

MongoDB Atlas: The collected data can be stored in a MongoDB Atlas database for efficient data
management and querying.

PostgreSQL: A powerful open-source relational database management system used to store and
manage the retrieved data.

PyMongo: A Python library that enables interaction with MongoDB, a NoSQL database. It is used for
storing and retrieving data from MongoDB in the YouTube Data harvesting.

Psycopg2: A PostgreSQL adapter for Python that allows seamless integration between Python and
PostgreSQL. It enables the YouTube Data Scraper to connect to and interact with the PostgreSQL
database.

Pandas: A powerful data manipulation and analysis library in Python. Pandas is used in the YouTube
Data Scraper to handle and process data obtained from YouTube, providing functionalities such as data
filtering, transformation, and aggregation.

##Process Flow
Obtain YouTube API credentials: Visit the Google Cloud Console.

Create a new project or select an existing project.

Enable the YouTube Data API v3 for your project.

Create API credentials for YouTube API v3.

##ETL Process
Extracting Data from YouTube API.

Transforming data into required format.

Loading Data into SQL

Input the Channel Id and click on Get Channel Statistics in order to retrive data from YouTube API.

Next click on Push to MongoDB to store data in MongoDB Lake.

Select a channel name from the dropdown Channel Details and click on Push to SQL to import data into
PostgreSQL.

Once imported, you can select the Analysis and Reports Page from the drop down to get a detailed
analysis of the collected data.
