import praw
from kafka import KafkaProducer
import json
import configparser
from prawcore.exceptions import Redirect

# config=configparser.ConfigParser()
# config.read_file('reddit.config')

#Setting up the reddit API
#Reddit Variable: NEED TO CHANGE THESE TO THE CURRENT ACCOUNT
CLIENT_ID = "O7NeunECCvluYRC5Bv9ppw"
CLIENT_SECRET = "8e-mHtjlQR1hizrG_OIGtRQfDdi2IA"
USER_AGENT = "streamingScript/v1.0 by Visible-Property-621"
PASSWORD = "UtDallas2024"
USERNAME = "Visible-Property-621"

reddit = praw.Reddit(
    client_id= CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT,
    password=PASSWORD,
    username = USERNAME 
)

##verify the reddit api is working
if (reddit):
    print('Connected to reddit')
else:
    print('Not working')

#Specify the subreddit -> using r/WallStreetBets
subreddit = reddit.subreddit('WallStreetBets')

#Setting up the Kafka Producer
producer =  KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)
##Verify the the producer is working
if  producer:
    print("Kafka is connected")
else: 
    print("Failed to connect to Kafka Server")

#Stream comments from the subreddit and them to kafka
for comment in subreddit.stream.comments():
    commentStr = str(comment.body)

    if isinstance(commentStr, bytes):
        commentStr = commentStr.decode('utf-8')
    
    commentJson = {'comment':commentStr}
    producer.send('topic1', value=commentStr)
    print(f'Comment sent to Kafka: {commentStr=}')

producer.flush()