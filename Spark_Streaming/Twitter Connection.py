#!/usr/bin/env python
# coding: utf-8

# # Twitter Connection

# In[ ]:


import tweepy


# In[ ]:


from tweepy import OAuthHandler, Stream


# In[ ]:


from tweepy.streaming import Stream
import socket, json


# In[ ]:


consumer_key = ""  # you costumer key in twitter acount 
consumer_secret = "" #you costumer secret key in twitter acount 
acces_token = " " # you acces token in twitter acount 
acces_secret = " " # you secret acces token in twitter acount 


# In[ ]:


# create will listen to the tweet them self and create one function that send the data through a socket


# In[ ]:


class TweetListener(Stream):
    def __init__ (self,csocket):
        self.client_socket = csocket
        
    def on_data(self,data):
        
        try:
            msg = json.loads(data)
            # print the message that gonna int < text>
            print(msg["text"].encode("utf-8")) # encode the message with utf8
            #  to send the message as live Data
            self.client_socket.send(msg["text"].encode("utf-8"))
            return True
        except BaseException as e:
            print("ERRO ",e)
            return True
        
    def on_error(self, status):
        print(status)
        return True
        
        
        
        
        


# In[ ]:


# we are connecting to twitter and this twitter stream we are gonna filter everything by certain string 

def send_data(c_socket):
    auth = OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(acces_token,acces_secret)
    
    twitter_stream = Stream(aut, TweetListener(c_socket))
    twitter_stream.filter(track["guitar"]) # filter STring we will get anything about guitar (music)


# In[ ]:


if __name__ == "__main__":
    s = socket.socket()
    host = "" # your ip adress
   
    port = 8888 # you can choose another 
    s.bind((host,port))
    
    print("listening on ports  8888  ")
    s.listen(5) # listen for five minute
    c,addr = s.accept() # establish the connection with the client
    
    sendData(c)


# In[ ]:




