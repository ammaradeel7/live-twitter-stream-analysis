# live-twitter-stream-analysis
Analysis of social media trends has many useful cases, such as targeted marketing,current affairs and their effects,psychological demography analysis and much more.Access and analysis of such data in real time, further opens doors to possibilities.
## The problem
Processing and use of the data is difficult due to the sheer amount and richness, and the velocity at which the data is recieved. 
## Proposed Solution
Apache Spark is a distributed general-purpose cluster-computing framework with easy access to parallelism in processing.The implementation is done in two modules,Th first module makes a tcp connection with twitter with the developer credentials,sends the location of which the data is required, recieves the stream and sends the data to the second module.
The second module recieves the data,parallelizes it, and sends it to the cluster for word count.The recieved count of data is stored in a dataframe,filtered and the top 10 words are displayed.
The code is easily customizable for words,hashtags,different locations,data recieve time and data recieve window.
