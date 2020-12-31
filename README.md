# data-modeling-postgreSQL

## Introduction
Sparkify is a startup music company and want to design a database that could help them understand what kind of music their user is listening to. 

## Data Description 
We have two type of json file here, one is song information and other is log information, they're both in json format. 
* songs: JSON files with data such as song title, artist name,song duration,year etc.
* logs: JSON files with data such as user log information and date time information. 

## Star Schema 
Fact Table: songplay
Dimension Table: users,songs,songsplay,time 

