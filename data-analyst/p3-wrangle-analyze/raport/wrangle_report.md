# Data Wrangling Report

## Introduction

This project focuses on data wrangling on pre-gathered datasets of a Twitter user @dog_rates.

Datasets
- twitter_archive_enhanced.csv. It contains 2356 tweets with 17 columns of features about that tweet.
- image_predition.tsv. This file contains 3 possible predictions of the breed type of an image. 
- tweet_json.jsonl. contains raw tweets taken from Twitter's API

## Assessing Data
Examining the dataset revealed some issues.

Quality Issues
- Data type issues. timestamp is not datetime formatted, Id could be string, float Ids could be used as string id.
- Missing values not represented uniformly. Some rows have np.nan, some python None
- The source has HTML tags it could be cleaned and made categorical
- Date parsed as rating numerator
- Dog names have a large number of incorrect values (like "a", "an" the", "my")
- Unused columns could be dropped
- Rating denominator increases with the number of dogs in the picture. (like 120 if there are 12 dogs). 
- Image predictions column names not clear


Tidiness Issues
- Dog types could be in one categorical column 
- 3 dataset could be merged into one
- Expanded URLs of a row might have an array of URLs. if it does all the URLs are the same. they should have been "...photo/2, ...photo/3". We can either fix it or remove this - column and keep only the number of extended URLs. We could reconstruct the URL from tweet id
- Image prediction could return only one breed. the most confident one.

## Iterating on data gathering

Iterating on data gathering

I decided to iterate on the data-gathering phase from the beginning. This means gathering up-to-date data from Twitter and making the preprocessing steps that Udacity already provided in the other datasets.

I followed these steps.
- Download twitter data using the twint project
- Use spacy NER, regex rules, and dog name dictionary to parse dog names from the tweet.
- Parse dog stage and ratings from text
- Create a dog breed classifier model and prepare predictions
- Examine for quality issues and clean the datasets
- Merge new datasets into a table and analyse

During this iteration, there were new Issues,

- null values are parsed as empty strings, arrays
- multiple representations of date and user field
- all True / False columns
- unused columns and when they exist their rows (quote and retweets)

## Result

- resulting table has 3273 rows and 11 columns
- contains data from 2016 to 2021

![table](images/result_table.png)