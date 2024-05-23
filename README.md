# Pinterest Data Pipeline

## Table of contents

1. [The Scope](#the-scope)
2. [Overview](#overview)
3. [What it does](#what-it-does)
4. [What I used](#what-i-used)
5. [What I learned](#what-i-learned)
6. [Instructions](#instructions)

## The Scope
Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, you'll create a similar system using the AWS Cloud.

## Overview 

## What I used

### AWS services
Most of this project is built on and run through AWS services. 
Here is a brief overview of the tools used

#### EC2 
I have used an EC2 Instance to run Kafka through 

#### S3
I have used an S3 bucket to store the processed data 

#### IAM 
I have an IAM role with all the permissions created

#### MSK + MSK Connect
These have been used for kafka management through AWS

## Databricks
Through this project i have used databricks to import, manage, clean and perform analytical queries on the data.
The structure I have followed is to have a notebook for the initial configurations including mounting the S3 bucket with the data in. Next I have 3 seperated notebooks to clean each of the three data tables. 
#### Api Gateway
This is a simple tool on AWS for setting up custom APIs 

## My experience while doing this project 
#### Chapter 1 - The Setup
The setup for this project was a long process to link all the AWS softwares and get them to talk to each other. \
From installing kafka and the other necessary modules onto the EC2 machine to even setting up the key pairs to be able to connect to the EC2 machine.\
I faced a couple issues when doing the setup, including not allowing proxy on my HTTP any request but issues were quickly resolved to the point the data now flows through to the S3 bucket as it should. \
Whilst the setup for this style of pipeline is fiddly in terms of having lots of locations for different config files and settings, once you get the hang of where to look and which configs are for which service it becomes very clear and very efficient in terms of time taken to setup. 

#### Chapter 2 - Databricks
During this step I have setup my Databricks account and linked through to the AWS S3 bucket with my data that has been pushed from my user posting emulation. \
Databricks is very easy to use and being able to swap between languages means changing schemas etc in sql then switching back to python is incredible easy.\
