# Webapp Deployment Guide

The purpose of this document is to guide you in gathering the project files and deploying to AWS Elastic Beanstalk.

*This guide assumes you have completed all steps in the [Development Guide](development-guide.md)*

### Step 1: Change Directory

Navigate to the `webapp/build` folder to access the deployment script
```
cd TexasGridAnalytics-SeniorDesign/webapp/build
```

### Step 2: Create or Update `requirements.txt` by Running `build_requirements.sh`
```
./build_requirements.sh
```
This will create a file called `requirements.txt` in the `webapp` directory.

### Step 3: Create `txgridanalytics.zip` by Running `build_deployment.sh`
```
./build_deployment.sh
```
This will create a file called `txgridanalytics-DATE.zip` in the `build` directory.

### Step 4: Update your Environment Variables on AWS

Your local `.env` file contains important environment variables crucial to the success of the code. The AWS Elastic Beanstalk environment uses a configuration page on the GUI to pass these. To view and edit these on AWS:

1. Navigate to your AWS Elastic Beanstalk environment page
2. Visit the `Configuration` pane
3. In the `Software` category, choose `Edit`
4. Under `Modify Software`, the `Environment properties` section lists the environment properties

[Source](https://docs.aws.amazon.com/elasticbeanstalk/latest/dg/environments-cfg-softwaresettings.html)

### Step 5: Upload and Deploy

1. Navigate to your AWS Elastic Beanstalk environment page
2. Click on `Upload and Deploy`
3. Upload your newly-created `txgridanalytics-DATE.zip` file

![Upload and Deploy](https://i.stack.imgur.com/ZJTre.png)

### That's it! AWS Elastic Beanstalk will handle it from there :)
