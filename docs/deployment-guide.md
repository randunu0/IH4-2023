# Webapp Deployment Guide

The purpose of this document is to guide you in gathering the project files and deploying to AWS Elastic Beanstalk.

*This guide assumes you have completed all steps in the [Development Guide](development-guide.md)*

### Step 1: Change Directory

Navigate to the `webapp/build` folder to access the deployment script
```
cd TexasGridAnalytics-SeniorDesign/webapp/build
```

### Step 2: Run `build_deployment.sh`
```
./build_deploy.sh
```
This will create a file called `txgridanalytics-DATE.zip` in the same directory.

### Step 3: Upload and Deploy

1. Navigate to your AWS Elastic Beanstalk application page.
2. Click on `Upload and Deploy`
3. Upload your newly-created `txgridanalytics-DATE.zip` file

![Upload and Deploy](https://i.stack.imgur.com/ZJTre.png)

### That's it! AWS Elastic Beanstalk will handle it from there :)
