## Texas Grid and Market Analytics
# Deployment Notes



### Frontend
___
The frontend of this web application is built using React.js and hosted on an Amazon AWS S3 Bucket within the grid-analytics.ece.utexas.edu tenant.

- Within the `webapp/frontend` folder, run `npm build` to compile your React.js webapp into the `webapp/frontend/build` folder
- Create a new S3 Bucket or locate the one you wish to use
    - ensure this bucket has `Grant public read access to this bucket` enabled
- Inside your bucket, click *Properties* and enable *Static Website Hosting*
- To deploy, upload your `build` folder to the bucket using either the AWS CLI or the web interface uploader

Source: https://medium.com/serverlessguru/deploy-reactjs-app-with-s3-static-hosting-f640cb49d7e6

### Backend

- To be decided.



*Last modified on 11/18/2021*