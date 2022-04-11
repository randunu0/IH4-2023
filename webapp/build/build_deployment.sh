tput setaf 2; echo "********** zipping files **********"
tput setaf 4;tput dim; zip -r txgridanalytics-$(date +"%Y-%m-%d").zip ../application.py ../templates ../resources ../assets ../requirements.txt
tput setaf 2; echo "********** all done! **********"
tput setaf 2; echo "Generated file can be found in the build directory"
echo "upload and deploy at https://us-east-2.console.aws.amazon.com/elasticbeanstalk/home?region=us-east-2#/environment/dashboard?applicationName=texas-grid-analytics&environmentId=e-ebcpgk42vr"