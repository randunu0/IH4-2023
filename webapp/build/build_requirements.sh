tput setaf 2; echo "********** creating requirements.txt **********"
tput setaf 4;tput dim; source ../venv/bin/activate
pip freeze > ../requirements.txt
tput setaf 2; echo "********** all done! **********"
tput setaf 2; echo "Generated file can be found in the webapp directory"