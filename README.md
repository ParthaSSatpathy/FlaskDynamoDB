# FlaskDynamoDB
An example of Python Flask and DynamoDB

## Steps to follow
Follow similar steps as in the course and add below ones:
1. EC2:
    - While creating EC2, make sure to update or add security group to have the port 5000 open
    - install Python3: `sudo yum install python36 python36-pip`
    - clone the git folder: `git clone https://github.com/SMPParthaS/FlaskDynamoDB.git`
    - cd to FlaskDynamoDB folder and create a virtual environment: `virtualenv -p python3 venv` and then `source venv/bin/activate` to activate virtual env
    - Install Packages: `pip install -r requirements.txt`
    - Start the server: `python3 app.py`
    - Default region is `us-east-1`, if you need to change it, you can change it in the `app.py` file `Line 5`
