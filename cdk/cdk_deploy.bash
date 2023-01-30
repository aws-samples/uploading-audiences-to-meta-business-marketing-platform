#!/bin/bash
# fails and stops execution if command fails
set -e

# set env
export CDK_DEFAULT_ACCOUNT=""
export CDK_DEFAULT_REGION="us-west-2"

# python virtual envirnment
# change venv directory as desired
python3 -m venv ~/.virtualenvs/metauploads_cdk
source .virtualenvs/metauploads_cdk/bin/activate

# install cdk
# python dependencies
pip install -r requirements.txt

# cdk cli
npm install -g aws-cdk

# bootstrap
cdk bootstrap

# create lambda layers
# These are copied in to s3 during stack deployment
../assets/lambda/create_layers.bash

#deploy
cdk deploy --hotswap