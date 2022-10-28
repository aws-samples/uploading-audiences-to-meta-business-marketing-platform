#!/bin/bash
set -e

# fb
pip install facebook_business -t ./layers/facebook_business
mkdir ./layers/1/
zip -r ./layers/1/facebook_business.zip ./layers/facebook_business
rm -r ./layers/facebook_business/