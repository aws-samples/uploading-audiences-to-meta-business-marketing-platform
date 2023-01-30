#!/bin/bash
set -e

# fb
mkdir -p ./python
pip install facebook_business -t ./python
zip -r ./layer.zip ./python
rm -r ./python/