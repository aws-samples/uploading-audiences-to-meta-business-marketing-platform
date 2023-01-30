#!/bin/bash
# generates cdk nag output of the cdk stack includes below line
# in the init method of the class
#        Aspects.of(self).add(cdk_nag.AwsSolutionsChecks())
# cd cdk
# cdk synth
# cd ..

# runs bandit for python vulnerabilities
echo "**********"
echo "cleanroom-activation-meta-normalize-scriptonly.py"
echo "**********"
bandit ./assets/glue/cleanroom-activation-meta-normalize-scriptonly.py
echo "**********"
echo "send_conversion_events.py"
echo "**********"
bandit ./assets/lambda/meta_conversions/send_conversion_events.py
echo "**********"
echo "app.py"
echo "**********"
bandit ./cdk/app.py
echo "**********"
echo "cdk_stack.py"
echo "**********"
bandit ./cdk/cdk/cdk_stack.py