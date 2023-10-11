# MongoDB FaaS Test Application for AWS Lambda

This folder contains source code and supporting files for a serverless test application to be run via AWS Lambda.

For information about how this test application is intended to be used, please see: https://github.com/mongodb/specifications/blob/master/source/faas-automated-testing/faas-automated-testing.rst#implementing-automated-faas-tests


## Running Locally

To run this locally, follow the instructions in the link above. If you aren't running an x86-64 architecture locally (e.g. Apple M1, etc.) you will need to bundle the gems via Docker so that gems with native components (e.g. BSON-Ruby) are built for the appropriate architecture.

The included `bundle-gems.sh` script is intended to help with this. To use it, change to the `mongodb` subdirectory, and then invoke the helper script:

~~~
faas/app/aws_lambda/mongodb $ ../bundle-gems.sh
~~~

This will invoke `bundle install` via a Docker container and create a `vendor` subdirectory. Once that is done, return to the `faas/app/aws_lambda` folder and run `sam build` and `sam invoke` (as described in the `faas-automated-testing` specification).
