# Testing AWS Authentication

## Server Configuration

AWS authentication requires the following to be done on the server side:

1. The AWS authentication mechanism must be enabled on the server. This
is done by adding `MONGODB-AWS` to the values in `authenticationMechanisms`
server parameter.

2. A user must be created in the `$external` database with the ARN matching
the IAM user or role that the client will authenticate as.

Note that the server does not need to have AWS keys provided to it - it
uses the keys that the client provides during authentication.

An easy way to configure the deployment in the required fashion is to
configure the deployment to accept both password authentication and
AWS authentication, and add a bootstrap user:

    mlaunch init --single --auth --username root --password toor \
      --setParameter authenticationMechanisms=MONGODB-AWS,SCRAM-SHA-1,SCRAM-SHA-256 \
      --dir /tmp/db

Then connect as the bootstrap user and create AWS-mapped users:

    mongo mongodb://root:toor@localhost:27017
    
    # In the mongo shell:
    use $external
    db.createUser({
      user: 'arn:aws:iam::1234567890:user/test',
      roles: [{role:'root', db:'admin'}]})

The ARN can be retrieved from the AWS management console. Alternatively,
if the IAM user's access and secret keys are known, trying to authenticate
as the user will log the user's ARN into the server log when authentication
fails; this ARN can be then used to create the server user.

With the server user created, it is possible to authenticate using AWS.
The following example uses regular user credentials for an IAM user
created as described in the next section;

    mongo 'mongodb://AKIAAAAAAAAAAAA:t9t2mawssecretkey@localhost:27017/?authMechanism=MONGODB-AWS&authsource=$external'

To authenticate, provide the IAM user's access key id as the username and
secret access key as the password. Note that the username and the password
must be percent-escaped when they are passed in the URI as the examples here
show. Also note that the user's ARN is not explicitly specified by the client
during authentication - the server determines the ARN from the acess
key id and the secret access key provided by the client.

## Provisioning Tools

The Ruby driver includes tools that set up the resources needed to test
AWS authentication. These are exposed by the `.evergreen/aws` script.
To use this script, it must be provided AWS credentials and the region
to operate in. The credentials and region can be given as command-line
arguments or set in the environment, as follows:

    export AWS_ACCESS_KEY_ID=AKIAYOURACCESSKEY
    export AWS_SECRET_ACCESS_KEY=YOURSECRETACCESSKEY
    export AWS_REGION=us-east-1

If you also perform manual testing (for example by following some of the
instructions in this file), ensure AWS_SESSION_TOKEN is not set
unless you are intending to invoke the `.evergreen/aws` script with
temporary credentials:

    unset AWS_SESSION_TOKEN

Note that [AWS CLI](https://aws.amazon.com/cli/) uses a different environment
variable for the region - `AWS_DEFAULT_REGION` rather than `AWS_REGION`.
If you also intend to use the AWS CLI, execute:

    export AWS_DEFAULT_REGION=$AWS_REGION

To verify that credentials are correctly set in the environment, you can
perform the following operations:

    # Test driver tooling
    ./.evergreen/aws key-pairs
    
    # Test AWS CLI
    aws sts get-caller-identity

Alternatively, to provide the credentials on each call to the driver's
`aws` script, use the `-a` and `-s` arguments as follows:

    ./.evergreen/aws -a KEY-ID -s SECRET-KEY key-pairs

## Common Setup

In order to test all AWS authentication scenarios, a large number of AWS
objects needs to be configured. This configuration is split into two parts:
common setup and scenario-specific setup.

The common setup is performed by running:

    ./.evergreen/aws setup-resources

This creates resources like security groups, IAM users and CloudWatch
log groups that do not cost money. It is possible to test authentication
with regular credentials and temporary credentials obtained via an
AssumeRole request using these resources. In order to test authentication
from an EC2 instance or an ECS task, the instance and/or the task need
to be started which costs money and is performed as separate steps as
detailed below.

## Regular Credentials - IAM User

AWS authentication as a regular IAM user requires having an IAM user to
authenticate as. This user can be created using the AWS management console.
The IAM user requires no permissions, but it must have the programmatic
access enabled (i.e. have an access key ID and the secret access key).

An IAM user is created as part of the common setup described earlier.
To reset and retrieve the access key ID and secret access key for the
created user, run:

    ./.evergreen/aws reset-keys

Note that if the user already had an access key, the old credentials are
removed and replaced with new credentials.

Given the credentials for the test user, the URI for running the driver
test suite can be formed as follows:

    export "MONGODB_URI=mongodb://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY@localhost:27017/?authMechanism=MONGODB-AWS&authsource=$external"

## Temporary Credentials - AssumeRole Request

To test a user authenticating with an assumed role, you can follow
[the example provided in Amazon documentation](https://aws.amazon.com/premiumsupport/knowledge-center/iam-assume-role-cli/)
to set up the assumed role and related objects and obtain temporary credentials
or use the driver's tooling using the commands given below.
Since the temporary credentials expire, the role needs to be re-assumed
periodically during testing and the new credentials and session token retrieved.

If following the example in Amazon's documentation,
[jq](https://stedolan.github.io/jq/) can be used to efficiently place the
credentials from the AssumeRole request into the environment, as follows:

    # Call given in the example guide
    aws sts assume-role --role-arn arn:aws:iam::YOUR-ACCOUNT-ID:role/example-role --role-session-name AWSCLI-Session >~/.aws-assumed-role.json
    
    # Extract the credentials
    export AWS_ACCESS_KEY_ID=`jq .Credentials.AccessKeyId  ~/.aws-assumed-role.json -r`
    export AWS_SECRET_ACCESS_KEY=`jq .Credentials.SecretAccessKey  ~/.aws-assumed-role.json -r`
    export AWS_SESSION_TOKEN=`jq .Credentials.SessionToken ~/.aws-assumed-role.json -r`

Alternatively, the `./evergreen/aws` script can be used to assume the role.
By default, it will assume the role that `setup-resources` action configured.

Note: The ability to assume this role is granted to the
[IAM user](#regular-credentials-iam-user) that the provisioning tool creates.
Therefore the shell must be configured with credentials of the test user,
not with credentials of the master user that performed the provisioning.

To assume the role created by the common setup, run:

    ./.evergreen/aws assume-role

It is also possible to specify the ARN of the role to assume manually, if
you created the role using other means:

    ./.evergreen/aws assume-role ASSUME-ROLE-ARN

To place the credentials into the environment:

    eval $(./.evergreen/aws assume-role)
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

With the credentials in the environment, to verify that the role was assumed
and the credentials are complete and correct, perform a `GetCallerIdentity`
call:

    aws sts get-caller-identity

Given the credentials for the test user, the URI for running the driver
test suite can be formed as follows:

    export "MONGODB_URI=mongodb://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY@localhost:27017/?authMechanism=MONGODB-AWS&authsource=$external&authMechanismProperties=AWS_SESSION_TOKEN:$AWS_SESSION_TOKEN"

## Temporary Credentials - EC2 Instance Role

To test authentication [using temporary credentials for an EC2 instance
role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html),
an EC2 instance launched with an IAM role or an EC2 instance configured
with an instance profile is required. No permissions are needed for the
IAM role used with the EC2 instance.

To create an EC2 instance with an attached role using the AWS console:

1. Crate an IAM role that the instance will use. It is not necessary to
specify any permissions.
2. Launch an instance, choosing the IAM role created in the launch wizard.

To define an instance profile which allows adding and removing an IAM role
to/from an instance at runtime, follow Amazon documentation
[here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#attach-iam-role).
To test temporary credentials obtained via an EC2 instance role in Evergreen,
an instance profile must be associated with the running instance as per
this guide.

The driver provides tooling to configure a suitable instance profile and
launch an EC2 instance that can have this instance profile attached to it.

The instance profile and associated IAM role are created by the common
setup described above. To launch an EC2 instance suitable for testing
authentication via an EC2 role, run:

    ./.evergreen/aws launch-ec2 path/to/ssh.key.pub

The `launch-ec2` command takes one argument which is the path to the
public key for the key pair to use for SSH access to the instance.

This script will output the instance ID of the launched instance. The
instance initially does not have an instance profile assigned; to assign
the instance profile created in the common setup to the instance, run:

    ./.evergreen/aws set-instance-profile i-instanceid

To remove the instance profile from the instance, run:

    ./.evergreen/aws clear-instance-profile i-instanceid

To provision the instance for running the driver's test suite via Docker, run:

    ip=12.34.56.78
    ./.evergreen/provision-remote ubuntu@$ip docker

To run the AWS auth tests using the EC2 instance role credentials, run:

    ./.evergreen/test-docker-remote ubuntu@$ip \
      MONGODB_VERSION=4.4 AUTH=aws-ec2 \
      -s .evergreen/run-tests-aws-auth.sh \
      -a .env.private

Note that if if you are not using MongoDB AWS account for testing, you
would need to specify MONGO_RUBY_DRIVER_AWS_AUTH_USER_ARN in your
`.env.private` file with the ARN of the user to add to MongoDB. The easiest
way to find out this value is to run the tests and note which username the
test suite is trying to authenticate as.

To terminate the instance, run:

    ./.evergreen/aws stop-ec2

## Temporary Credentials - ECS Task Role

The basic procedure for setting up an ECS cluster is described in
[this guide](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_AWSCLI_Fargate.html).
For testing AWS auth, the ECS task must have a role assigned to it which is
covered in [this guide](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html)
and additionally [here](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_execution_IAM_role.html).

Although not required for testing AWS auth specifically, it is very helpful
for general troubleshooting of ECS provisioning to have log output from the
tasks. Logging to CloudWatch is covered by [this Amazon guide](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/QuickStartEC2Instance.html)
with these potentially helpful [additional](https://stackoverflow.com/questions/50397217/how-to-determine-the-cloudwatch-log-stream-for-a-fargate-service#50704804)
[resources](https://help.sumologic.com/03Send-Data/Collect-from-Other-Data-Sources/AWS_Fargate_log_collection).
A log group must be manually created, the steps for which are described
[here](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/Working-with-log-groups-and-streams.html).

Additional references:

- [Task definition CPU and memory values](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-task-definition.html)

The common setup creates all of the necessary prerequisites to test
authentication using ECS task credentials, which includes an empty ECS
cluster. To test authentication, a service needs to be created in the
ECS cluster that runs the SSH daemon, which can be done by running:

    ./.evergreen/aws launch-ecs path/to/ssh.key.pub

The `launch-ecs` command takes one argument which is the path to the
public key for the key pair to use for SSH access to the instance.

This script generally produces no output if it succeeds. As the service takes
some time to start, run the following command to check its status:

    ./.evergreen/aws ecs-status

The status output shows the tasks running in the ECS cluster ordered by their
generation, with the newest ones first. Event log for the cluster is displayed,
as well as event stream for the running task of the latest available generation
which includes the Docker execution output collected via CloudWatch.
The status output includes the public IP of the running task once it is
available, which can be used to SSH into the container and run the tests.

Note that when AWS auth from an ECS task is tested in Evergreen, the task is
accessed via its private IP; when the test is performed using the provisioning
tooling described in this document, the task is accessed via its public IP.

If the public IP address is in the `IP` shell variable, provision the task:

    ./.evergreen/provision-remote root@$IP local

To run the credentials retrieval test on the ECS task, execute:

    ./.evergreen/test-remote root@$IP env AUTH=aws-ecs RVM_RUBY=ruby-2.7 MONGODB_VERSION=4.4 TEST_CMD='rspec spec/integration/aws*spec.rb' .evergreen/run-tests.sh

To run the test again without rebuilding the remote environment, execute:

    ./.evergreen/test-remote -e root@$IP \
      env AUTH=aws-ecs RVM_RUBY=ruby-2.7 sh -c '\
        export PATH=`pwd`/rubies/ruby-2.7/bin:$PATH && \
        eval export `strings /proc/1/environ |grep ^AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` && \
        bundle exec rspec spec/integration/aws*spec.rb'

Note that this command retrieves the value of `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`
from the PID 1 environment and places it into the current environment prior to
running the tests.

To terminate the AWS auth-related ECS tasks, run:

    ./.evergreen/aws stop-ecs
