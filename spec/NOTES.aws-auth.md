# AWS Authentication Implementation Notes

## AWS Account

Per [its documentation](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html,
the GetCallerIdentity API call that the server makes to STS to authenticate
the user using MONGODB-AWS auth mechanism requires no privileges. This means
in order to test authentication using non-temporary credentials (i.e.,
AWS access key id and secret access key only) it is sufficient to create an
IAM user that has no permissions but does have programmatic access enabled
(i.e. has an access key id and secret access key).

## AWS Signature V4

The driver implements the AWS signature v4 internally rather than relying on
a third-party library (such as the
[AWS SDK for Ruby](https://docs.aws.amazon.com/sdk-for-ruby/v3/api/index.html))
to provide the signature implementation. The implementation is quite compact
but getting it working took some effort due to:

1. [The server not logging AWS responses when authentication fails
](https://jira.mongodb.org/browse/SERVER-46909)
2. Some of the messages from STS being quite cryptic (I could not figure out
what the problem was for either "Request is missing Authentication Token" or
"Request must contain a signature that conforms to AWS standards", and
ultimately resolved these problems by comparing my requests to those produced
by the AWS SDK).
3. Amazon's own documentation not providing an example signature calculation
that could be followed to verify correctness, especially since this is a
multi-step process and all kinds of subtle errors are possible in many of the
steps like using a date instead of a time, hex-encoding a MAC in an
intermediate step or not separating header values from the list of signed
headers by two newlines.

### Reference Implementation - AWS SDK

To see actual working STS requests I used Amazon's
[AWS SDK for Ruby](https://docs.aws.amazon.com/sdk-for-ruby/v3/api/index.html)
([API docs for STS client](https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/EC2/Client.html),
[configuration documentation](https://docs.aws.amazon.com/sdk-for-ruby/v3/developer-guide/setup-config.html))
as follows:

1. Set the credentials in the environment (note that the region must be
explicitly provided):

    export AWS_ACCESS_KEY_ID=AKIAREALKEY
    export AWS_SECRET_ACCESS_KEY=Sweee/realsecret
    export AWS_REGION=us-east-1

2. Install the correct gem and launch IRb:

    gem install aws-sdk-core
    irb -raws-sdk-core -Iaws/sts

3. Send a GetCallerIdentity request, as used by MongoDB server:

    Aws::STS::Client.new(
      logger: Logger.new(STDERR, level: :debug),
      http_wire_trace: true,
    ).get_caller_identity

This call enables HTTP request and response logging and produces output
similar to the following:

    opening connection to sts.amazonaws.com:443...
    opened
    starting SSL for sts.amazonaws.com:443...
    SSL established, protocol: TLSv1.2, cipher: ECDHE-RSA-AES128-SHA
    <- "POST / HTTP/1.1\r\nContent-Type: application/x-www-form-urlencoded; charset=utf-8\r\nAccept-Encoding: \r\nUser-Agent: aws-sdk-ruby3/3.91.1 ruby/2.7.0 x86_64-linux aws-sdk-core/3.91.1\r\nHost: sts.amazonaws.com\r\nX-Amz-Date: 20200317T194745Z\r\nX-Amz-Content-Sha256: ab821ae955788b0e33ebd34c208442ccfc2d406e2edc5e7a39bd6458fbb4f843\r\nAuthorization: AWS4-HMAC-SHA256 Credential=AKIAREALKEY/20200317/us-east-1/sts/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=6cd3a60a2d7dfba0dcd17f9c4c42d0186de5830cf99545332253a327bba14131\r\nContent-Length: 43\r\nAccept: */*\r\n\r\n"
    -> "HTTP/1.1 200 OK\r\n"
    -> "x-amzn-RequestId: c56f5d68-8763-4032-a835-fd95efd83fa6\r\n"
    -> "Content-Type: text/xml\r\n"
    -> "Content-Length: 401\r\n"
    -> "Date: Tue, 17 Mar 2020 19:47:44 GMT\r\n"
    -> "\r\n"
    reading 401 bytes...
    -> ""
    -> "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n  <GetCallerIdentityResult>\n    <Arn>arn:aws:iam::5851234356:user/test</Arn>\n    <UserId>AIDAREALUSERID</UserId>\n    <Account>5851234356</Account>\n  </GetCallerIdentityResult>\n  <ResponseMetadata>\n    <RequestId>c56f5d68-8763-4032-a835-fd95efd83fa6</RequestId>\n  </ResponseMetadata>\n</GetCallerIdentityResponse>\n"
    read 401 bytes
    Conn keep-alive
    I, [2020-03-17T15:47:45.275421 #9815]  INFO -- : [Aws::STS::Client 200 0.091573 0 retries] get_caller_identity()  

    => #<struct Aws::STS::Types::GetCallerIdentityResponse user_id="AIDAREALUSERID", account="5851234356", arn="arn:aws:iam::5851234356:user/test">

Note that:

1. The set of headers sent by the AWS SDK differs from the set
  of headers that the MONGODB-AWS auth mechanism specification mentions.
  I used the AWS SDK implementation as a guide to determine the correct shape
  of the request to STS and in particular the `Authorization` header.
  The source code of Amazon's implementation is
  [here](https://github.com/aws/aws-sdk-ruby/blob/master/gems/aws-sigv4/lib/aws-sigv4/signer.rb)
  and it generates, in particular, the x-amz-content-sha256` header
  which the MONGODB-AWS auth mechanism specification does not mention.
2. This is a working request which can be replayed, making it possible
  to send this request that was created by the AWS SDK repeatedly with minor
  alterations to study STS error reporting behavior. STS as of this writing
  allows a 15 minute window during which a request may be replayed.
3. The printed request only shows the headers and not the request body.
  In case of the GetCallerIdentity, the payload is fixed and is the same as
  what the MONGODB-AWS auth mechanism specification requires
  (`Action=GetCallerIdentity&Version=2011-06-15`).

Because the AWS SDK includes a different set of headers in its requests,
it not feasible to compare the canonical requests generated by AWS SDK
verbatim to the canonical requests generated by the driver.

### Manual Requests

It is possible to manually send requests to STS using OpenSSL `s_client`
tool in combination with the [printf](https://linux.die.net/man/3/printf)
utility to transform the newline escapes. A sample command replaying the
request printed above is as follows:

    (printf "POST / HTTP/1.1\r\nContent-Type: application/x-www-form-urlencoded; charset=utf-8\r\nAccept-Encoding: \r\nUser-Agent: aws-sdk-ruby3/3.91.1 ruby/2.7.0 x86_64-linux aws-sdk-core/3.91.1\r\nHost: sts.amazonaws.com\r\nX-Amz-Date: 20200317T194745Z\r\nX-Amz-Content-Sha256: ab821ae955788b0e33ebd34c208442ccfc2d406e2edc5e7a39bd6458fbb4f843\r\nAuthorization: AWS4-HMAC-SHA256 Credential=AKIAREALKEY/20200317/us-east-1/sts/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=6cd3a60a2d7dfba0dcd17f9c4c42d0186de5830cf99545332253a327bba14131\r\nContent-Length: 43\r\nAccept: */*\r\n\r\n" &&
      echo "Action=GetCallerIdentity&Version=2011-06-15" &&
      sleep 5) |openssl s_client -connect sts.amazonaws.com:443

Note the sleep call - `s_client` does not wait for the remote end to provide
a response before exiting, thus the sleep on the input side allows 5 seconds
for STS to process the request and respond.

For reference, Amazon provides [GetCallerIdentity API documentation
](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html).

### Integration Test - Signature Generation

The Ruby driver includes an integration test for signature generation, where
the driver makes the call to `GetCallerIdentity` STS endpoint using the
provided AWS credentials. This test is in
`spec/integration/aws_auth_request_spec.rb`.

### STS Error Responses

The error responses produced by STS sometimes do not clearly indicate the
problem. Below are some of the puzzling responses I encountered:

- *Request is missing Authentication Token*: request is missing the
  `Authorization` header, or the value of the header does not begin with
  `AWS4-`. For example, this error is produced if the signature algorithm
  is erroneously given as `AWS-HMAC-SHA256` instead of `AWS4-HMAC-SHA256`
  with the remainder of the header value being correctly constructed.
  This error is also produced if the value of the header erroneously includes
  the name of the header (i.e. the header name is specified twice in the header
  line) but the value is otherwise completely valid. This error has no relation
  to the "session token" or "security token" as used with temporary AWS
  credentials.
- *The security token included in the request is invalid*: this error can be
  produced in several circumstances:
  - When the AWS access key id, as specified in the scope part of the
    `Authorization` header, is not a valid access key id. In the case of
    non-temporary credentials being used for authentication, the error refers to
    a "security token" but the authentication process does not actually use a
    security token as this term is used in the AWS documentation describing
    temporary credentials.
  - When using temporary credentials and the security token is not provided
    in the STS request at all (x-amz-security-token header).
- *Signature expired: 20200317T000000Z is now earlier than 20200317T222541Z
  (20200317T224041Z - 15 min.)*: This error happens when `x-amz-date` header
  value is the formatted date (`YYYYMMDD`) rather than the ISO8601 formatted
  time (`YYYYMMDDTHHMMSSZ`). Note that the string `20200317T000000Z` is never
  explicitly provided in the request - it is derived by AWS from the provided
  header `x-amz-date: 20200317`.
- *The request signature we calculated does not match the signature
  you provided. Check your AWS Secret Access Key and signing method. Consult
  the service documentation for details*: this is the error produced when
  the signature is not calculated correctly but everything else in the
  request is valid. If a different error is produced, most likely the problem
  is in something other than signature calculation.
- *The security token included in the request is expired*: this error is
  produced when temporary credentials are used and the credentials have
  expired.

See also [AWS documentation for STS error messages](https://docs.aws.amazon.com/STS/latest/APIReference/CommonErrors.html).

### Resources

Generally I found Amazon's own documentation to be the best for implementing
the signature calculation. The following documents should be read in order:

- [Signing AWS requests overview](https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html)
- [Creating canonical request](https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html)
- [Creating string to sign](https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html)
- [Calculating signature](https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html)

### Signature Debugger

The most excellent [awssignature.com](http://www.awssignature.com/) was
indispensable in debugging the actual signature calculation process.

### MongoDB Server

MongoDB server internally defines the set of headers that it is prepared to
handle when it is processing AWS authentication. Headers that are not part
of that set cause the server to reject driver's payloads.

The error reporting when additional headers are provided and when the
correct set of headers is provided but the headers are not ordered
lexicographically [can be misleading](https://jira.mongodb.org/browse/SERVER-47488).

## Direct AWS Requests

[STS GetCallerIdentity API docs](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetCallerIdentity.html)

When making direct requests to AWS, adding `Accept: application/json`
header will return the results in the JSON format, including the errors.

## AWS CLI

[Configuration reference](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)

Note that AWS CLI uses `AWS_DEFAULT_REGION` environment variable to configure
the region used for operations.

## AWS Ruby SDK

[Configuration reference](https://docs.aws.amazon.com/sdk-for-ruby/v3/developer-guide/setup-config.html)

Note that AWS Ruby SDK uses `AWS_REGION` environment variable to configure
the region used for operations.

[STS::Client#assume_role documentation](https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/STS/Client.html#assume_role-instance_method)

## IMDSv2

`X-aws-ec2-metadata-token-ttl-seconds` is a required header when using
IMDSv2 EC2 instance metadata requests. This header is used in the examples
on [Amazon's page describing
IMDSv2](https://aws.amazon.com/blogs/security/defense-in-depth-open-firewalls-reverse-proxies-ssrf-vulnerabilities-ec2-instance-metadata-service/),
but is not explicitly stated as being required.

Not providing this header fails the PUT requests with HTTP code 400.

## IAM Roles For EC2 Instances

### Metadata Rate Limit

[Amazon documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html#instancedata-throttling)
states that the EC2 instance metadata endpoint is rate limited. Since the
driver accesses it to obtain credentials whenever a connection is established,
rate limits may adversely affect the driver's ability to establish connections.

### Instance Profile Assignment

It can take over 5 seconds for an instance to see its instance profile change
reflected in the instance metadata. Evergreen test runs seem to experience
this delay to a significantly larger extent than testing in a standalone
AWS account.

## IAM Roles For ECS Tasks

### ECS Task Roles

When an ECS task (or more precisely, the task definition) is created,
it is possible to specify an *execution role* and a *task role*. The two are
completely separate; an execution role is required to, for example, be
able to send container logs to CloudWatch if the container is running in
Fargate, and a task role is required for AWS authentication purposes.

The ECS task role is also separate from EC2 instance role and the IAM role
for a user to assume a role - these roles all require different configuration.

### `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` Scope

As stated in [this Amazon support document](https://aws.amazon.com/premiumsupport/knowledge-center/ecs-iam-task-roles-config-errors/),
the `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` environment variable is only
available to the PID 1 process in the container. Other processes need to
extract it from PID 1's environment:

    strings /proc/1/environment

### Other ECS Metadata

`strings /proc/1/environment` also shows a number of other enviroment
variables available in the container with metadata. For example a test
container yields:

    HOSTNAME=f893c90ec4bd
    ECS_CONTAINER_METADATA_URI=http://169.254.170.2/v3/5fb0b11b-c4c8-4cdb-b68b-edf70b3f4937
    AWS_DEFAULT_REGION=us-east-2
    AWS_EXECUTION_ENV=AWS_ECS_FARGATE
    AWS_REGION=us-east-2
    AWS_CONTAINER_CREDENTIALS_RELATIVE_URI=/v2/credentials/f17b5770-9a0d-498c-8d26-eea69f8d0924

### Metadata Rate Limit

[Amazon documentation](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/troubleshoot-task-iam-roles.html)
states that ECS task metadata endpoint is subject to rate limiting,
which is configured via [ECS_TASK_METADATA_RPS_LIMIT container agent
parameter](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-agent-config.html).
When the rate limit is reached, requests fail with `429 Too Many Requests`
HTTP status code.

Since the driver accesses this endpoint to obtain credentials whenever
a connection is established, rate limits may adversely affect the driver's
ability to establish connections.
