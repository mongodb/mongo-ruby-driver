# Test Users

The Mongo Ruby Driver tests assume the presence of two `Mongo::Auth::User` objects:
`root_user` and `test_user`. This document details the roles and privileges granted
to those users as well as how they are created and used in the tests.

Both users are defined in the [spec_config](support/spec_config.rb#L376) file.

## root_user
`root_user` is the test user with the most privileges. It is created with the following roles:
- userAdminAnyDatabase
- dbAdminAnyDatabase
- readWriteAnyDatabase
- clusterAdmin

By default, `root_user` is given a username of `root-user` and a password of `password`.
However, you may override these defaults by specifying a username and password in the
`MONGODB_URI` environment variable while running your tests. For example, if you set `MONGODB_URI` to: `mongodb://alanturing:enigma@localhost:27017/`, the username of `root_user` would be set to `alanturing`, and the password would be set to `enigma`.

## test_user
`test_user` is the user created with a more limited set of privileges. It is created with the following
roles:
- readWrite on the ruby-driver database
- dbAdmin on the ruby-driver database

It is also granted the following roles against a database called "invalid_database." These permissions are used for the purpose of running tests against a database that doesn't exist.
- readWrite on the invalid_database database
- dbAdmin on the invalid_database database

`test_user` also has the following roles, which are exclusively used to test transactions:
- readWrite on the hr database
- dbAdmin on the hr database
- readWrite on the reporting database
- dbAdmin on the reporting database

The `test_user` has the username `test-user` and the password `password`; these values are not customizable without changing the source code.

## User Creation

Both users are typically created in the [spec_setup](support/spec_setup.rb) script, which can be
run in two ways: either by running `bundle exec rake spec:prepare`, which only runs spec setup without
running any actual tests, or by running `rake`, which runs spec setup and the entire test suite.

First, the `spec_setup` script attempts to create the `root_user`. If this user already exists (for example,
if you have already created this user in your test instance), `spec_setup` will skip this step. Once
the script has verified the existence of `root_user`, it will create a client authenticated with the `root_user` and use that client to create a second user, `test_user`. Because `root_user` has the `userAdminAnyDatabase` role, it has the permissions necessary to create and destroy users on your MongoDB instance. If you have already created a user with the same credentials as `test_user` prior to running
the `spec_setup` script, the script will delete this user and re-create it.

The `root_user` is created in the `admin` database, while the `test_user` is created in the `ruby-driver`
database.

The authentication mechanism used to store the user credentials is going to change depending on the version of MongoDB running on your deployment. If you are running tests against a MongoDB instance with a server version older than 3.0, the users will be created using the `MONGODB-CR` authentication mechanism. If your server version is between 3.0 and 3.6 (inclusive), the test users will be created using the `SCRAM-SHA-1` mechanism, which was introduced as the new default starting in MongoDB version 3.0. If you are running a version of MongoDB newer than 4.0, test users will be authenticated using either `SCRAM-SHA-1` or `SCRAM-SHA-256`.

**Note:** (m-launch)[http://blog.rueckstiess.com/mtools/mlaunch.html], the client tool we use to spin up MongoDB instances for our tests, creates users EXCLUSIVELY with the `SCRAM-SHA-1` mechanism, even when `SCRAM-SHA-256` is enabled on the test server. This should not impact your ability to run the Mongo Ruby Driver test suite.

## Test Usage

`root_user` is used in the Mongo Ruby Driver tests to perform functionality that requires its high-level
roles and privileges (if your client is set up with authentication), such as creating and destroying users and database administration. To easily set up a `Mongo::Client` object authenticated with the roles and privileges of `root_user`, you can initialize a client using the `ClientRegistry` module as follows:

```
client = ClientRegistry.instance.global_client('root_authorized')
```

Of course, not every test will require you to create a client with so many privileges. Often, it is enough
to have a user who is only authorized to read and write to a specific test database. In this case, it is preferable to use `test_user`. To initialize a `Mongo::Client` object authenticated with the `test_user` object, use the `ClientRegistry` module as follows:

```
client = ClientRegistry.instance.global_client('authorized')
```

Once you have initialized these client objects, you may use them to perform functionality required by your tests.
