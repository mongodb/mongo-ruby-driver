# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module SASLPlainTests

  # Tests for the PLAIN (LDAP) Authentication Mechanism.
  #
  # Note: These tests will be skipped automatically unless the test environment
  # has been configured.
  #
  # In order to run these tests, set the following environment variables:
  #
  #   export MONGODB_SASL_HOST='server.domain.com'
  #   export MONGODB_SASL_USER='application%2Fuser%40example.com'
  #   export MONGODB_SASL_PASS='password'
  #
  #   # optional (defaults to '$external')
  #   export MONGODB_SASL_SOURCE='source_database'
  #
  if ENV.key?('MONGODB_SASL_HOST') && ENV.key?('MONGODB_SASL_USER') && ENV.key?('MONGODB_SASL_PASS')

    def test_plain_authenticate
      replica_set = @client.class.name == 'Mongo::MongoReplicaSetClient'

      # TODO: Remove this once we have a replica set configured for SASL in CI
      return if ENV.key?('CI') && replica_set

      host   = replica_set ? [ENV['MONGODB_SASL_HOST']] : ENV['MONGODB_SASL_HOST']
      client = @client.class.new(host)
      source = ENV['MONGODB_SASL_SOURCE'] || '$external'
      db     = client['test']

      # should successfully authenticate
      assert db.authenticate(ENV['MONGODB_SASL_USER'], ENV['MONGODB_SASL_PASS'], true, source, 'PLAIN')
      assert client[source].logout

      # should raise on missing password
      ex = assert_raise Mongo::MongoArgumentError do
        db.authenticate(ENV['MONGODB_SASL_USER'], nil, true, source, 'PLAIN')
      end
      assert_match /username and password are required/, ex.message

      # should raise on invalid password
      assert_raise Mongo::AuthenticationError do
        db.authenticate(ENV['MONGODB_SASL_USER'], 'foo', true, source, 'PLAIN')
      end
    end

    def test_plain_authenticate_from_uri
      source = ENV['MONGODB_SASL_SOURCE'] || '$external'

      uri    = "mongodb://#{ENV['MONGODB_SASL_USER']}:#{ENV['MONGODB_SASL_PASS']}@" +
               "#{ENV['MONGODB_SASL_HOST']}/some_db?authSource=#{source}" +
               "&authMechanism=PLAIN"

      client = @client.class.from_uri(uri)
      db     = client['test']

      # should be able to checkout a socket (authentication gets applied)
      assert socket = client.checkout_reader(:mode => :primary)
      client[source].logout(:socket => socket)
      client.checkin(socket)

      uri = "mongodb://#{ENV['MONGODB_SASL_USER']}@#{ENV['MONGODB_SASL_HOST']}/" +
            "some_db?authSource=#{source}&authMechanism=PLAIN"

      # should raise for missing password
      ex = assert_raise Mongo::MongoArgumentError do
        client = @client.class.from_uri(uri)
      end
      assert_match /username and password are required/, ex.message

      uri = "mongodb://#{ENV['MONGODB_SASL_USER']}:foo@#{ENV['MONGODB_SASL_HOST']}/" +
            "some_db?authSource=#{source}&authMechanism=PLAIN"

      # should raise for invalid password
      client = @client.class.from_uri(uri)
      assert_raise Mongo::AuthenticationError do
        client.checkout_reader(:mode => :primary)
      end
    end

  end

end
