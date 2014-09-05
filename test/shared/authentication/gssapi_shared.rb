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

module GSSAPITests

  # Tests for the GSSAPI Authentication Mechanism.
  #
  # Note: These tests will be skipped automatically unless the test environment
  # has been configured.  
  #
  # In order to run these tests, you must be using JRuby and must set the following
  #   environment variables. The realm and KDC are required so that the corresponding
  #   system properties can be set:
  #
  #   export MONGODB_GSSAPI_HOST='server.domain.com'
  #   export MONGODB_GSSAPI_USER='applicationuser@example.com'
  #   export MONGODB_GSSAPI_REALM='applicationuser@example.com'
  #   export MONGODB_GSSAPI_KDC='SERVER.DOMAIN.COM'
  #
  # You must use kinit when on MRI.
  # You have the option of providing a config file that references a keytab file on JRuby:
  #
  #   export JAAS_LOGIN_CONFIG_FILE='file:///path/to/config/file'
  #
  MONGODB_GSSAPI_HOST    = ENV['MONGODB_GSSAPI_HOST']
  MONGODB_GSSAPI_USER    = ENV['MONGODB_GSSAPI_USER']
  MONGODB_GSSAPI_REALM   = ENV['MONGODB_GSSAPI_REALM']
  MONGODB_GSSAPI_KDC     = ENV['MONGODB_GSSAPI_KDC']
  MONGODB_GSSAPI_PORT    = ENV['MONGODB_GSSAPI_PORT'] || '27017'
  MONGODB_GSSAPI_DB      = ENV['MONGODB_GSSAPI_DB']
  JAAS_LOGIN_CONFIG_FILE = ENV['JAAS_LOGIN_CONFIG_FILE'] # only JRuby

  if ENV.key?('MONGODB_GSSAPI_HOST') && ENV.key?('MONGODB_GSSAPI_USER') &&
     ENV.key?('MONGODB_GSSAPI_REALM') && ENV.key?('MONGODB_GSSAPI_KDC') &&
     ENV.key?('MONGODB_GSSAPI_DB')
    def test_gssapi_authenticate
      client = Mongo::MongoClient.new(MONGODB_GSSAPI_HOST, MONGODB_GSSAPI_PORT)
      if client['admin'].command(:isMaster => 1)['setName']
        client = Mongo::MongoReplicaSetClient.new(["#{MONGODB_GSSAPI_HOST}:#{MONGODB_GSSAPI_PORT}"])
      end

      set_system_properties
      db = client[MONGODB_GSSAPI_DB]
      db.authenticate(MONGODB_GSSAPI_USER, nil, nil, nil, 'GSSAPI')
      assert db.command(:dbstats => 1)

      threads = []
      4.times do
        threads << Thread.new do
          assert db.command(:dbstats => 1)
        end
      end
      threads.each(&:join)
    end

    def test_gssapi_authenticate_uri
      require 'cgi'
      set_system_properties
      username = CGI::escape(ENV['MONGODB_GSSAPI_USER'])
      uri = "mongodb://#{username}@#{ENV['MONGODB_GSSAPI_HOST']}:#{ENV['MONGODB_GSSAPI_PORT']}/?" +
         "authMechanism=GSSAPI"
      client = @client.class.from_uri(uri)
      assert client[MONGODB_GSSAPI_DB].command(:dbstats => 1)
    end

    def test_wrong_service_name_fails
      extra_opts = { :gssapi_service_name => 'example' }
      client = Mongo::MongoClient.new(MONGODB_GSSAPI_HOST, MONGODB_GSSAPI_PORT)
      if client['admin'].command(:isMaster => 1)['setName']
        client = Mongo::MongoReplicaSetClient.new(["#{MONGODB_GSSAPI_HOST}:#{MONGODB_GSSAPI_PORT}"])
      end

      set_system_properties
      assert_raise_error Mongo::AuthenticationError do
        client[MONGODB_GSSAPI_DB].authenticate(MONGODB_GSSAPI_USER, nil, nil, nil, 'GSSAPI', extra_opts)
      end
    end

    def test_wrong_service_name_fails_uri
      set_system_properties

      require 'cgi'
      username = CGI::escape(ENV['MONGODB_GSSAPI_USER'])
      uri = "mongodb://#{username}@#{ENV['MONGODB_GSSAPI_HOST']}:#{ENV['MONGODB_GSSAPI_PORT']}/?" +
         "authMechanism=GSSAPI&gssapiServiceName=example"
      client = @client.class.from_uri(uri)
      assert_raise_error Mongo::AuthenticationError do
        client[MONGODB_GSSAPI_DB].command(:dbstats => 1)
      end
    end

    def test_extra_opts
      extra_opts = { :gssapi_service_name => 'example', :canonicalize_host_name => true }
      client = Mongo::MongoClient.new(MONGODB_GSSAPI_HOST, MONGODB_GSSAPI_PORT)
      set_system_properties

      Mongo::Sasl::GSSAPI.expects(:authenticate).with do |username, client, socket, opts|
        assert_equal opts[:gssapi_service_name], extra_opts[:gssapi_service_name]
        assert_equal opts[:canonicalize_host_name], extra_opts[:canonicalize_host_name]
        [ username, client, socket, opts ]
      end.returns('ok' => true )
      client[MONGODB_GSSAPI_DB].authenticate(MONGODB_GSSAPI_USER, nil, nil, nil, 'GSSAPI', extra_opts)
    end

    def test_extra_opts_uri
      extra_opts = { :gssapi_service_name => 'example', :canonicalize_host_name => true }
      set_system_properties

      Mongo::Sasl::GSSAPI.expects(:authenticate).with do |username, client, socket, opts|
        assert_equal opts[:gssapi_service_name], extra_opts[:gssapi_service_name]
        assert_equal opts[:canonicalize_host_name], extra_opts[:canonicalize_host_name]
        [ username, client, socket, opts ]
      end.returns('ok' => true)

      require 'cgi'
      username = CGI::escape(ENV['MONGODB_GSSAPI_USER'])
      uri = "mongodb://#{username}@#{ENV['MONGODB_GSSAPI_HOST']}:#{ENV['MONGODB_GSSAPI_PORT']}/?" +
         "authMechanism=GSSAPI&gssapiServiceName=example&canonicalizeHostName=true"
      client = @client.class.from_uri(uri)
      client.expects(:receive_message).returns([[{ 'ok' => 1 }], 1, 1])
      client[MONGODB_GSSAPI_DB].command(:dbstats => 1)
    end

    # In order to run this test, you must set the following environment variable:
    #
    #   export MONGODB_GSSAPI_HOST_IP='---.---.---.---'
    #
    if ENV.key?('MONGODB_GSSAPI_HOST_IP')
      def test_canonicalize_host_name
        extra_opts = { :canonicalize_host_name => true }
        set_system_properties
        client = Mongo::MongoClient.new(ENV['MONGODB_GSSAPI_HOST_IP'], MONGODB_GSSAPI_PORT)

        db = client[MONGODB_GSSAPI_DB]
        db.authenticate(MONGODB_GSSAPI_USER, nil, nil, nil, 'GSSAPI', extra_opts)
        assert db.command(:dbstats => 1)
      end
    end

    def test_invalid_extra_options
      extra_opts = { :invalid => true, :option => true }
      client = Mongo::MongoClient.new(MONGODB_GSSAPI_HOST)

      assert_raise Mongo::MongoArgumentError do
        client[MONGODB_GSSAPI_DB].authenticate(MONGODB_GSSAPI_USER, nil, nil, nil, 'GSSAPI', extra_opts)
      end
    end

    private
    def set_system_properties
      if RUBY_PLATFORM =~ /java/
        java.lang.System.set_property 'javax.security.auth.useSubjectCredsOnly', 'false'
        java.lang.System.set_property "java.security.krb5.realm", MONGODB_GSSAPI_REALM
        java.lang.System.set_property "java.security.krb5.kdc", MONGODB_GSSAPI_KDC
        java.lang.System.set_property "java.security.auth.login.config", JAAS_LOGIN_CONFIG_FILE if JAAS_LOGIN_CONFIG_FILE
      end
    end
  end

end
