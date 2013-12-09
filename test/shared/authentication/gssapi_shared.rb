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
  if ENV.key?('MONGODB_GSSAPI_HOST') && ENV.key?('MONGODB_GSSAPI_PRINCIPAL') && ENV.key?('MONGODB_GSSAPI_SERVICE_NAME')

    def create_client(*args)
      if @client_class == MongoClient
        @client_class.new(*args[0], args[1])
      else
        @client_class.new(args[0], args[1])
      end
    end

    def test_gssapi_simple
      db        = @client.db(TEST_DB)
      pricipal  = ENV['MONGODB_GSSAPI_PRINCIPAL']
      mechanism = 'GSSAPI'
      assert db.authenticate(principal, nil, true, nil, mechanism)
    end

    def test_gssapi_from_uri
      pricipal  = ENV['MONGODB_GSSAPI_PRINCIPAL']
      mechanism = 'GSSAPI'
      uri       = "mongodb://#{principal}@#{@uri_info}/admin?authMechanism=#{mechanism}"
      client    = Mongo::URIParser.new(uri).connection({})
      assert client.database_name
    end

    def test_gssapi_with_service_name
      db           = @client.db(TEST_DB)
      pricipal     = ENV['MONGODB_GSSAPI_PRINCIPAL']
      service_name = ENV['MONGODB_GSSAPI_SERVICE_NAME']
      mechanism    = 'GSSAPI'
      assert db.authenticate(principal, nil, true, nil, mechanism, :gssapi_service_name => service_name)
    end

    def test_gssapi_with_service_name_from_uri
      pricipal     = ENV['MONGODB_GSSAPI_PRINCIPAL']
      service_name = ENV['MONGODB_GSSAPI_SERVICE_NAME']
      mechanism    = 'GSSAPI'
      uri          = "mongodb://#{principal}@#{@uri_info}/admin?authMechanism=#{mechanism}&gssapiServiceName=#{service_name}"
      client       = Mongo::URIParser.new(uri).connection({})
      assert client.database_name
    end

  end
end
