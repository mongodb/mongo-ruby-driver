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

module SSLTests
  include Mongo

  CERT_PATH   = "#{Dir.pwd}/test/fixtures/certificates/"
  CLIENT_CERT = "#{CERT_PATH}client.pem"
  CA_CERT     = "#{CERT_PATH}ca.pem"

  def create_client(*args)
    if @client_class == MongoClient
      @client_class.new(*args[0], args[1])
    else
      @client_class.new(args[0], args[1])
    end
  end

  # This test doesn't connect, no server config required
  def test_ssl_configuration
    # raises when ssl=false and ssl opts specified
    assert_raise MongoArgumentError do
      create_client(@connect_info, :connect  => false,
                                   :ssl      => false,
                                   :ssl_cert => CLIENT_CERT)
    end

    # raises when ssl=nil and ssl opts specified
    assert_raise MongoArgumentError do
      create_client(@connect_info, :connect => false,
                                   :ssl_key => CLIENT_CERT)
    end

    # raises when verify=true and no ca_cert
    assert_raise MongoArgumentError do
      create_client(@connect_info, :connect    => false,
                                   :ssl        => true,
                                   :ssl_key    => CLIENT_CERT,
                                   :ssl_cert   => CLIENT_CERT,
                                   :ssl_verify => true)
    end
  end

  # Requires MongoDB built with SSL and the follow options:
  #
  # mongod --dbpath /path/to/data/directory --sslOnNormalPorts \
  # --sslPEMKeyFile /path/to/server.pem \
  # --sslCAFile /path/to/ca.pem \
  # --sslCRLFile /path/to/crl.pem \
  # --sslWeakCertificateValidation
  #
  # Make sure you have 'server' as an alias for localhost in /etc/hosts
  #
  def test_ssl_basic
    client = create_client(@connect_info, :connect => false, :ssl => true)
    assert client.connect
  end

  # Requires MongoDB built with SSL and the follow options:
  #
  # mongod --dbpath /path/to/data/directory --sslOnNormalPorts \
  # --sslPEMKeyFile /path/to/server.pem \
  # --sslCAFile /path/to/ca.pem \
  # --sslCRLFile /path/to/crl.pem
  #
  # Make sure you have 'server' as an alias for localhost in /etc/hosts
  #
  def test_ssl_with_cert
    client = create_client(@connect_info, :connect  => false,
                                          :ssl      => true,
                                          :ssl_cert => CLIENT_CERT,
                                          :ssl_key  => CLIENT_CERT)
    assert client.connect
  end

  def test_ssl_with_peer_cert_validation
    client = create_client(@connect_info, :connect     => false,
                                          :ssl         => true,
                                          :ssl_key     => CLIENT_CERT,
                                          :ssl_cert    => CLIENT_CERT,
                                          :ssl_verify  => true,
                                          :ssl_ca_cert => CA_CERT)
    assert client.connect
  end

  def test_ssl_peer_cert_validation_hostname_fail
    client = create_client(@bad_connect_info, :connect     => false,
                                              :ssl         => true,
                                              :ssl_key     => CLIENT_CERT,
                                              :ssl_cert    => CLIENT_CERT,
                                              :ssl_verify  => true,
                                              :ssl_ca_cert => CA_CERT)
    assert_raise ConnectionFailure do
      client.connect
    end
  end

  # Requires mongod built with SSL and the follow options:
  #
  # mongod --dbpath /path/to/data/directory --sslOnNormalPorts \
  # --sslPEMKeyFile /path/to/server.pem \
  # --sslCAFile /path/to/ca.pem \
  # --sslCRLFile /path/to/crl_client_revoked.pem
  #
  # Make sure you have 'server' as an alias for localhost in /etc/hosts
  #
  def test_ssl_with_invalid_cert
    assert_raise ConnectionFailure do
      create_client(@connect_info, :ssl         => true,
                                   :ssl_key     => CLIENT_CERT,
                                   :ssl_cert    => CLIENT_CERT,
                                   :ssl_verify  => true,
                                   :ssl_ca_cert => CA_CERT)
    end
  end

  # X509 Authentication Tests
  #
  # Requires MongoDB built with SSL and the follow options:
  #
  # mongod --auth --dbpath /path/to/data/directory --sslOnNormalPorts \
  # --sslPEMKeyFile /path/to/server.pem \
  # --sslCAFile /path/to/ca.pem \
  # --sslCRLFile /path/to/crl.pem
  #
  if ENV.key?('MONGODB_X509_USER')

    def test_x509_authentication
      mechanism = 'MONGODB-X509'
      client    = create_client(@connect_info, :ssl => true,
                                               :ssl_cert => CLIENT_CERT)

      return unless client.server_version > '2.5.2'

      user     = ENV['MONGODB_X509_USER']
      db       = client.db('$external')

      # add user for test (enable auth)
      roles    = [{:role => 'readWriteAnyDatabase', :db => 'admin'},
                  {:role => 'userAdminAnyDatabase', :db => 'admin'}]
      db.add_user(user, nil, false, :roles => roles)

      assert db.authenticate(user, nil, nil, nil, mechanism)
      assert db.collection_names

      assert db.logout
      assert_raise Mongo::AuthenticationError do
        db.collection_names
      end

      assert MongoReplicaSetClient.from_uri(
        "mongodb://#{user}@#{@uri_info}/admin?authMechanism=#{mechanism}")
      assert db.collection_names

      # clean up and remove all users
      db.command(:dropAllUsersFromDatabase => 1)
      db.logout
    end

  end

end
