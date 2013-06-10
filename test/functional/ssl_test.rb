require 'test_helper'

class SSLCertValidationTest < Test::Unit::TestCase
  include Mongo

  CERT_PATH   = "#{Dir.pwd}/test/fixtures/certificates/"
  CLIENT_CERT = "#{CERT_PATH}client.pem"
  CA_CERT     = "#{CERT_PATH}ca.pem"

  # This test doesn't connect, no server config required
  def test_ssl_configuration
    # raises when ssl=false and ssl opts specified
    assert_raise MongoArgumentError do
      MongoClient.new('server', 27017, :connect  => false,
                                       :ssl      => false,
                                       :ssl_cert => CLIENT_CERT)
    end

    # raises when ssl=nil and ssl opts specified
    assert_raise MongoArgumentError do
      MongoClient.new('server', 27017, :connect => false,
                                       :ssl_key => CLIENT_CERT)
    end

    # raises when verify=true and no ca_cert
    assert_raise MongoArgumentError do
      MongoClient.new('server', 27017, :connect    => false,
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
    client = MongoClient.new('server', 27017, :connect => false,
                                              :ssl     => true)
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
    client = MongoClient.new('server', 27017, :connect  => false,
                                              :ssl      => true,
                                              :ssl_cert => CLIENT_CERT,
                                              :ssl_key  => CLIENT_CERT)
    assert client.connect
  end

  def test_ssl_with_peer_cert_validation
    client = MongoClient.new('server', 27017, :connect     => false,
                                              :ssl         => true,
                                              :ssl_key     => CLIENT_CERT,
                                              :ssl_cert    => CLIENT_CERT,
                                              :ssl_verify  => true,
                                              :ssl_ca_cert => CA_CERT)
    assert client.connect
  end

  def test_ssl_peer_cert_validation_hostname_fail
    client = MongoClient.new('localhost', 27017, :connect     => false,
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
      MongoClient.new('server', 27017, :ssl         => true,
                                       :ssl_key     => CLIENT_CERT,
                                       :ssl_cert    => CLIENT_CERT,
                                       :ssl_verify  => true,
                                       :ssl_ca_cert => CA_CERT)
    end
  end

end
