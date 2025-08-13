# frozen_string_literal: true

require 'lite_spec_helper'
require 'base64'
require 'tempfile'

RSpec.shared_examples 'atlas connectivity test' do |env_var|
  skip "Environment variable #{env_var} is not set" unless ENV[env_var]

  after do
    client.close
  end

  it 'runs hello successfully' do
    expect { client.database.command(ping: 1) }
      .not_to raise_error
  end
end

describe 'Atlas connectivity' do
  context 'with regular authentication' do
    regular_auth_env_vars = %w[
      ATLAS_REPLICA_SET_URI
      ATLAS_SHARDED_URI
      ATLAS_FREE_TIER_URI
      ATLAS_TLS11_URI
      ATLAS_TLS12_URI
    ]

    regular_auth_env_vars.each do |var|
      describe "Connecting to #{var}" do
        let(:uri) { ENV[var] }
        let(:client) { Mongo::Client.new(uri) }

        include_examples 'atlas connectivity test', var
      end
    end
  end

  context 'with X.509 authentication' do
    x509_auth_env_vars = [
      %w[ATLAS_X509_URI ATLAS_X509_CERT_BASE64],
      %w[ATLAS_X509_DEV_URI ATLAS_X509_DEV_CERT_BASE64]
    ]

    x509_auth_env_vars.each do |uri_var, cert_var|
      describe "Connecting to #{uri_var} with certificate" do
        let(:client_cert) do
          decoded = Base64.decode64(ENV[cert_var])
          cert_file = Tempfile.new([ 'x509-cert', '.pem' ])
          cert_file.write(decoded)
          cert_file.close
          cert_file
        end

        let(:uri) do
          "#{ENV[uri_var]}&tlsCertificateKeyFile=#{URI::DEFAULT_PARSER.escape(client_cert.path)}"
        end

        let(:client) do
          Mongo::Client.new(uri)
        end

        after do
          client_cert.unlink if client_cert
        end

        include_examples 'atlas connectivity test', uri_var
      end
    end
  end
end
