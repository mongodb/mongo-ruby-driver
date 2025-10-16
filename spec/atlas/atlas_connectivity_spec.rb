# frozen_string_literal: true

require 'lite_spec_helper'
require 'base64'
require 'tempfile'

RSpec.shared_examples 'atlas connectivity test' do
  after do
    client.close
  rescue StandardError
    # no-op
  end

  it 'runs hello successfully' do
    expect { client.database.command(ping: 1) }
      .not_to raise_error
  end
end

describe 'Atlas connectivity' do
  before do
    skip 'These tests must be run against a live Atlas cluster' unless ENV['ATLAS_TESTING']
  end

  context 'with regular authentication' do
    regular_auth_env_vars = %w[
      ATLAS_SERVERLESS
      ATLAS_SRV_SERVERLESS
      ATLAS_FREE
      ATLAS_SRV_FREE
      ATLAS_REPL
      ATLAS_SRV_REPL
      ATLAS_SHRD
      ATLAS_SRV_SHRD
      ATLAS_TLS11
      ATLAS_SRV_TLS11
      ATLAS_TLS12
      ATLAS_SRV_TLS12
    ]

    regular_auth_env_vars.each do |uri_var|
      describe "Connecting to #{uri_var}" do
        before do
          raise "Environment variable #{uri_var} is not set" unless ENV[uri_var]
        end

        let(:uri) { ENV[uri_var] }

        let(:client) { Mongo::Client.new(uri) }

        include_examples 'atlas connectivity test'
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
        before do
          raise "Environment variable #{uri_var} is not set" unless ENV[uri_var]
        end

        let(:client_cert) do
          decoded = Base64.strict_decode64(ENV[cert_var])
          cert_file = Tempfile.new([ 'x509-cert', '.pem' ])
          cert_file.write(decoded)
          File.chmod(0o600, cert_file.path)
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
          client_cert&.unlink
        end

        include_examples 'atlas connectivity test'
      end
    end
  end
end
