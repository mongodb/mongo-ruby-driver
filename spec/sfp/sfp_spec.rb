# frozen_string_literal: true

require 'lite_spec_helper'
require 'base64'
require 'tempfile'
require 'uri'

# Connectivity tests for an Atlas Secure Frontend Processor (SFP), a proxy that
# sits in front of Atlas clusters to provide additional security capabilities.
# See the atlas-sfp-testing specification for the full list of required tests.
#
# These tests run against a preconfigured Atlas cluster and are driven entirely
# by environment variables. They are skipped unless SFP_TESTING is set.
describe 'Atlas SFP connectivity' do
  before do
    skip 'Set SFP_TESTING to run tests against a live Atlas SFP cluster' unless ENV['SFP_TESTING']
  end

  # Each authenticated test runs under three variations: a baseline, one with a
  # compressor enabled, and one with Server API version 1.
  variations = {
    'baseline' => {},
    'with compression' => { compressors: %w[ zlib ] },
    'with server API version 1' => { server_api: { version: '1' } },
  }.freeze

  after do
    client.close
  rescue StandardError
    # no-op
  end

  shared_examples 'connects successfully' do
    it 'succeeds at ping' do
      result = client.use(:admin).database.command(ping: 1).documents.first
      expect(result['ok']).to eq(1)
    end

    it 'reports the expected authentication state' do
      info = client.use(:admin).database.command(connectionStatus: 1).documents.first
      expect(info['ok']).to eq(1)
      authenticated_users = info[:authInfo][:authenticatedUsers]
      if authenticated
        expect(authenticated_users).not_to be_empty
      else
        expect(authenticated_users).to be_empty
      end
    end
  end

  shared_examples 'performs CRUD operations' do
    # Drivers MUST use a unique collection name for each test run.
    let(:collection) { client.use('db')["sfp_test_#{BSON::ObjectId.new}"] }

    # Drivers MUST drop the test collection after the test completes,
    # regardless of success or failure.
    after do
      collection.drop
    rescue StandardError
      # no-op
    end

    it 'inserts and reads back a document' do
      collection.insert_one(_id: 0)
      expect(collection.find(_id: 0).to_a).to eq([ { '_id' => 0 } ])
    end
  end

  context 'when unauthenticated' do
    let(:authenticated) { false }
    let(:client) { Mongo::Client.new(ENV.fetch('SFP_ATLAS_URI')) }

    include_examples 'connects successfully'
  end

  context 'when using SCRAM-SHA-256 authentication' do
    let(:authenticated) { true }

    variations.each do |description, options|
      context description do
        let(:client) do
          Mongo::Client.new(
            ENV.fetch('SFP_ATLAS_URI'),
            {
              user: ENV.fetch('SFP_ATLAS_USER'),
              password: ENV.fetch('SFP_ATLAS_PASSWORD'),
              auth_mech: :scram256,
            }.merge(options)
          )
        end

        include_examples 'connects successfully'
        include_examples 'performs CRUD operations'
      end
    end
  end

  context 'when using X.509 authentication' do
    let(:authenticated) { true }

    let(:client_certificate) do
      decoded = Base64.strict_decode64(ENV.fetch('SFP_ATLAS_X509_BASE64'))
      file = Tempfile.new([ 'sfp-x509-cert', '.pem' ])
      file.write(decoded)
      File.chmod(0o600, file.path)
      file.close
      file
    end

    let(:uri) do
      "#{ENV.fetch('SFP_ATLAS_X509_URI')}&tlsCertificateKeyFile=" \
        "#{URI::DEFAULT_PARSER.escape(client_certificate.path)}"
    end

    after do
      client_certificate.unlink
    rescue StandardError
      # no-op
    end

    variations.each do |description, options|
      context description do
        let(:client) { Mongo::Client.new(uri, options) }

        include_examples 'connects successfully'
        include_examples 'performs CRUD operations'
      end
    end
  end
end
