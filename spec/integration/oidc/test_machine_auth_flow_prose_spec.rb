# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

# Generic test callback that pulls tokens from the env.
class TestCallback
  attr_reader :token_file

  def initialize(token_file: 'test_user1')
    @token_file = token_file
  end

  def execute(timeout:, version:, username: nil)
    location = File.join(ENV.fetch('OIDC_TOKEN_DIR'), token_file)
    token = File.read(location)
    { access_token: token }
  end
end

describe 'OIDC Authentication Prose Tests' do
  require_oidc 'test'

  describe 'Machine Authentication Flow Prose Tests' do
    let(:uri) do
      ENV.fetch('MONGODB_URI_SINGLE')
    end

    context 'when using callback authentication' do
      # 1.1 Callback is called during authentication
      context 'when executing an operation' do
        let(:callback) do
          TestCallback.new(token_file: 'test_machine')
        end

        let(:client) do
          Mongo::Client.new(uri,
            database: 'test',
            retry_reads: false,
            auth_mech_properties: {
              oidc_callback: callback
            }
          )
        end

        let(:collection) do
          client['test']
        end

        before(:each) do
          allow(callback).to receive(:execute).and_call_original
        end

        after(:each) do
          client.close
        end

        it 'successfully authenticates' do
          # Create an OIDC configured client.
          # Perform a find operation that succeeds.
          # Assert that the callback was called 1 time.
          # Close the client.
          expect(collection.find.to_a).to be_empty
          expect(callback).to have_received(:execute).once
        end
      end

      # 1.2 Callback is called once for multiple connections
      context 'when using multiple connections' do
        let!(:callback) do
          TestCallback.new(token_file: 'test_machine')
        end

        let!(:client) do
          Mongo::Client.new(uri,
            database: 'test',
            retry_reads: false,
            auth_mech_properties: {
              oidc_callback: callback
            }
          )
        end

        let!(:collection) do
          client['test']
        end

        before(:each) do
          allow(callback).to receive(:execute).and_call_original
        end

        after(:each) do
          client.close
        end

        it 'only calls the callback once for each thread' do
          # Start 10 threads and run 100 find operations in each thread that all succeed.
          # Assert that the callback was called 1 time for each thread.
          threads = []
          10.times do
            threads << Thread.new do
              100.times do
                expect(collection.find.to_a).to be_empty
              end
            end
          end
          threads.each do |thread|
            thread.join
          end
          expect(callback).to have_received(:execute).exactly(10).times
        end
      end
    end

    context 'when validating callbacks' do
      # 2.1 Valid Callback Inputs
      context 'when callback inputs are valid' do
        let(:callback) do
          TestCallback.new(token_file: 'test_machine')
        end

        let(:client) do
          Mongo::Client.new(uri,
            database: 'test',
            retry_reads: false,
            auth_mech_properties: {
              oidc_callback: callback
            }
          )
        end

        let(:collection) do
          client['test']
        end

        before(:each) do
          allow(callback).to receive(:execute).and_call_original
        end

        after(:each) do
          client.close
        end

        it 'successfully authenticates' do
          # Create an OIDC configured client with an OIDC callback that validates its inputs and returns a valid access token.
          # Perform a find operation that succeeds.
          # Assert that the OIDC callback was called with the appropriate inputs, including the timeout parameter if possible.
          # Close the client.
          expect(collection.find.to_a).to be_empty
          expect(callback).to have_received(:execute).with(timeout: 60000, version: 1, username: nil).once
        end
      end

      # 2.2 OIDC Callback Returns Null
      context 'when the callback returns null' do
        let(:callback) do
          TestCallback.new()
        end

        let(:client) do
          Mongo::Client.new(uri,
            database: 'test',
            retry_reads: false,
            auth_mech_properties: {
              oidc_callback: callback
            }
          )
        end

        let(:collection) do
          client['test']
        end

        before(:each) do
          allow(callback).to receive(:execute).and_return(nil)
        end

        after(:each) do
          client.close
        end

        it 'fails authentication' do
          # Create an OIDC configured client with an OIDC callback that returns null.
          # Perform a find operation that fails.
          # Close the client.
          expect {
            collection.find.to_a
          }.to raise_error(Mongo::Error::OidcError)
        end
      end

      # 2.3 OIDC Callback Returns Missing Data
      context 'when the callback returns missing data' do
        let(:callback) do
          TestCallback.new()
        end

        let(:client) do
          Mongo::Client.new(uri,
            database: 'test',
            retry_reads: false,
            auth_mech_properties: {
              oidc_callback: callback
            }
          )
        end

        let(:collection) do
          client['test']
        end

        before(:each) do
          allow(callback).to receive(:execute).and_return({ field: 'value' })
        end

        after(:each) do
          client.close
        end

        it 'fails authentication' do
          # Create an OIDC configured client with an OIDC callback that returns data not conforming to the OIDCCredential with missing fields.
          # Perform a find operation that fails.
          # Close the client.
          expect {
            collection.find.to_a
          }.to raise_error(Mongo::Error::OidcError)
        end
      end

      # 2.4 Invalid Client Configuration with Callback
      context 'when the client is misconfigured' do
        let(:callback) do
          TestCallback.new()
        end

        it 'fails on client configuration' do
          # Create an OIDC configured client with an OIDC callback and auth mechanism property ENVIRONMENT:test.
          # Assert it returns a client configuration error.
          expect {
            Mongo::Client.new(uri,
              database: 'test',
              retry_reads: false,
              auth_mech_properties: {
                oidc_callback: callback,
                environment: 'test'
              }
            )
          }.to raise_error(Mongo::Auth::InvalidConfiguration)
        end
      end

      # 2.5 Invalid use of ALLOWED_HOSTS
      context 'when allowed hosts are misconfigured' do
        let(:callback) do
          TestCallback.new()
        end

        it 'fails on client configuration' do
          # Create an OIDC configured client with auth mechanism properties {"ENVIRONMENT": "azure", "ALLOWED_HOSTS": []}.
          # Assert it returns a client configuration error upon client creation, or client connect if your driver validates on connection.
          expect {
            Mongo::Client.new(uri,
              database: 'test',
              retry_reads: false,
              auth_mech_properties: {
                environment: 'azure',
                allowed_hosts: []
              }
            )
          }.to raise_error(Mongo::Auth::InvalidConfiguration)
        end
      end
    end

    context 'when authentication fails' do
      # 3.1 Authentication failure with cached tokens fetch a new token and retry auth
      context 'when tokens are cached' do
        let(:callback) do
          TestCallback.new()
        end

        let(:client) do
          Mongo::Client.new(uri,
            database: 'test',
            retry_reads: false,
            auth_mech_properties: {
              oidc_callback: callback
            }
          )
        end

        let(:collection) do
          client['test']
        end

        let(:cache) do
        end

        before(:each) do
        end

        # Create an OIDC configured client.
        # Poison the Client Cache with an invalid access token.
        # Perform a find operation that succeeds.
        # Assert that the callback was called 1 time.
        # Close the client.
        it 'successfully authenticates' do
        end
      end

      # 3.2 Authentication failures without cached tokens return an error
      context 'when no tokens are cached' do
      end

      # 3.3 Unexpected error code does not clear the cache
      context 'when error code is unexpected' do
      end
    end

    context 'when reauthenticating' do
      # 4.1 Reauthentication Succeeds
      context 'when reauthentication succeeds' do
      end

      context 'when reauthentication fails' do
        # 4.2 Read Commands Fail If Reauthentication Fails
        context 'when executing a read' do
        end

        # 4.3 Write Commands Fail If Reauthentication Fails
        context 'when executing a write' do
        end
      end
    end
  end
end
