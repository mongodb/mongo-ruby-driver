# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

# These tests assume the server was started with the certificates in
# spec/support/certificates, and has the user that Evergreen scripts create
# corresponding to the client certificate.
describe 'X.509 auth integration tests' do
  require_x509_auth

  let(:authenticated_user_info) do
    # https://stackoverflow.com/questions/21414608/mongodb-show-current-user
    info = client.database.command(connectionStatus: 1).documents.first
    info[:authInfo][:authenticatedUsers].first
  end

  let(:authenticated_user_name) { authenticated_user_info[:user] }

  let(:client) do
    new_local_client(SpecConfig.instance.addresses, client_options)
  end

  let(:base_client_options) { SpecConfig.instance.ssl_options }

  context 'when auth not specified' do
    let(:client_options) do
      base_client_options
    end

    it 'does not authenticate' do
      authenticated_user_info.should be nil
    end
  end

  context 'certificate matching a defined user' do
    let(:common_name) do
      "C=US,ST=New York,L=New York City,O=MongoDB,OU=x509,CN=localhost".freeze
    end

    let(:subscriber) { Mrss::EventSubscriber.new }

    shared_examples 'authenticates successfully' do
      it 'authenticates successfully' do
        authenticated_user_name.should == common_name
      end

      let(:commands) do
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
        authenticated_user_name
        commands = subscriber.started_events.map(&:command_name)
      end

      context 'server 4.2 and lower' do
        max_server_version '4.2'

        it 'uses the authenticate command to authenticate' do
          commands.should == %w(authenticate connectionStatus)
        end
      end

      context 'server 4.4 and higher' do
        min_server_fcv '4.4'

        it 'uses speculative authentication in hello to authenticate' do
          commands.should == %w(connectionStatus)
        end
      end
    end

    context 'when user name is not explicitly provided' do
      let(:client_options) do
        base_client_options.merge(auth_mech: :mongodb_x509)
      end

      it_behaves_like 'authenticates successfully'
    end

    context 'when user name is explicitly provided and matches certificate common name' do
      let(:client_options) do
        base_client_options.merge(auth_mech: :mongodb_x509, user: common_name)
      end

      it_behaves_like 'authenticates successfully'
    end

    context 'when user name is explicitly provided and does not match certificate common name' do
      let(:client_options) do
        base_client_options.merge(auth_mech: :mongodb_x509, user: 'OU=world,CN=hello')
      end

      it 'fails to authenticate' do
        lambda do
          authenticated_user_name
        end.should raise_error(Mongo::Auth::Unauthorized, /Client certificate.*is not authorized/)
      end

      # This test applies to both pre-4.4 and 4.4+.
      # When speculative authentication fails, the response is indistinguishable
      # from that of a server that does not support speculative authentication,
      # and we will try to authenticate as a separate command.
      it 'uses the authenticate command to authenticate' do
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
        lambda do
          authenticated_user_name
        end.should raise_error(Mongo::Auth::Unauthorized, /Client certificate.*is not authorized/)
        commands = subscriber.started_events.map(&:command_name)
        commands.should == %w(authenticate)
      end
    end
  end
end
