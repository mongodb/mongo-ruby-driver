# frozen_string_literal: true

require 'spec_helper'

describe 'CSOT for encryption' do
  require_libmongocrypt
  min_server_fcv '4.2'

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:subscriber) { Mrss::EventSubscriber.new }

  describe 'mongocryptd' do
    before do
      Process.spawn(
        'mongocryptd',
        '--pidfilepath=bypass-spawning-mongocryptd.pid', '--port=23000', '--idleShutdownTimeoutSecs=60',
        [:out, :err] => '/dev/null'
      )
    end

    let(:client) do
      Mongo::Client.new('mongodb://localhost:23000/?timeoutMS=1000').tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    let(:ping_command) do
      subscriber.started_events.find do |event|
        event.command_name == 'ping'
      end&.command
    end

    it 'does not set maxTimeMS for commands sent to mongocryptd' do
      expect do
        client.use('admin').command(ping: 1)
      end.to raise_error(Mongo::Error::OperationFailure)

      expect(ping_command).not_to have_key('maxTimeMS')
    end
  end
end
