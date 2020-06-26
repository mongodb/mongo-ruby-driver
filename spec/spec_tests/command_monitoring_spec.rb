require 'spec_helper'

require 'runners/crud'
require 'runners/command_monitoring'

describe 'Command Monitoring Events' do

  COMMAND_MONITORING_TESTS.each do |file|

    spec = Mongo::CommandMonitoring::Spec.new(file)

    spec.tests.each do |test|
      context(test.description) do

        if test.min_server_fcv
          min_server_fcv test.min_server_fcv
        end
        if test.max_server_version
          max_server_version test.max_server_version
        end

        let(:subscriber) do
          EventSubscriber.new
        end

        let(:monitoring) do
          authorized_client.send(:monitoring)
        end

        before do
          authorized_collection.find.delete_many
          authorized_client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
        end

        test.expectations.each_with_index do |expectation, index|

          it "generates a #{expectation.event_name} for #{expectation.command_name}" do
            begin
              test.run(authorized_collection, subscriber)
              check_event(subscriber, index, expectation)
            rescue Mongo::Error::OperationFailure, Mongo::Error::BulkWriteError
              check_event(subscriber, index, expectation)
            end
          end
        end
      end
    end
  end

  def check_event(subscriber, index, expectation)
    subscriber.all_events.length.should > index
    # TODO move this filtering into EventSubscriber
    events = subscriber.all_events.reject do |event|
      (
        event.is_a?(Mongo::Monitoring::Event::CommandStarted) ||
        event.is_a?(Mongo::Monitoring::Event::CommandSucceeded) ||
        event.is_a?(Mongo::Monitoring::Event::CommandFailed)
      ) &&
      %w(authenticate getnonce saslStart saslContinue).include?(event.command_name)
    end
    actual_event = events[index]
    expected_event_type = expectation.event_type.sub(/_event$/, '')
    Utils.underscore(actual_event.class.name.sub(/.*::/, '')).to_s.should == expected_event_type
    expect(actual_event).to send(expectation.matcher, expectation)
  end
end
