require 'spec_helper'

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
          Mongo::CommandMonitoring::TestSubscriber.new
        end

        let(:monitoring) do
          authorized_client.send(:monitoring)
        end

        before do
          authorized_collection.find.delete_many
          authorized_client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
        end

        after do
          monitoring.subscribers[Mongo::Monitoring::COMMAND].delete(subscriber)
          authorized_collection.find.delete_many
        end

        test.expectations.each do |expectation|

          it "generates a #{expectation.event_name} for #{expectation.command_name}" do
            begin
              test.run(authorized_collection)
              event = subscriber.send(expectation.event_type)[expectation.command_name]
              expect(event).to send(expectation.matcher, expectation)
            rescue Mongo::Error::OperationFailure, Mongo::Error::BulkWriteError
              event = subscriber.send(expectation.event_type)[expectation.command_name]
              expect(event).to send(expectation.matcher, expectation)
            end
          end
        end
      end
    end
  end
end
