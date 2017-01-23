require 'spec_helper'

def skippable?(file)
  !write_command_enabled? && (file.include?('bulkWrite') || file.include?('insert'))
end

def ignore?(test)
  if version = test.ignore_if_server_version_greater_than
    return true if version == "3.0" && find_command_enabled?
  end
  if version = test.ignore_if_server_version_less_than
    return true if version == "3.1" && !find_command_enabled?
  end
  false
end

describe 'Command Monitoring Events' do

  COMMAND_MONITORING_TESTS.each do |file|

    if !skippable?(file)

      spec = Mongo::CommandMonitoring::Spec.new(file)

      spec.tests.each do |test|

        context(test.description) do

          let(:subscriber) do
            Mongo::CommandMonitoring::TestSubscriber.new
          end

          let(:monitoring) do
            authorized_client.instance_variable_get(:@monitoring)
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

            it "generates a #{expectation.event_name} for #{expectation.command_name}", unless: ignore?(test) do
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
end
