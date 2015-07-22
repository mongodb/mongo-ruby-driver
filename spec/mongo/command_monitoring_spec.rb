require 'spec_helper'

describe 'Command Monitoring Events' do

  COMMAND_MONITORING_TESTS.each do |file|

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

          it "generates a #{expectation.event_name} for #{expectation.command_name}" do
            begin
              results = test.run(authorized_collection)
              event = subscriber.send(expectation.event_type)[expectation.command_name]
              expect(event).to_not be_nil
              expect(event.command_name.to_s).to eq(expectation.command_name)
              expect(event.database_name.to_s).to eq(expectation.database_name)
              # expect(event.send(expectation.payload_name)).to
              # include(expectation.data)
            rescue Mongo::Error::OperationFailure
            end
          end
        end
      end
    end
  end
end
