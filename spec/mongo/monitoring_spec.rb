require 'spec_helper'

describe Mongo::Monitoring do

  describe '#publish' do

    context 'a sample listener to a query series of events' do

      # This is a simple example of a subscriber to events that simply logs them.
      class LogSubscriber

        def initialize
          @logger = Logger.new($stdout)
        end

        def started(event)
          @logger.info("MONGODB.#{event.name} STARTED | #{event.connection} | #{event.arguments}")
        end

        def completed(event)
          @logger.info("MONGODB.#{event.name} COMPLETED | #{event.connection} | (#{event.duration}s)")
        end

        def failed(event)
          @logger.info("MONGODB.#{event.name} FAILED | #{event.connection} | #{event.message} | (#{event.duration}s)")
        end
      end

      let(:subscriber) do
        LogSubscriber.new
      end

      before do
        Mongo::Monitoring.subscribe(Mongo::Monitoring::COMMAND, subscriber)

        102.times do |n|
          authorized_collection.insert_one({ name: "test_#{n}" })
        end
      end

      after do
        Mongo::Monitoring.send(:subscribers).clear

        authorized_collection.find.delete_many
      end

      it 'logs the events in the series' do
        authorized_collection.find.to_a
      end
    end
  end
end
