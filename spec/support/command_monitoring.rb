module Mongo
  module CommandMonitoring

    class Spec


      def initialize(file)
        @spec = YAML.load(ERB.new(File.new(file).read).result)
        @data = @spec['data']
        @tests = @spec['tests']
      end

      def tests
        @tests.map do |test|
          Test.new(@data, test)
        end
      end
    end

    class Test

      attr_reader :description

      attr_reader :expectations

      def initialize(data, test)
        @data = data
        @description = test['description']
        @operation = Mongo::CRUD::Operation.get(test['operation'])
        @expectations = test['expectations'].map{ |e| Expectation.new(e) }
      end

      def run(collection)
        collection.insert_many(@data)
        @operation.execute(collection)
      end
    end

    class Expectation

      attr_reader :event_type

      def command_name
        @event_data['command_name']
      end

      def database_name
        @event_data['database_name']
      end

      def event_name
        event_type.gsub('_', ' ')
      end

      def initialize(expectation)
        @event_type = expectation.keys.first
        @event_data = expectation[@event_type]
      end
    end

    class TestSubscriber

      def started(event)
        command_started_event[event.command_name] = event
      end

      def succeeded(event)
        command_succeeded_event[event.command_name] = event
      end

      def failed(event)
        command_failed_event[event.command_name] = event
      end

      private

      def command_started_event
        @started_events ||= BSON::Document.new
      end

      def command_succeeded_event
        @succeeded_events ||= BSON::Document.new
      end

      def command_failed_event
        @failed_events ||= BSON::Document.new
      end
    end
  end
end
