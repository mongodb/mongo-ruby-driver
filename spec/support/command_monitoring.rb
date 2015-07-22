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
    end

    class Expectation

      def initialize(expectation)
        @event_type = expectation.keys.first
        @event_data = expectation[@event_type]
      end
    end
  end
end
