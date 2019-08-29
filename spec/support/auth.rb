module Mongo
  module Auth
    class Spec

      attr_reader :description
      attr_reader :tests

      def initialize(file)
        file = File.new(file)
        @spec = YAML.load(ERB.new(file.read).result)
        file.close
        @description = File.basename(file)

      end

      def tests
        @tests ||= @spec['tests'].collect do |spec|
          Test.new(spec)
        end
      end
    end
  end
end
