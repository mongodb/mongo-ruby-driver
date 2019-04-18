module Mongo
  module CRUD
    class SpecBase
      def initialize
        @requirements = if run_on = @spec['runOn']
          run_on.map do |spec|
            Requirement.new(spec)
          end
        elsif Requirement::YAML_KEYS.any? { |key| @spec.key?(key) }
          [Requirement.new(@spec)]
        else
          nil
        end
      end

      attr_reader :requirements
    end
  end
end
