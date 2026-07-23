# frozen_string_literal: true

module Unified
  class TestGroup
    def initialize(path, **opts)
      data = if path.is_a?(String)
               ::Utils.load_spec_yaml_file(path)
             else
               path
             end
      # Parse in :bson mode so extended-JSON types are preserved as their BSON
      # equivalents (e.g. $numberLong -> BSON::Int64, $date -> Time). QE range
      # tests require this: the server rejects a range field whose configured
      # min/max type does not match the field type (e.g. long field with int min).
      @spec = BSON::ExtJSON.parse_obj(data, mode: :bson)
      @options = opts
    end

    attr_reader :options

    def tests
      reqs = @spec['runOnRequirements']

      @spec.fetch('tests').map do |test|
        sub = @spec.dup
        sub.delete('tests')
        sub['test'] = test
        sub['group_runOnRequirements'] = reqs
        Test.new(sub, **options)
      end
    end
  end
end
