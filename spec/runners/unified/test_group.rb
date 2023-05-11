# frozen_string_literal: true
# rubocop:todo all

module Unified

  class TestGroup
    def initialize(path, **opts)
      if String === path
        data = ::Utils.load_spec_yaml_file(path)
      else
        data = path
      end
      @spec = BSON::ExtJSON.parse_obj(data)
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
