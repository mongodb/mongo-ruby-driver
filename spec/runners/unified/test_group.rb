module Unified

  class TestGroup
    def initialize(path)
      @spec = BSON::ExtJSON.parse_obj(YAML.load(File.read(path)))
    end

    def tests
      reqs = @spec['runOnRequirements']

      @spec.fetch('tests').map do |test|
        sub = @spec.dup
        sub.delete('tests')
        sub['test'] = test
        sub['group_runOnRequirements'] = reqs
        Test.new(sub)
      end
    end
  end
end
