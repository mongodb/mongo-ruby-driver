require 'spec_helper'

describe 'Code Quality' do
  if RUBY_VERSION > '1.9' && RUBY_PLATFORM != 'java'

    it 'has no style-guide violations' do
      require 'tailor/cli'
      result = silence do
        t = Tailor::CLI.new %w(lib)
        t.result
      end
      result = result.values.flatten.select {|v| !v.empty?}
      expect(result.size).to eq(0)
    end

    it 'has full test coverage' do
      expect(SimpleCov.result.covered_percent).to be >= 99
    end
  end
end
