require 'spec_helper'

describe 'Code Quality', :quality do

  unless RUBY_VERSION < '1.9'
    it 'has no style-guide violations' do
      require 'rubocop'
      result = silence { Rubocop::CLI.new.run }
      puts '[FAIL] Style issues found! To view a report, ' +
           'please run "rubocop" from the project root.' unless result == 0
      expect(result).to eq(0)
    end

    it 'has required minimum test coverage' do
      expect(SimpleCov.result.covered_percent).to be >= COVERAGE_MIN
    end
  end

end
