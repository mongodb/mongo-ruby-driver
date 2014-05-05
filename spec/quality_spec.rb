require 'spec_helper'

describe 'Code Quality', :quality do

  if RUBY_VERSION > '1.9' && RUBY_VERSION < '2.2'
    pending 'has no style-guide violations', :style do
      require 'rubocop'
      result = silence { Rubocop::CLI.new.run }
      puts '[STYLE] style violations found. ' +
           'Please run \'rubocop\' for a full report.' if result == 1
      expect(result).to eq(0)
    end

    unless RUBY_PLATFORM =~ /java/
      it 'has required minimum test coverage' do
        expect(SimpleCov.result.covered_percent).to be >= COVERAGE_MIN
      end
    end
  end
end
