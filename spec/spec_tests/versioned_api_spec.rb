# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/versioned_api"
UNIFIED_TESTS = Dir.glob("#{base}/**/*.yml").sort
# https://jira.mongodb.org/browse/RUBY-3721
SKIPPED_TESTS = 'runcommand-helper-no-api-version-declared.yml'

TESTS = UNIFIED_TESTS.reject { |file| file.end_with?(SKIPPED_TESTS) }

describe 'Versioned API spec tests' do
  define_unified_spec_tests(base, TESTS)
end
