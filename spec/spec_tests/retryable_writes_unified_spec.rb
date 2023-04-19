# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/retryable_writes"
RETRYABLE_WRITE_UNIFIED_TESTS = Dir.glob("#{base}/unified/**/*.yml").sort

describe 'Retryable reads spec tests - unified' do
  require_wired_tiger
  require_no_multi_mongos

  # Do not run these tests when write retries are disabled globally -
  # the tests won't work in that case and testing them with retries enabled
  # is simply redundant.
  require_retry_writes

  define_unified_spec_tests(base, RETRYABLE_WRITE_UNIFIED_TESTS)
end
