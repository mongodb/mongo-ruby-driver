# frozen_string_literal: true

require 'spec_helper'

require 'runners/unified'

base = "#{CURRENT_PATH}/spec_tests/data/read_write_concern/write_concern_errors"
RW_CONCERN_WCE_TESTS = Dir.glob("#{base}/**/*.yml").sort

describe 'Read write concern write concern error unified spec tests' do
  define_unified_spec_tests(
    base,
    RW_CONCERN_WCE_TESTS,
    skip_descriptions: {
      # The Ruby driver does not implement GridFS file rename, so the
      # rename operations cannot be exercised.
      'file rename reports a writeConcernError' => 'Ruby driver does not support GridFS rename',
      'renameByName reports a writeConcernError' => 'Ruby driver does not support GridFS rename',
    }
  )
end
