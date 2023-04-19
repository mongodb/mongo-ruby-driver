# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

# This test can be used to manually verify that there are no leaked
# background threads - execute it after executing another test (in the same
# rspec run) that is suspected to leak background threads, such as by
# running:
#
# rspec your_spec.rb spec/integration/check_clean_slate_spec.rb

describe 'Check clean slate' do
  clean_slate_for_all_if_possible

  it 'checks' do
    # Nothing
  end
end
