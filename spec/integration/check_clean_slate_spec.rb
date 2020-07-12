require 'lite_spec_helper'

# This test can be used to manually verify that there are no leaked
# background threads - execute it after executing another test (in the same
# rspec run) that is suspected to leak background threads.

describe 'Check clean slate' do
  clean_slate_for_all_if_possible

  it 'checks' do
    # Nothing
  end
end
