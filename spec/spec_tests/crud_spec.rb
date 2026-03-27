# frozen_string_literal: true

require 'spec_helper'

require 'runners/crud'

describe 'CRUD v1 spec tests' do
  define_crud_spec_tests(CRUD_TESTS) do |_spec, _req, _test|
    let(:client) { authorized_client }
  end
end
