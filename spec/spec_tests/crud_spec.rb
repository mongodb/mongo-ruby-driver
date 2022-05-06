# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

require 'runners/crud'

describe 'CRUD v1 spec tests' do
  define_crud_spec_tests(CRUD_TESTS) do |spec, req, test|
    let(:client) { authorized_client }
  end
end
