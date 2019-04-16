require 'spec_helper'

describe do
  define_crud_spec_tests('CRUD spec tests', CRUD_TESTS.sort) do |spec, req, test|
    let(:client) { authorized_client }
  end
end
