require 'spec_helper'

describe 'CRUD spec tests' do
  define_crud_spec_tests(CRUD_TESTS) do |spec, req, test|
    let(:client) { authorized_client }
  end
end
