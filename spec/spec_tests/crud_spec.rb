require 'spec_helper'

define_crud_spec_tests('CRUD spec tests', CRUD_TESTS.sort) do |spec, test|
  let(:client) { authorized_client }
  let(:collection) { client['crud_spec_test'] }

  before do
    collection.delete_many
    test.setup_test(collection)
  end
end
