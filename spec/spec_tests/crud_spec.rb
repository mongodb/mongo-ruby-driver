require 'spec_helper'

describe do
  define_crud_spec_tests('CRUD spec tests', CRUD_TESTS.sort) do |spec, req, test|
    let(:client) { authorized_client }
    let(:collection) { client['crud_spec_test'] }

    before do
      if req.nil? || req.satisfied?
        collection.delete_many
        test.setup_test(collection)
      end
    end
  end
end
