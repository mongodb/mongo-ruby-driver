require 'lite_spec_helper'

describe 'Operations' do
  let(:uri) { ENV['ATLAS_URI'] }
  let(:client) { Mongo::Client.new(uri) }

  before do
    if uri.nil?
      skip "ATLAS_URI not set in environment"
    end
  end

  describe 'list_collections' do
    # Atlas free tier proxy enforces restrictions on list_collections
    # arguments. This tests verifies that list_collections works on Atlas

    it 'works' do
      # We are not allowed to mutate the database, therefore the list of
      # collections would generally be empty.
      expect do
        client.database.list_collections
      end.not_to raise_error
    end
  end
end
