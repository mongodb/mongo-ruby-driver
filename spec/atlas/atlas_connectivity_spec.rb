# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe 'Atlas connectivity' do
  let(:uri) { ENV['ATLAS_URI'] }
  let(:client) { Mongo::Client.new(uri) }

  require_atlas

  describe 'connection to Atlas' do
    after do
      client.close
    end

    it 'runs ismaster successfully' do
      expect { client.database.command(:hello => 1) }
        .not_to raise_error
    end

    it 'runs findOne successfully' do
      expect { client.use(:test)['test'].find.to_a }
        .not_to raise_error
    end
  end
end
