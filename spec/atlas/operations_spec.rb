# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe 'Operations' do
  let(:uri) { ENV['ATLAS_URI'] }
  let(:client) { Mongo::Client.new(uri) }

  require_atlas

  describe 'ping' do
    it 'works' do
      expect do
        client.database.command(ping: 1)
      end.not_to raise_error
    end
  end
end
