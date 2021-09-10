# frozen_string_literal: true
# encoding: utf-8

require 'lite_spec_helper'

describe 'Atlas connectivity' do
  let(:uri) { ENV['ATLAS_URI'] }
  let(:client) { Mongo::Client.new(uri) }

  before do
    if uri.nil?
      skip "ATLAS_URI not set in environment"
    end
  end

  describe 'connection to Atlas' do
    it 'runs ismaster successfully' do
      result = client.database.command(:ismaster => 1)
      expect(result.documents.first['ismaster']).to be true
    end

    it 'runs findOne successfully' do
      result = client.use(:test)['test'].find.to_a
      expect(result).to be_a(Array)
    end
  end
end
