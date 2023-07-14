# frozen_string_literal: true
# encoding: utf-8

require 'mongo'
require 'lite_spec_helper'

describe 'Clean exit' do
  require_external_connectivity
  require_solo

  context 'with SRV URI' do

    let(:uri) do
      'mongodb+srv://test1.test.build.10gen.cc/?tls=false'
    end

    it 'exits cleanly' do
      client = Mongo::Client.new(uri)
      client.database.collection_names.to_a
    end
  end
end
