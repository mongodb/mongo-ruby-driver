# frozen_string_literal: true
# encoding: utf-8

require 'mongo'

describe 'Clean exit' do

  before(:all) do
    unless %w(1 true yes).include?(ENV['SOLO'])
      skip 'Set SOLO=1 in environment to run solo tests'
    end

    if %w(1 true yes).include?(ENV['EXTERNAL_DISABLED'])
      skip "Test requires external connectivity"
    end
  end

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
