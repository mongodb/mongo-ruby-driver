# frozen_string_literal: true

require 'spec_helper'

describe 'adaptiveRetries option' do
  describe 'client option' do
    it 'defaults to nil (not set)' do
      client = new_local_client_nmio(['localhost:27017'])
      expect(client.options[:adaptive_retries]).to be_nil
    end

    it 'can be set to true' do
      client = new_local_client_nmio(['localhost:27017'], adaptive_retries: true)
      expect(client.options[:adaptive_retries]).to be true
    end

    it 'can be set to false' do
      client = new_local_client_nmio(['localhost:27017'], adaptive_retries: false)
      expect(client.options[:adaptive_retries]).to be false
    end
  end

  describe 'URI option' do
    it 'parses adaptiveRetries=true' do
      client = new_local_client_nmio('mongodb://localhost:27017/?adaptiveRetries=true')
      expect(client.options[:adaptive_retries]).to be true
    end

    it 'parses adaptiveRetries=false' do
      client = new_local_client_nmio('mongodb://localhost:27017/?adaptiveRetries=false')
      expect(client.options[:adaptive_retries]).to be false
    end
  end
end
