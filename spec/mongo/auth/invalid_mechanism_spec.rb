# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Auth::InvalidMechanism do
  describe 'message' do
    let(:exception) { described_class.new(:foo) }

    it 'includes all built in mechanisms' do
      expect(exception.message).to eq('Invalid authentication mechanism, please use one of the following mechanisms: :aws, :gssapi, :mongodb_cr, :mongodb_x509, :plain, :scram, :scram256')
    end

    it 'does not include the supplied mechanism' do
      expect(exception.message).not_to include('foo')
    end
  end
end
