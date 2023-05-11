# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Aws::Request do

  describe "#formatted_time" do
    context "when time is provided and frozen" do
      let(:original_time) { Time.at(1592399523).freeze }
      let(:request) do
        described_class.new(access_key_id: 'access_key_id',
          secret_access_key: 'secret_access_key',
          session_token: 'session_token',
          host: 'host',
          server_nonce: 'server_nonce',
          time: original_time
        )
      end

      it 'doesn\'t modify the time instance variable' do
        expect { request.formatted_time }.to_not raise_error
      end

      it 'returns the correct formatted time' do
        expect(request.formatted_time).to eq('20200617T131203Z')
      end
    end

    context "when time is not provided" do
      let(:request) do
        described_class.new(access_key_id: 'access_key_id',
          secret_access_key: 'secret_access_key',
          session_token: 'session_token',
          host: 'host',
          server_nonce: 'server_nonce'
        )
      end

      it 'doesn\'t raise an error on formatted_time' do
        expect { request.formatted_time }.to_not raise_error
      end
    end
  end

  describe "#signature" do
    context "when time is provided and frozen" do
      let(:original_time) { Time.at(1592399523).freeze }
      let(:request) do
        described_class.new(access_key_id: 'access_key_id',
          secret_access_key: 'secret_access_key',
          session_token: 'session_token',
          host: 'host',
          server_nonce: 'server_nonce',
          time: original_time
        )
      end

      it 'doesn\'t raise error on signature' do
        expect { request.signature }.to_not raise_error
      end
    end

    context "when time is not provided" do
      let(:request) do
        described_class.new(access_key_id: 'access_key_id',
          secret_access_key: 'secret_access_key',
          session_token: 'session_token',
          host: 'host',
          server_nonce: 'server_nonce'
        )
      end

      it 'doesn\'t raise error on signature' do
        expect { request.signature }.to_not raise_error
      end
    end
  end
end
