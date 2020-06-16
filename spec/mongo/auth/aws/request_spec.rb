require 'spec_helper'

describe Mongo::Auth::Aws::Request do 

  describe "#formatted_time" do 
    original_time = Time.now.freeze
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
  end

  describe "#signature" do 
    let(:request) do 
      described_class.new(access_key_id: 'access_key_id', 
        secret_access_key: 'secret_access_key', 
        session_token: 'session_token', 
        host: 'host', 
        server_nonce: 'server_nonce',
      )
    end

    it 'doesn\'t raise exception on signature' do 
      expect { request.signature }.to_not raise_error
    end
  end
end
