require 'spec_helper'
require 'json'

describe Mongo::Auth::Aws::Request do 

    describe "#formatted_time" do 
        let(:request) do 
            described_class.new(access_key_id: 'access_key_id', 
                secret_access_key: 'secret_access_key', 
                session_token: 'session_token', 
                host: 'host', 
                server_nonce: 'server_nonce'
            )
        end

        it 'doesn\'t modify the time instance variable' do
            original_time = request.time.dup
            request.formatted_time
            expect(request.time).to eq(original_time)
            expect(request.time.strftime('%Z')).to eq(original_time.strftime('%Z'))

        end
    end
end