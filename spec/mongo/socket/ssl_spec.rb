require 'spec_helper'

describe Mongo::Socket::SSL do

  describe '#connect!', if: running_ssl? do

    let(:socket) do
      described_class.new(*DEFAULT_ADDRESS.split(":"), DEFAULT_ADDRESS.split(":")[0], 5, Socket::PF_INET, options)
    end

    context 'when a certificate is provided' do

      let(:options) do
        {
          :ssl => true,
          :ssl_cert => CLIENT_PEM,
          :ssl_key => CLIENT_PEM
        }
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end

    context 'when a bad certificate is provided' do

      let(:options) do
        {
          :ssl => true,
          :ssl_cert => CLIENT_PEM,
          :ssl_key => CRL_PEM
        }
      end

      it 'raises an exception' do
        expect {
          socket.connect!
        }.to raise_error
      end
    end

    context 'when a CA certificate is provided', if: running_ssl? && testing_locally? do

      let(:options) do
        {
            :ssl => true,
            :ssl_cert => CLIENT_PEM,
            :ssl_key => CLIENT_PEM,
            :ssl_ca_cert => CA_PEM
        }
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end
  end
end
