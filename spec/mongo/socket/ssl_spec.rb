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
            :ssl_key => CLIENT_PEM,
            :ssl_verify => false
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
            :ssl_key => CRL_PEM,
            :ssl_verify => false
        }
      end

      it 'raises an exception' do
        expect {
          socket.connect!
        }.to raise_error
      end
    end

    context 'when a CA certificate is provided', if: testing_ssl_locally? do

      let(:options) do
        {
            :ssl => true,
            :ssl_cert => CLIENT_PEM,
            :ssl_key => CLIENT_PEM,
            :ssl_ca_cert => CA_PEM,
            :ssl_verify => true
        }
      end

      before do
        socket.connect!
      end

      it 'connects to the server' do
        expect(socket).to be_alive
      end
    end

    context 'when a CA certificate is not provided', if: testing_ssl_locally? do

      let(:options) do
        {
            :ssl => true,
            :ssl_cert => CLIENT_PEM,
            :ssl_key => CLIENT_PEM,
            :ssl_verify => true
        }
      end

      before do
        ENV['SSL_CERT_FILE']= CA_PEM
        socket.connect!
      end

      it 'uses the default cert store' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is not specified', if: testing_ssl_locally? do

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

      it 'verifies the server certificate' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is true', if: testing_ssl_locally? do

      let(:options) do
        {
            :ssl => true,
            :ssl_cert => CLIENT_PEM,
            :ssl_key => CLIENT_PEM,
            :ssl_ca_cert => CA_PEM,
            :ssl_verify => true
        }
      end

      before do
        socket.connect!
      end

      it 'verifies the server certificate' do
        expect(socket).to be_alive
      end
    end

    context 'when ssl_verify is false' do

      let(:options) do
        {
            :ssl => true,
            :ssl_cert => CLIENT_PEM,
            :ssl_key => CLIENT_PEM,
            :ssl_ca_cert => 'invalid',
            :ssl_verify => false
        }
      end

      before do
        socket.connect!
      end

      it 'does not verify the server certificate' do
        expect(socket).to be_alive
      end
    end
  end
end
