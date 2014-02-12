require 'spec_helper'

describe Mongo::Pool::Socket::SSL::Context do

  describe '.create' do

    let(:subject) do
      OpenSSL::X509::Name.parse("/DC=org/DC=mongodb/CN=TestCA")
    end

    let(:key) do
      Helpers::TEST_KEY_RSA1024
    end

    let(:now) do
      Time.at(Time.now.to_i)
    end

    let(:s) do
      0xdeadbeafdeadbeafdeadbeafdeadbeaf
    end

    let(:exts) do
      [
        ["basicConstraints","CA:TRUE,pathlen:1",true],
        ["keyUsage","keyCertSign, cRLSign",true],
        ["subjectKeyIdentifier","hash",false],
      ]
    end

    let(:digest) do
      OpenSSL::Digest::SHA1.new
    end

    let(:certificate) do
      issue_cert(subject, key, s, now, now + 3600, exts, nil, nil, digest)
    end

    let(:path) do
      '/path/to/cacert.pem'
    end

    context 'when ssl_cert is provided' do

      let(:context) do
        described_class.create(:ssl_cert => path)
      end

      before do
        expect(File).to receive(:open).with(path).and_return(certificate)
      end

      it 'sets the cert on the context 'do
        expect(context.cert.subject).to eq(certificate.subject)
      end
    end

    context 'when ssl_key is provided' do

      let(:context) do
        described_class.create(:ssl_key => path)
      end

      before do
        expect(File).to receive(:open).with(path).and_return(key)
      end

      it 'sets the cert on the context 'do
        expect(context.key.to_s).to eq(key.to_s)
      end
    end

    context 'when ssl_ca_cert is provided' do

      let(:ca_file) do
        '/path/to/cacert.pem'
      end

      let(:context) do
        described_class.create(:ssl_ca_cert => ca_file)
      end

      it 'sets the ca file'do
        expect(context.ca_file).to eq(ca_file)
      end

      it 'sets the verify mode' do
        expect(context.verify_mode).to eq(OpenSSL::SSL::VERIFY_PEER)
      end
    end
  end
end
