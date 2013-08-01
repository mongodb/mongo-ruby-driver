shared_examples 'shared socket behavior' do

  describe '#read' do
    before { allow(socket).to receive(:read).and_return(Object.new) }

    context 'when an exception occurs in Socket#read' do
      before do
        allow(object).to receive(:alive?).and_return(true)
        object.connect
      end

      it 'raises a Mongo::SocketTimeoutError for Errno::ETIMEDOUT' do
        allow_any_instance_of(::Socket).to receive(:read) do
          raise Errno::ETIMEDOUT
        end
        expect { object.read(4096) }.to raise_error(Mongo::SocketTimeoutError)
      end

      it 'raises a Mongo::SocketError for IOError' do
        allow_any_instance_of(::Socket).to receive(:read) { raise IOError }
        expect { object.read(4096) }.to raise_error(Mongo::SocketError)
      end

      it 'raises a Mongo::SocketError for SystemCallError' do
        allow_any_instance_of(::Socket).to receive(:read) do
          raise SystemCallError, 'Oh god. Everything is ruined.'
        end
        expect { object.read(4096) }.to raise_error(Mongo::SocketError)
      end

      it 'raises a Mongo::SocketError for OpenSSL::SSL::SSLError' do
        allow_any_instance_of(::Socket).to receive(:read) do
          raise OpenSSL::SSL::SSLError
        end
        expect { object.read(4096) }.to raise_error(Mongo::SocketError)
      end
    end
  end

  describe '#write' do
    let(:payload) { Object.new }

    before { allow(socket).to receive(:write).and_return(1024) }

    context 'when an exception occurs in Socket#write' do
      before do
        allow(object).to receive(:alive?).and_return(true)
        object.connect
      end

      it 'raises a Mongo::SocketTimeoutError for Errno::ETIMEDOUT' do
        allow_any_instance_of(::Socket).to receive(:write) do
          raise Errno::ETIMEDOUT
        end

        expect do
          object.write(payload)
        end.to raise_error(Mongo::SocketTimeoutError)
      end

      it 'raises a Mongo::SocketError for IOError' do
        allow_any_instance_of(::Socket).to receive(:write) { raise IOError }
        expect { object.write(payload) }.to raise_error(Mongo::SocketError)
      end

      it 'raises a Mongo::SocketError for SystemCallError' do
        allow_any_instance_of(::Socket).to receive(:write) do
          raise SystemCallError, 'Oh god. Everything is ruined.'
        end
        expect { object.write(payload) }.to raise_error(Mongo::SocketError)
      end

      it 'raises a Mongo::SocketError for OpenSSL::SSL::SSLError' do
        allow_any_instance_of(::Socket).to receive(:write) do
          raise OpenSSL::SSL::SSLError
        end
        expect { object.write(payload) }.to raise_error(Mongo::SocketError)
      end
    end
  end

end
