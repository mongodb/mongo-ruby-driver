shared_examples 'message with a header' do
  describe 'header' do
    describe 'length' do
      let(:field) { bytes[0..3] }
      it 'serializes the length' do
        expect(field).to be_int32(bytes.size)
      end
    end

    describe 'request id' do
      let(:field) { bytes[4..7] }
      it 'serializes the request id' do
        expect(field).to be_int32(message.request_id)
      end
    end

    describe 'response to' do
      let(:field) { bytes[8..11] }
      it 'serializes the response to' do
        expect(field).to be_int32(0)
      end
    end

    describe 'op code' do
      let(:field) { bytes[12..15] }
      it 'serializes the op code' do
        expect(field).to be_int32(opcode)
      end
    end
  end
end
