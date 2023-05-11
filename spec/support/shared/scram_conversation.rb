# frozen_string_literal: true
# rubocop:todo all

shared_context 'scram conversation context' do
  let(:connection) do
    double('connection').tap do |connection|
      features = double('features')
      allow(features).to receive(:op_msg_enabled?).and_return(true)
      allow(connection).to receive(:features).and_return(features)
      allow(connection).to receive(:server)
      allow(connection).to receive(:mongos?)
    end
  end
end

shared_examples 'scram conversation' do

  describe '#parse_payload' do
    let(:user) { double('user') }
    let(:mechanism) { :scram }

    shared_examples_for 'parses as expected' do
      it 'parses as expected' do
        conversation.send(:parse_payload, payload).should == expected
      end
    end

    context 'regular payload' do
      let(:payload) { 'foo=bar,hello=world' }
      let(:expected) do
        {'foo' => 'bar', 'hello' => 'world'}
      end

      it_behaves_like 'parses as expected'
    end

    context 'equal signs in value' do
      let(:payload) { 'foo=bar==,hello=world=is=great' }
      let(:expected) do
        {'foo' => 'bar==', 'hello' => 'world=is=great'}
      end

      it_behaves_like 'parses as expected'
    end

    context 'missing value' do
      let(:payload) { 'foo=,hello=' }
      let(:expected) do
        {'foo' => '', 'hello' => ''}
      end

      it_behaves_like 'parses as expected'
    end

    context 'missing key/value pair' do
      let(:payload) { 'foo=,,hello=' }
      let(:expected) do
        {'foo' => '', 'hello' => ''}
      end

      it_behaves_like 'parses as expected'
    end

    context 'missing key' do
      let(:payload) { '=bar' }

      it 'raises an exception' do
        lambda do
          conversation.send(:parse_payload, payload)
        end.should raise_error(Mongo::Error::InvalidServerAuthResponse, /Payload malformed: missing key/)
      end
    end

    context 'all keys missing' do
      let(:payload) { ',,,' }
      let(:expected) do
        {}
      end

      it_behaves_like 'parses as expected'
    end
  end
end

shared_context 'scram continue and finalize replies' do

  let(:continue_document) do
    BSON::Document.new(
      'conversationId' => 1,
      'done' => false,
      'payload' => continue_payload,
      'ok' => 1.0
    )
  end

  let(:finalize_document) do
    BSON::Document.new(
      'conversationId' => 1,
      'done' => false,
      'payload' => finalize_payload,
      'ok' => 1.0
    )
  end
end
