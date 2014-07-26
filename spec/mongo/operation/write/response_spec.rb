require 'spec_helper'

# describe Mongo::Operation::Write::Response do

  # describe '#initialize' do

    # let(:response) do
      # described_class.new(reply)
    # end

    # context 'when the reply is successful' do

      # let(:reply) do
        # Mongo::Protocol::Reply.new
      # end

      # let(:documents) do
        # [{ 'ok' => 1.0, 'n' => 1 }]
      # end

      # before do
        # reply.instance_variable_set(:@documents, documents)
      # end

      # it 'sets the documents on the response' do
        # expect(response.documents).to eq(documents)
      # end

      # it 'sets the written count' do
        # expect(response.n).to eq(1)
      # end
    # end

    # context 'when the reply is not successful' do

      # let(:reply) do
        # Mongo::Protocol::Reply.new
      # end

      # let(:documents) do
        # [{ 'ok' => 0.0, 'n' => 1 }]
      # end

      # before do
        # reply.instance_variable_set(:@documents, documents)
      # end

      # it 'raises an exception' do
        # expect { response }.to raise_error
      # end
    # end

    # context 'when the reply is nil' do

      # let(:reply) { nil }

      # it 'does not raise an exception' do
        # expect(response.documents).to be_empty
      # end

      # it 'does not set the written count' do
        # expect(response.n).to be_nil
      # end
    # end

    # context 'when providing a count' do

      # let(:reply) do
        # Mongo::Protocol::Reply.new
      # end

      # let(:response) do
        # described_class.new(reply, 5)
      # end

      # before do
        # reply.instance_variable_set(:@documents, [{ 'ok' => 1 }])
      # end

      # it 'sets the document count' do
        # expect(response.n).to eq(5)
      # end
    # end
  # end
# end
