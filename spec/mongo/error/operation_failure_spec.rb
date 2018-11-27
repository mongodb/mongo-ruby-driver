require 'spec_helper'

describe Mongo::Error::OperationFailure do

  describe '#code' do
    subject do
      described_class.new('not master (10107)', nil,
        :code => 10107, :code_name => 'NotMaster')
    end

    it 'returns the code' do
      expect(subject.code).to eq(10107)
    end
  end

  describe '#code_name' do
    subject do
      described_class.new('not master (10107)', nil,
        :code => 10107, :code_name => 'NotMaster')
    end

    it 'returns the code name' do
      expect(subject.code_name).to eq('NotMaster')
    end
  end

  describe '#write_retryable?' do
    context 'when there is a read retryable message' do
      let(:error) { Mongo::Error::OperationFailure.new('problem: socket exception', nil) }

      it 'returns false' do
        expect(error.write_retryable?).to eql(false)
      end
    end

    context 'when there is a write retryable message' do
      let(:error) { Mongo::Error::OperationFailure.new('problem: node is recovering', nil) }

      it 'returns true' do
        expect(error.write_retryable?).to eql(true)
      end
    end

    context 'when there is a non-retryable message' do
      let(:error) { Mongo::Error::OperationFailure.new('something happened', nil) }

      it 'returns false' do
        expect(error.write_retryable?).to eql(false)
      end
    end

    context 'when there is a retryable code' do
      let(:error) { Mongo::Error::OperationFailure.new('no message', nil,
        :code => 91, :code_name => 'ShutdownInProgress') }

      it 'returns true' do
        expect(error.write_retryable?).to eql(true)
      end
    end

    context 'when there is a non-retryable code' do
      let(:error) { Mongo::Error::OperationFailure.new('no message', nil,
        :code => 43, :code_name => 'SomethingHappened') }

      it 'returns false' do
        expect(error.write_retryable?).to eql(false)
      end
    end
  end

  describe '#change_stream_resumable?' do
    context 'when there is a network error' do
      context 'getMore' do
        let(:error) { Mongo::Error::OperationFailure.new('problem: socket exception',
          Mongo::Operation::GetMore::Result.new([])) }

        it 'returns true' do
          expect(error.change_stream_resumable?).to be true
        end
      end

      context 'not getMore' do
        let(:error) { Mongo::Error::OperationFailure.new('problem: socket exception', nil) }

        it 'returns false' do
          expect(error.change_stream_resumable?).to be false
        end
      end
    end

    context 'when there is a resumable message' do
      context 'getMore response' do
        let(:error) { Mongo::Error::OperationFailure.new('problem: node is recovering',
          Mongo::Operation::GetMore::Result.new([])) }

        it 'returns true' do
          expect(error.change_stream_resumable?).to eql(true)
        end
      end

      context 'not a getMore response' do
        let(:error) { Mongo::Error::OperationFailure.new('problem: node is recovering', nil) }

        it 'returns false' do
          expect(error.change_stream_resumable?).to eql(false)
        end
      end
    end

    context 'when there is a resumable code' do
      context 'getMore response' do
        let(:error) { Mongo::Error::OperationFailure.new('no message',
          Mongo::Operation::GetMore::Result.new([]),
          :code => 91, :code_name => 'ShutdownInProgress') }

        it 'returns true' do
          expect(error.change_stream_resumable?).to eql(true)
        end
      end

      context 'not a getMore response' do
        let(:error) { Mongo::Error::OperationFailure.new('no message', nil,
          :code => 91, :code_name => 'ShutdownInProgress') }

        it 'returns false' do
          expect(error.change_stream_resumable?).to eql(false)
        end
      end
    end

    context 'when there is a non-resumable code' do
      context 'getMore response' do
        let(:error) { Mongo::Error::OperationFailure.new('no message',
          Mongo::Operation::GetMore::Result.new([]),
          :code => 136, :code_name => 'CappedPositionLost') }

        it 'returns false' do
          expect(error.change_stream_resumable?).to eql(false)
        end
      end

      context 'not a getMore response' do
        let(:error) { Mongo::Error::OperationFailure.new('no message', nil,
          :code => 136, :code_name => 'CappedPositionLost') }

        it 'returns false' do
          expect(error.change_stream_resumable?).to eql(false)
        end
      end
    end
  end

  describe '#labels' do

    context 'when the result is nil' do

      subject do
        described_class.new('not master (10107)', nil,
          :code => 10107, :code_name => 'NotMaster')
      end

      it 'has no labels' do
        expect(subject.labels).to eq([])
      end
    end

    context 'when the result is not nil' do

      let(:reply_document) do
        {
            'code' => 251,
            'codeName' => 'NoSuchTransaction',
            'errorLabels' => labels,
        }
      end

      let(:reply) do
        Mongo::Protocol::Reply.new.tap do |r|
          # Because this was not created by Mongo::Protocol::Reply::deserialize, we need to manually
          # initialize the fields.
          r.instance_variable_set(:@documents, [reply_document])
          r.instance_variable_set(:@flags, [])
        end
      end

      let(:result) do
        Mongo::Operation::Result.new(reply)
      end

      subject do
        begin
          result.send(:raise_operation_failure)
        rescue => e
          e
        end
      end

      context 'when the error has no labels' do

        let(:labels) do
          []
        end

        it 'has the correct labels' do
          expect(subject.labels).to eq(labels)
        end
      end


      context 'when the error has labels' do

        let(:labels) do
          [ Mongo::Error::TRANSIENT_TRANSACTION_ERROR_LABEL ]
        end

        it 'has the correct labels' do
          expect(subject.labels).to eq(labels)
        end
      end
    end
  end
end
