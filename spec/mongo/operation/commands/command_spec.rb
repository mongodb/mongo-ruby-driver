require 'spec_helper'

describe Mongo::Operation::Commands::Command do

  let(:selector) { { :ismaster => 1 } }
  let(:options) { { :limit => -1 } }
  let(:spec) do
    { :selector => selector,
      :options     => options,
      :db_name  => TEST_DB
    }
  end
  let(:op) { described_class.new(spec) }

  describe '#initialize' do

    it 'sets the spec' do
      expect(op.spec).to be(spec)
    end
  end

  describe '#==' do

    context 'when the ops have different specs' do

      let(:other_selector) { { :ping => 1 } }
      let(:other_spec) do
        { :selector => other_selector,
          :options => {},
          :db_name => 'test',
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  describe '#execute' do

    context 'when the command succeeds' do

      let(:response) do
        op.execute(authorized_primary.context)
      end

      it 'returns the reponse' do
        expect(response).to be_successful
      end
    end

    context 'when the command fails' do

      let(:selector) do
        { notacommand: 1 }
      end

      it 'raises an exception' do
        expect {
          op.execute(authorized_primary.context)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'when a document exceeds max bson size' do

      let(:selector) do
        { :ismaster => '1'*17000000 }
      end

      it 'raises an error' do
        expect {
          op.execute(authorized_primary.context)
        }.to raise_error(Mongo::Error::MaxBSONSize)
      end
    end
  end
end
