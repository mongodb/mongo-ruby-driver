require 'spec_helper'

describe Mongo::Indexable do

  let(:client) do
    Mongo::Client.new(
      [ '127.0.0.1:27017' ],
      database: TEST_DB,
      username: ROOT_USER.name,
      password: ROOT_USER.password
    )
  end

  before do
    client.cluster.scan!
  end

  describe '#ensure_index' do

    let(:indexable) do
      client[TEST_COLL]
    end

    context 'when the index is created' do

      let(:spec) do
        { randomfield: 1 }
      end

      let(:result) do
        indexable.ensure_index(spec, unique: true)
      end

      it 'returns ok' do
        expect(result).to be_ok
      end
    end

    context 'when index creation fails' do

      let(:spec) do
        { name: 1 }
      end

      it 'raises an exception' do
        expect {
          indexable.ensure_index(spec, unique: true)
        }.to raise_error(Mongo::Operation::Write::Failure)
      end
    end
  end
end
