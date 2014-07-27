require 'spec_helper'

describe Mongo::Operation::Command do

  let(:selector) { { :ismaster => 1 } }
  let(:opts) { { :limit => -1 } }
  let(:spec) do
    { :selector => selector,
      :opts     => opts,
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
          :opts => {},
          :db_name => 'test',
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
    end
  end

  context '#merge' do

    let(:other_op) { described_class.new(spec) }

    it 'raises an exception' do
      expect{ op.merge(other_op) }.to raise_exception
    end
  end

  context '#merge!' do

    let(:other_op) { described_class.new(spec) }

    it 'raises an exception' do
      expect{ op.merge!(other_op) }.to raise_exception
    end
  end

  describe '#execute' do

    let(:client) do
      Mongo::Client.new(
        [ '127.0.0.1:27017' ],
        database: TEST_DB,
        username: 'root-user',
        password: 'password'
      )
    end


    let(:server) do
      client.cluster.servers.first
    end

    before do
      # @todo: Replace with condition variable
      client.cluster.scan!
    end

    context 'when the command succeeds' do

      let(:response) do
        op.execute(server.context)
      end

      it 'returns the reponse' do
        expect(response).to be_ok
      end
    end

    context 'when the command fails' do

      let(:selector) do
        { notacommand: 1 }
      end

      it 'raises an exception' do
        expect {
          op.execute(server.context)
        }.to raise_error(Mongo::Operation::Write::Failure)
      end
    end

    context 'when the command cannot run on a secondary' do

      context 'when the server is a secondary' do

        pending 'it re-routes to the primary'
      end
    end
  end
end
