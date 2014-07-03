require 'spec_helper'

describe Mongo::Operation::MapReduce do
  include_context 'operation'

  let(:opts) { {} }
  let(:selector) do
    { :mapreduce => 'test_coll',
      :map       => '',
      :reduce    => '',
    }
  end
  let(:spec) do
    { :selector => selector,
      :opts     => opts,
      :db_name  => db_name
    }
  end
  let(:op) { described_class.new(spec) }

  describe '#initialize' do

    context 'spec' do

      it 'sets the spec' do
        expect(op.spec).to be(spec)
      end
    end
  end

  describe '#==' do

    context ' when two ops have different specs' do
      let(:other_selector) do
        { :mapreduce => 'other_test_coll',
          :map => '',
          :reduce => '',
        }
      end
      let(:other_spec) do
        { :selector => other_selector,
          :opts => {},
          :db_name => db_name,
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

    it 'is not allowed' do
      expect{ op.merge(other_op) }.to raise_exception
    end
  end

  context '#merge!' do
    let(:other_op) { described_class.new(spec) }

    it 'is not allowed' do
      expect{ op.merge!(other_op) }.to raise_exception
    end
  end

  describe '#execute' do

    context 'message' do

      it 'creates a query wire protocol message with correct specs' do
        allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
          primary_server
        end

        expect(Mongo::Protocol::Query).to receive(:new) do |db, coll, sel, options|
          expect(db).to eq(db_name)
          expect(coll).to eq(Mongo::Database::COMMAND)
          expect(sel).to eq(selector)
          expect(options).to eq(opts)
        end
        op.execute(primary_context)
      end
    end

    context 'connection' do
      let(:selector) do
        { :mapreduce => 'test_coll',
          :map       => '',
          :reduce    => ''
        }
      end

      it 'dispatches the message on the connection' do
        allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
          primary_server
        end

        expect(connection).to receive(:dispatch)
        op.execute(primary_context)
      end
    end

    context 'rerouting' do

      context 'when out is inline and server is a secondary' do
        let(:selector) do
          { :mapreduce => 'test_coll',
            :map       => '',
            :reduce    => '',
            :out       => 'inline'
          }
        end

        it 'sends the operation to the secondary' do
          allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
            primary_server
          end
          expect(secondary_context).to receive(:with_connection)
          op.execute(secondary_context)
        end
      end

      context 'when out is a collection and server is a secondary' do
        let(:selector) do
          { :mapreduce => 'test_coll',
            :map       => '',
            :reduce    => '',
            :out       => 'other_coll'
          }
        end

        it 'reroutes the operation to the primary' do
          allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
            primary_server
          end
          expect(primary_context).to receive(:with_connection)
          op.execute(secondary_context)
        end
      end
    end
  end
end
