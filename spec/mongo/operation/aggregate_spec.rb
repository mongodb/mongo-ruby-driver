require 'spec_helper'

describe Mongo::Operation::Aggregate do

  let(:opts) { {} }
  let(:selector) do
    { :aggregate => 'test_coll',
      :pipeline => [],
    }
  end
  let(:db_name) { 'TEST_DB' }
  let(:spec) do
    { :selector => selector,
      :opts => {},
      :db_name => db_name
    }
  end
  let(:op) { described_class.new(spec) }

  let(:secondary_server) do
    double('secondary_server').tap do |s|
      allow(s).to receive(:secondary?) { true }
    end
  end
  let(:primary_server) do
    double('primary_server').tap do |s|
      allow(s).to receive(:secondary?) { false }
      allow(s).to receive(:context) { primary_context }
    end
  end
  let(:primary_context) do
    double('primary_context').tap do |cxt|
      allow(cxt).to receive(:with_connection).and_yield(connection)
      allow(cxt).to receive(:server) { primary_server }
    end
  end
  let(:secondary_context) do
    double('secondary_context').tap do |cxt|
      allow(cxt).to receive(:with_connection).and_yield(connection)
      allow(cxt).to receive(:server) do
        secondary_server
      end
    end
  end
  let(:connection) do
    double('connection').tap do |conn|
      allow(conn).to receive(:dispatch) { [] }
    end
  end

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
        { :aggregate => 'another_test_coll',
          :pipeline => [],
        }
      end
      let(:other_spec) do
        { :selector => other_selector,
          :opts => opts,
          :db_name => db_name,
        }
      end
      let(:other) { described_class.new(other_spec) }

      it 'returns false' do
        expect(op).not_to eq(other)
      end
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
          expect(coll).to eq(Mongo::Operation::COMMAND_COLLECTION_NAME)
          expect(sel).to eq(selector)
          expect(options).to eq(opts)
        end
        op.execute(primary_context)
      end
    end

    context 'connection' do
      let(:selector) do
        { :aggregate => 'test_coll',
          :pipeline => [],
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

      context 'when out is specified and server is a secondary' do
        let(:selector) do
          { :aggregate => 'test_coll',
            :pipeline => [{ '$out' => 'test_coll' }],
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

      context 'when out is specified and server is a primary' do
        let(:selector) do
          { :aggregate => 'test_coll',
            :pipeline => [{ '$out' => 'test_coll' }],
          }
        end

        it 'sends the operation to the primary' do
          allow_any_instance_of(Mongo::ServerPreference::Primary).to receive(:server) do
            primary_server
          end
          expect(primary_context).to receive(:with_connection)
          op.execute(primary_context)
        end
      end
    end
  end
end
