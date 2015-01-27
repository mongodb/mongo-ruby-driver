require 'spec_helper'

describe Mongo::Operation::Aggregate do
  include_context 'operation'

  let(:selector) do
    { :aggregate => coll_name,
      :pipeline => [],
    }
  end
  let(:spec) do
    { :selector => selector,
      :options => {},
      :db_name => db_name
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
        { :aggregate => 'another_test_coll',
          :pipeline => [],
        }
      end
      let(:other_spec) do
        { :selector => other_selector,
          :options => options,
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
        allow_any_instance_of(Mongo::ReadPreference::Primary).to receive(:server) do
          primary_server
        end

        expect(Mongo::Protocol::Query).to receive(:new) do |db, coll, sel, options|
          expect(db).to eq(db_name)
          expect(coll).to eq(Mongo::Database::COMMAND)
          expect(sel).to eq(selector)
          expect(options).to eq(options)
        end
        op.execute(primary_context)
      end
    end

    context 'connection' do

      it 'dispatches the message on the connection' do
        allow_any_instance_of(Mongo::ReadPreference::Primary).to receive(:server) do
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

        it 'raises an error' do
          allow_any_instance_of(Mongo::ReadPreference::Primary).to receive(:server) do
            primary_server
          end
          expect {
            op.execute(secondary_context)
          }.to raise_error(Mongo::Operation::Aggregate::NeedPrimaryServer)
        end
      end

      context 'when out is specified and server is a primary' do
        let(:selector) do
          { :aggregate => 'test_coll',
            :pipeline => [{ '$out' => 'test_coll' }],
          }
        end

        it 'sends the operation to the primary' do
          allow_any_instance_of(Mongo::ReadPreference::Primary).to receive(:server) do
            primary_server
          end
          expect(primary_context).to receive(:with_connection)
          op.execute(primary_context)
        end
      end
    end
  end
end
