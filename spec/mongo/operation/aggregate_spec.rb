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

    context 'when the aggregation fails' do

      let(:selector) do
        { :aggregate => coll_name,
          :pipeline => [{ '$invalid' => 'operator' }],
        }
      end

      it 'raises an exception' do
        expect {
          op.execute(authorized_primary.context)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end

    context 'rerouting' do

      before do
        allow_any_instance_of(Mongo::Operation::Aggregate::Result).to receive(:validate!) do
          true
        end
      end

      context 'when out is specified and server is a secondary' do
        let(:selector) do
          { :aggregate => 'test_coll',
            :pipeline => [{ '$out' => 'test_coll' }],
          }
        end

        it 'raises an error' do
          allow_any_instance_of(Mongo::ServerSelector::Primary).to receive(:server) do
            primary_server
          end
          expect {
            op.execute(secondary_context)
          }.to raise_error(Mongo::Error::NeedPrimaryServer)
        end
      end

      context 'when out is specified and server is a primary' do
        let(:selector) do
          { :aggregate => 'test_coll',
            :pipeline => [{ '$out' => 'test_coll' }],
          }
        end

        it 'sends the operation to the primary' do
          allow_any_instance_of(Mongo::ServerSelector::Primary).to receive(:server) do
            primary_server
          end
          expect(primary_context).to receive(:with_connection)
          op.execute(primary_context)
        end
      end
    end
  end
end
