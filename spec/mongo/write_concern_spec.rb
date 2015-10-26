require 'spec_helper'

describe Mongo::WriteConcern do

  describe '#get' do

    context 'when w is 0' do

      context 'when no other options are provided' do

        let(:options) do
          { w: 0 }
        end

        it 'returns an Unacknowledged write concern object' do
          expect(Mongo::WriteConcern.get(options)).to be_a(Mongo::WriteConcern::Unacknowledged)
        end
      end

      context 'when j is also provided' do

        context 'when j is false' do

          let(:options) do
            { w: 0, j: false }
          end

          it 'returns an Unacknowledged write concern object' do
            expect(Mongo::WriteConcern.get(options)).to be_a(Mongo::WriteConcern::Unacknowledged)
          end
        end

        context 'when j is true' do

          let(:options) do
            { w: 0, j: true }
          end

          it 'raises an exception' do
            expect {
              Mongo::WriteConcern.get(options)
            }.to raise_error(Mongo::Error::InvalidWriteConcern)
          end
        end

        context 'when fsync is true' do

          let(:options) do
            { w: 0, fsync: true }
          end

          it 'raises an exception' do
            expect {
              Mongo::WriteConcern.get(options)
            }.to raise_error(Mongo::Error::InvalidWriteConcern)
          end
        end
      end

      context 'when wtimeout is also provided' do

        let(:options) do
          { w: 0, wimteout: 100 }
        end

        it 'returns an Unacknowledged write concern object' do
          expect(Mongo::WriteConcern.get(options)).to be_a(Mongo::WriteConcern::Unacknowledged)
        end
      end
    end

    context 'when w is less than 0' do

      let(:options) do
        { w: -1 }
      end

      it 'raises an exception' do
        expect {
          Mongo::WriteConcern.get(options)
        }.to raise_error(Mongo::Error::InvalidWriteConcern)
      end
    end
  end
end
