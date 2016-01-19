require 'spec_helper'

describe Mongo::WriteConcern do

  describe '#get' do

    context 'when no options are set' do

      let(:options) do
        { }
      end

      it 'returns an Acknowledged write concern object' do
        expect(Mongo::WriteConcern.get(options)).to be_a(Mongo::WriteConcern::Acknowledged)
      end
    end

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

    context 'when w is greater than 0' do

      let(:options) do
        { w: 2, journal: true }
      end

      it 'returns an Acknowledged write concern object' do
        expect(Mongo::WriteConcern.get(options)).to be_a(Mongo::WriteConcern::Acknowledged)
      end

      it 'sets the options' do
        expect(Mongo::WriteConcern.get(options).options).to eq(options)
      end
    end

    context 'when w is a string' do

      let(:options) do
        { w: 'majority', journal: true }
      end

      it 'returns an Acknowledged write concern object' do
        expect(Mongo::WriteConcern.get(options)).to be_a(Mongo::WriteConcern::Acknowledged)
      end

      it 'sets the options' do
        expect(Mongo::WriteConcern.get(options).options).to eq(options)
      end
    end

    context 'when w is a symbol' do

      let(:options) do
        { w: :majority, journal: true }
      end

      it 'returns an Acknowledged write concern object' do
        expect(Mongo::WriteConcern.get(options)).to be_a(Mongo::WriteConcern::Acknowledged)
      end

      it 'sets w to a string' do
        expect(Mongo::WriteConcern.get(options).options[:w]).to eq('majority')
      end
    end
  end
end
