# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::WriteConcern do

  describe '#get' do

    let(:wc) { Mongo::WriteConcern.get(options) }

    context 'when no options are set' do

      let(:options) do
        { }
      end

      it 'returns an Acknowledged write concern object' do
        expect(Mongo::WriteConcern.get(options)).to be_a(Mongo::WriteConcern::Acknowledged)
      end
    end

    context 'when the value is a WriteConcern object' do

      let(:value) do
        Mongo::WriteConcern.get({})
      end

      it 'returns the object' do
        expect(Mongo::WriteConcern.get(value)).to be(value)
      end
    end

    context 'when the value is nil' do

      it 'returns nil' do
        expect(Mongo::WriteConcern.get(nil)).to be(nil)
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

          context 'when j is given as a string' do

            let(:options) do
              { w: 0, 'j' => true }
            end

            it 'raises an exception' do
              expect {
                Mongo::WriteConcern.get(options)
              }.to raise_error(Mongo::Error::InvalidWriteConcern)
            end
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
        { w: 2, j: true }
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
        { w: 'majority', j: true }
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
        { w: :majority, j: true }
      end

      it 'returns an Acknowledged write concern object' do
        expect(Mongo::WriteConcern.get(options)).to be_a(Mongo::WriteConcern::Acknowledged)
      end

      it 'sets w to a string' do
        expect(Mongo::WriteConcern.get(options).options[:w]).to eq('majority')
      end
    end

    context 'when options are provided with string keys' do

      context 'acknowledged write concern' do
        let(:options) do
          { 'w' => 2, 'j' => true }
        end

        it 'converts keys to symbols' do
          expect(wc).to be_a(Mongo::WriteConcern::Acknowledged)
          expect(wc.options[:w]).to eq(2)
          expect(wc.options[:j]).to be true
        end
      end

      context 'unacknowledged write concern' do
        let(:options) do
          { 'w' => 0 }
        end

        it 'converts keys to symbols' do
          expect(wc).to be_a(Mongo::WriteConcern::Unacknowledged)
          expect(wc.options[:w]).to eq(0)
        end

        context 'and j is true' do
          let(:options) do
            { 'w' => 0, j: true }
          end

          it 'raises an exception' do
            expect do
              wc
            end.to raise_error(Mongo::Error::InvalidWriteConcern, /:j cannot be true when :w is 0/)
          end
        end
      end
    end

    context 'when :journal option is given' do
      let(:options) do
        { 'w' => 1, journal: true }
      end

      it 'raises an exception' do
        expect do
          wc
        end.to raise_error(Mongo::Error::InvalidWriteConcern, /use :j for journal/)
      end
    end
  end
end
