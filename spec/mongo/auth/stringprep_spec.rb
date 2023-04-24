# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::StringPrep do
  include Mongo::Auth::StringPrep

  describe '#prepare' do
    let(:prepared_data) do
      prepare(data, mappings, prohibited, options)
    end

    context 'with no options' do
      let(:mappings) do
        []
      end

      let(:prohibited) do
        []
      end

      let(:options) do
        {}
      end

      context 'when the data has invalid bidi' do
        let(:data) do
          "\u0627\u0031"
        end

        it 'does not raise an error' do
          expect(prepared_data).to eq("\u0627\u0031")
        end
      end

      context 'when the data has unicode codes' do
        let(:data) do
          "ua\u030Aer"
        end

        it 'does not normalize the data' do
          expect(prepared_data).to eq("ua\u030Aer")
        end
      end
    end

    context 'with options specified' do
      let (:mappings) do
        [Mongo::Auth::StringPrep::Tables::B1, Mongo::Auth::StringPrep::Tables::B2]
      end

      let (:prohibited) do
        [
          Mongo::Auth::StringPrep::Tables::C1_1,
          Mongo::Auth::StringPrep::Tables::C1_2,
          Mongo::Auth::StringPrep::Tables::C6,
        ]
      end

      let (:options) do
        {
          normalize: true,
          bidi: true,
        }
      end

      context 'when the input is empty' do
        let(:data) do
          ''
        end

        it 'returns the empty string' do
          expect(prepared_data).to eq('')
        end
      end

      context 'when the input is ASCII' do
        let(:data) do
          'user'
        end

        it 'returns the same string on ASCII input' do
          expect(prepared_data).to eq('user')
        end
      end

      context 'when the input contains zero-width spaces' do
        let(:data) do
          "u\u200Ber"
        end

        it 'removes the zero-width spaces' do
          expect(prepared_data).to eq('uer')
        end
      end

      context 'when the input contains non-ASCII characters' do
        let(:data) do
          "u\u00DFer"
        end

        it 'maps the non-ASCII characters to ASCII' do
          expect(prepared_data).to eq('usser')
        end
      end

      context 'when the input contains unicode codes' do
        let(:data) do
          "ua\u030Aer"
        end

        it 'unicode normalizes the input' do
          expect(prepared_data).to eq("u\u00e5er")
        end
      end

      context 'when the input contains prohibited characters' do
        let(:data) do
          "u\uFFFDer"
        end

        it 'raises an error' do
          expect {
            prepared_data
          }.to raise_error(Mongo::Error::FailedStringPrepValidation)
        end
      end

      context 'when the data is proper bidi' do
        let(:data) do
          "\u0627\u0031\u0628"
        end

        it 'does not raise an error' do
          expect(
            prepared_data
          ).to eq("\u0627\u0031\u0628")
        end
      end

      context 'when bidi input contains prohibited bidi characters' do
        let(:data) do
          "\u0627\u0589\u0628"
        end

        it 'raises an error' do
          expect {
            prepared_data
          }.to raise_error(Mongo::Error::FailedStringPrepValidation)
        end
      end

      context 'when bidi input has an invalid first bidi character' do
        let(:data) do
          "\u0031\u0627"
        end

        it 'raises an error' do
          expect {
            prepared_data
          }.to raise_error(Mongo::Error::FailedStringPrepValidation)
        end
      end

      context 'when bidi input has an invalid last bidi character' do
        let(:data) do
          "\u0627\u0031"
        end

        it 'raises an error' do
          expect {
            prepared_data
          }.to raise_error(Mongo::Error::FailedStringPrepValidation)
        end
      end

      context 'when bidi input has a bad character' do
        let(:data) do
          "\u206D"
        end

        it 'raises an error' do
          expect {
            prepared_data
          }.to raise_error(Mongo::Error::FailedStringPrepValidation)
        end
      end
    end
  end
end
