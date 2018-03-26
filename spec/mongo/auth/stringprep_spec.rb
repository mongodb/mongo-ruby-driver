require 'spec_helper'

describe Mongo::Auth::StringPrep do
  include Mongo::Auth::StringPrep

  describe '#prepare' do
    context 'with no options' do
      it 'does not check bidi' do
        expect(prepare("\u0627\u0031", [], [])).to eq("\u0627\u0031")
      end

      it 'does not unicode normalize' do
        expect(prepare("ua\u030Aer", [], [])).to eq("ua\u030Aer")
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

      it 'returns the empty string on empty input' do
        expect(prepare('', mappings, prohibited, options)).to eq('')
      end

      it 'returns the same string on ASCII input' do
        expect(prepare('user', mappings, prohibited, options)).to eq('user')
      end

      it 'removes zero-width spaces from the input' do
        expect(prepare("u\u200Ber", mappings, prohibited, options)).to eq('uer')
      end

      it 'maps non-ASCII characters to ASCII' do
        expect(prepare("u\u00DFer", mappings, prohibited, options)).to eq('usser')
      end

      it 'unicode normalizes the input' do
        expect(prepare("ua\u030Aer", mappings, prohibited, options)).to eq("u\u00e5er")
      end

      it 'raises an error on prohibited input' do
        expect {
          prepare("u\uFFFDer", mappings, prohibited, options)
        }.to raise_error(Mongo::Error::FailedStringPrepOperation)
      end

      it 'does not raise an error on proper bidi input' do
        expect(
          prepare("\u0627\u0031\u0628", mappings, prohibited, options)
        ).to eq("\u0627\u0031\u0628")
      end

      it 'raises an error on bidi input with prohibited bidi character' do
        expect {
          prepare("\u0627\u0589\u0628", mappings, prohibited, options)
        }.to raise_error(Mongo::Error::FailedStringPrepOperation)
      end

      it 'raises an error on bidi input with invalid first bidi character' do
        expect {
          prepare("\u0031\u0627", mappings, prohibited, options)
        }.to raise_error(Mongo::Error::FailedStringPrepOperation)
      end

      it 'raises an error on bidi input with invalid first bidi character' do
        expect {
          prepare("\u0627\u0031", mappings, prohibited, options)
        }.to raise_error(Mongo::Error::FailedStringPrepOperation)
      end

      it 'raises an error on input with prohibited bidi character' do
        expect {
          prepare("\u0627\u0031", mappings, prohibited, options)
        }.to raise_error(Mongo::Error::FailedStringPrepOperation)
      end
    end
  end
end
