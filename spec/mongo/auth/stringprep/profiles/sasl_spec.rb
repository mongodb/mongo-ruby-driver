require 'spec_helper'

describe Mongo::Auth::StringPrep::Profiles::SASL do
  def prepare(data)
    Mongo::Auth::StringPrep.prepare(
      data,
      Mongo::Auth::StringPrep::Profiles::SASL::MAPPINGS,
      Mongo::Auth::StringPrep::Profiles::SASL::PROHIBITIED,
      normalize: true,
      bidi: true,
    )
  end

  describe 'StringPrep#prepare' do
    context 'when Ruby version is below 2.2.0', if: RUBY_VERSION < '2.2.0' do
      it 'raises an error' do
        expect {
          prepare('')
        }.to raise_error(Mongo::Error::FailedStringPrepValidation)
      end
    end

    context 'when Ruby version is at least 2.2.0', if: RUBY_VERSION >= '2.2.0' do
      it 'removes unnecessary punctuation' do
        expect(prepare("I\u00ADX")).to eq('IX')
      end

      it 'replaces non-ASCII spaces' do
        expect(prepare("I\u2000X")).to eq('I X')
      end

      it 'returns the same string on ASCII input' do
        expect(prepare('user')).to eq('user')
      end

      it 'preserves case' do
        expect(prepare('USER')).to eq('USER')
      end

      it 'unicode normalizes single-character codes' do
        expect(prepare("\u00AA")).to eq('a')
      end

      it 'unicode normalizes multi-character codes' do
        expect(prepare("\u2168")).to eq('IX')
      end

      it 'raises an error on prohibited input' do
        expect {
          prepare("\u0007")
        }.to raise_error(Mongo::Error::FailedStringPrepValidation)
      end

      it 'raises an error on invalid bidi input' do
        expect {
          prepare("\u0627\u0031")
        }.to raise_error(Mongo::Error::FailedStringPrepValidation)
      end
    end
  end
end
