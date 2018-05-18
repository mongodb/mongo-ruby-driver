require 'spec_helper'

describe Mongo::Auth::StringPrep::Profiles::SASL do
  let(:prepared_data) do
    Mongo::Auth::StringPrep.prepare(
      data,
      mappings,
      prohibited,
      options
    )
  end

  let(:mappings) do
    Mongo::Auth::StringPrep::Profiles::SASL::MAPPINGS
  end

  let(:prohibited) do
    Mongo::Auth::StringPrep::Profiles::SASL::PROHIBITED
  end

  let(:options) do
    {
      normalize: true,
      bidi: true
    }
  end

  describe 'StringPrep#prepare' do
    context 'when Ruby version is below 2.2.0', if: RUBY_VERSION < '2.2.0' do
      let(:data) do
        ''
      end

      it 'raises an error' do
        expect {
          prepared_data
        }.to raise_error(Mongo::Error::FailedStringPrepValidation)
      end
    end

    context 'when Ruby version is at least 2.2.0', if: RUBY_VERSION >= '2.2.0' do
      context 'when there is unnecessary punctuation' do
        let(:data) do
          "I\u00ADX"
        end

        it 'removes the punctuation' do
          expect(prepared_data).to eq('IX')
        end
      end

      context 'when there are non-ASCII spaces' do
        let(:data) do
          "I\u2000X"
        end

        it 'replaces them with ASCII spaces' do
          expect(prepared_data).to eq('I X')
        end
      end

      context 'when the input is ASCII' do
        let(:data) do
          'user'
        end

        it 'returns the same string' do
          expect(prepared_data).to eq('user')
        end
      end

      context 'when the data contains uppercase characters' do
        let(:data) do
          'USER'
        end

        it 'preserves case' do
          expect(prepared_data).to eq('USER')
        end
      end

      context 'when the data contains single-character codes' do
        let(:data) do
          "\u00AA"
        end

        it 'normalizes the codes' do
          expect(prepared_data).to eq('a')
        end
      end

      context 'when the data contains multi-character codes' do
        let(:data) do
          "\u2168"
        end

        it 'normalizes the codes' do
          expect(prepared_data).to eq('IX')
        end
      end

      context 'when the data contains prohibited input' do
        let(:data) do
          "\u0007"
        end

        it 'raises an error' do
          expect {
            prepared_data
          }.to raise_error(Mongo::Error::FailedStringPrepValidation)
        end
      end

      context 'when the data contains invalid bidi input' do
        let(:data) do
          "\u0627\u0031"
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
