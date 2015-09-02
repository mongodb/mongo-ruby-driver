require 'spec_helper'

describe Mongo::Monitoring::Event::Secure do

  let(:document) do
    BSON::Document.new(test: 'value')
  end

  let(:klass) do
    Class.new do
      include Mongo::Monitoring::Event::Secure
    end
  end

  describe '#redacted' do

    let(:secure) do
      klass.new
    end

    context 'when the command must be redacted' do

      context 'when the command name is a string' do

        let(:redacted) do
          secure.redacted('saslStart', document)
        end

        it 'returns an empty document' do
          expect(redacted).to be_empty
        end
      end

      context 'when the command name is a symbol' do

        let(:redacted) do
          secure.redacted(:saslStart, document)
        end

        it 'returns an empty document' do
          expect(redacted).to be_empty
        end
      end
    end

    context 'when the command is not in the redacted list' do

      let(:redacted) do
        secure.redacted(:find, document)
      end

      it 'returns the document' do
        expect(redacted).to eq(document)
      end
    end
  end
end
