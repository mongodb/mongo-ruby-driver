# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

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

      context 'the command is not a hello/legacy hello command' do

        let(:redacted) do
          secure.redacted(:find, document)
        end

        it 'returns the document' do
          expect(redacted).to eq(document)
        end

      end

      %w(hello ismaster isMaster).each do |command|
        context command do
          it 'returns an empty document if speculative auth' do
            expect(
              secure.redacted(command, BSON::Document.new('speculativeAuthenticate' => "foo"))
            ).to be_empty
          end

          it 'returns an original document if no speculative auth' do
            expect(
              secure.redacted(command, document)
            ).to eq(document)
          end
        end
      end

    end
  end

  describe '#compression_allowed?' do

    context 'when the selector represents a command for which compression is not allowed' do

      let(:secure) do
        klass.new
      end

      Mongo::Monitoring::Event::Secure::REDACTED_COMMANDS.each do |command|

        let(:selector) do
          { command => 1 }
        end

        context "when the command is #{command}" do

          it 'does not allow compression for the command' do
           expect(secure.compression_allowed?(selector.keys.first)).to be(false)
          end
        end
      end
    end

    context 'when the selector represents a command for which compression is allowed' do

      let(:selector) do
        { ping: 1 }
      end

      let(:secure) do
        klass.new
      end

      context 'when the command is :ping' do

        it 'does not allow compression for the command' do
          expect(secure.compression_allowed?(selector.keys.first)).to be(true)
        end
      end
    end
  end
end
