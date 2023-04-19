# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'
require 'runners/read_write_concern_document'

READ_WRITE_CONCERN_DOCUMENT_TESTS =
  Dir.glob("#{CURRENT_PATH}/spec_tests/data/read_write_concern/document/*.yml").sort

describe 'Connection String' do
  READ_WRITE_CONCERN_DOCUMENT_TESTS.each do |test_path|
    spec = ReadWriteConcernDocument::Spec.new(test_path)

    context(spec.description) do

      spec.tests.each_with_index do |test, index|

        context test.description do

          let(:actual) do
            Mongo::WriteConcern.get(test.input_document)
          end

          let(:actual_server_document) do
            Utils.camelize_hash(actual.options)
          end

          if test.valid?

            it 'parses successfully' do
              expect do
                actual
              end.not_to raise_error
            end

            it 'has expected server document' do
              expect(actual_server_document).to eq(test.server_document)
            end

            if test.server_default?
              it 'is server default' do
                expect(actual.options).to eq({})
              end
            end

            if test.server_default? == false
              it 'is not server default' do
                expect(actual.options).not_to eq({})
              end
            end

            if test.acknowledged?
              it 'is acknowledged' do
                expect(actual.acknowledged?).to be true
              end
            end

            if test.acknowledged? == false
              it 'is not acknowledged' do
                expect(actual.acknowledged?).to be false
              end
            end

          else

            it 'is invalid' do
              expect do
                actual
              end.to raise_error(Mongo::Error::InvalidWriteConcern)
            end

          end
        end
      end
    end
  end
end
