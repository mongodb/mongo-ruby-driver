require 'spec_helper'

describe 'Transactions' do

  TRANSACTIONS_TESTS.sort.each do |file|

    spec = Mongo::Transactions::Spec.new(file)

    context(spec.description) do
      spec.tests.each do |test|
        context(test.description) do

          before(:each) do
            test.setup_test
          end

          let(:results) do
            test.run
          end

          it 'returns the correct result', if: test_transactions? do
            expect(results[:results]).to match_operation_result(test)
          end

          it 'has the correct data in the collection', if: test_transactions? && test.outcome_collection_data do
            expect(results[:contents]).to match_collection_data(test)
          end

          it 'has the correct command_started events', if: test_transactions? && test.expectations do
            test.expectations.each do |expectation|
              # We convert the hashes to sorted arrays to ensure that asserting equality between
              # the expected and actual event descriptions don't fail due to the same entries being
              # in a different order.
              expectation['command_started_event']['command'] = expectation['command_started_event']['command'].to_a.sort
              expectation['command_started_event']['command'].delete_if { |_, v| v == nil }
            end

            expect(results[:events]).to eq(test.expectations)
          end
        end
      end
    end
  end
end
