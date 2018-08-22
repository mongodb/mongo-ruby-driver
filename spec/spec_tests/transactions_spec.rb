require 'spec_helper'

describe 'Transactions' do

  TRANSACTIONS_TESTS.sort.each do |file|

    spec = Mongo::Transactions::Spec.new(file)

    context(spec.description) do
      spec.tests.each do |test_factory|
        test_instance = test_factory.call

        context(test_instance.description) do
          require_transaction_support

          let(:test) { test_factory.call }

          before(:each) do
            test.setup_test
          end

          after(:each) do
            test.teardown_test
          end

          let(:results) do
            test.run
          end

          it 'returns the correct result' do
            expect(results[:results]).to match_operation_result(test)
          end

          it 'has the correct data in the collection', if: test_instance.outcome_collection_data do
            expect(results[:contents]).to match_collection_data(test)
          end

          it 'has the correct command_started events', if: test_instance.expectations do
            expectations = test_instance.expectations.map do |expectation|
              # We convert the hashes to sorted arrays to ensure that
              # asserting equality between the expected and actual event
              # descriptions don't fail due to the same entries being
              # in a different order.
              command_event = expectation['command_started_event']['command'].to_a.sort
              command_event.delete_if { |_, v| v == nil }
              event = {'command' => command_event}
              event_expectation = expectation['command_started_event'].merge(event)
              expectation.merge('command_started_event' => event_expectation)
            end

            expect(results[:events]).to eq(expectations)
          end
        end
      end
    end
  end
end
