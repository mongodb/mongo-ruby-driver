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

          if test_instance.skip_reason
            before do
              skip test_instance.skip_reason
            end
          end

          before(:each) do
            test.setup_test
          end

          after(:each) do
            test.teardown_test
          end

          let(:results) do
            test.run
          end

          let(:verifier) { Mongo::Transactions::Verifier.new(test) }

          it 'returns the correct result' do
            verifier.verify_operation_result(results[:results])
          end

          it 'has the correct data in the collection', if: test_instance.outcome_collection_data do
            results
            verifier.verify_collection_data(results[:contents])
          end

          if test_instance.expectations
            it 'has the correct number of command_started events' do
              verifier.verify_command_started_event_count(results)
            end

            test_instance.expectations.each_with_index do |expectation, i|
              it "has the correct command_started event #{i}" do
                verifier.verify_command_started_event(results, i)
              end
            end
          end
        end
      end
    end
  end
end
