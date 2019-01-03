require 'spec_helper'

describe 'Transactions API' do

  TRANSACTIONS_API_TESTS.sort.each do |file|

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

          if test_instance.expectations
            it 'has the correct number of command_started events' do
              test_instance.verifier.verify_command_started_event_count(results)
            end

            test_instance.expectations.each_with_index do |expectation, i|
              it "has the correct command_started event #{i}" do
                test_instance.verifier.verify_command_started_event(results, i)
              end
            end
          end
        end
      end
    end
  end
end
