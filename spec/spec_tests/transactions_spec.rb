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

          let(:verifier) { Mongo::CRUD::Verifier.new(test) }

          it 'returns the correct result' do
            expect(results[:results]).to match_operation_result(test)
          end

          it 'has the correct data in the collection', if: test_instance.outcome_collection_data do
            results
            verifier.verify_collection_data(results[:contents])
          end

          it 'has the correct command_started events', if: test_instance.expectations do
            test_instance.verifier.verify_command_started_events(results)
          end
        end
      end
    end
  end
end
