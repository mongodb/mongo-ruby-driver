require 'spec_helper'

describe 'Transactions API' do

  TRANSACTIONS_API_TESTS.sort.each do |file|

    spec = Mongo::Transactions::Spec.new(file)

    context(spec.description) do
      define_spec_tests_with_requirements(spec) do |req|
        spec.tests.each do |test_factory|
          test_instance = test_factory.call

          context(test_instance.description) do

            let(:test) { test_factory.call }

            if test_instance.skip_reason
              before do
                skip test_instance.skip_reason
              end
            end

            before(:each) do
              if req.satisfied?
                test.setup_test
              end
            end

            after(:each) do
              if req.satisfied?
                test.teardown_test
              end
            end

            let(:results) do
              test.run
            end

            let(:verifier) { Mongo::CRUD::Verifier.new(test) }

            it 'returns the correct result' do
              verifier.verify_operation_result(test_instance.expected_results, results[:results])
            end

            it 'has the correct data in the collection', if: test_instance.outcome.collection_data? do
              results
              verifier.verify_collection_data(
                test_instance.outcome.collection_data,
                results[:contents])
            end

            if test_instance.expectations
              it 'has the correct number of command_started events' do
                verifier.verify_command_started_event_count(
                  test_instance.expectations, results[:events])
              end

              test_instance.expectations.each_with_index do |expectation, i|
                it "has the correct command_started event #{i}" do
                  verifier.verify_command_started_event(
                    test_instance.expectations, results[:events], i)
                end
              end
            end
          end
        end
      end
    end
  end
end
