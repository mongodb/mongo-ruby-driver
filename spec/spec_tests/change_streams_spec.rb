require 'spec_helper'

describe 'ChangeStreams' do
  require_wired_tiger

  CHANGE_STREAMS_TESTS.each do |file|

    spec = Mongo::ChangeStreams::Spec.new(file)

    context(spec.description) do

      define_spec_tests_with_requirements(spec) do

        spec.tests.each do |test|

          context(test.description) do

            before(:each) do
              test.setup_test
            end

            let(:result) do
              test.run
            end

            it 'returns the correct result' do
              expect(result[:result]).to match_result(test)
            end

            it 'has the correct command_started events', if: test.expectations do
              expect(result[:events]).to match_commands(test)
            end
          end
        end
      end
    end
  end
end
