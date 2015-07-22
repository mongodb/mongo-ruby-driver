require 'spec_helper'

describe 'Command Monitoring Events' do

  COMMAND_MONITORING_TESTS.each do |file|

    spec = Mongo::CommandMonitoring::Spec.new(file)

    spec.tests.each do |test|

      context(test.description) do

        it '' do
          p test.expectations
        end
      end
    end
  end
end
