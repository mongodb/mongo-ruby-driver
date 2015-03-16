require 'spec_helper'

describe 'Intgeration' do

  INTEGRATION_TESTS.each do |file|
    include Mongo::MongoOrchestration

    spec = Mongo::MongoOrchestration::Spec.new(file)

    context(spec.description) do

      it 'succeeds' do
        begin
          spec.run
        rescue Mongo::MongoOrchestration::ServiceNotAvailable
          skip
        end
      end
    end
  end
end
