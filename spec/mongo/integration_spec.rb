require 'spec_helper'

describe 'Intgeration' do

  INTEGRATION_TESTS.each do |file|
    include Mongo::MongoOrchestration

    spec = Mongo::MongoOrchestration::Spec.new(file)

    context(spec.description) do

      it 'succeeds', if: Mongo::MongoOrchestration.available?(spec.base_url) do
        spec.run
      end
    end
  end
end
