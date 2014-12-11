require 'spec_helper'

describe MongoOrchestration do

  context 'when creating a standalone' do

    context 'when the mongo orchestration service is available' do

      before do
        initialize_standalone!
      end

      it 'sets up a standalone object with a config' do
        expect(@standalone.config).to_not be_nil
      end

      it 'sets up a standalone object with an id' do
        expect(@standalone.id).to eq('standalone')
      end
    end

    context 'when the mongo orchestration service is not available' do

      it 'raises an error' do
        expect do
          initialize_standalone!('http://localhost:1000')
        end.to raise_exception(MongoOrchestration::ServiceNotAvailable)
      end
    end
  end
end