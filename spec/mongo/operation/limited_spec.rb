require 'spec_helper'

describe Mongo::Operation::Limited do

  describe '#options' do

    let(:limited) do
      Class.new do
        include Mongo::Operation::Specifiable
        include Mongo::Operation::Limited
      end.new({ :options => spec })
    end

    context 'when no limit is provided' do

      let(:spec) do
        { :skip => 5 }
      end

      it 'returns a limit of -1' do
        expect(limited.options).to eq({ :skip => 5, :limit => -1 })
      end
    end

    context 'when a limit is already provided' do

      context 'when the limit is -1' do

        let(:spec) do
          { :skip => 5, :limit => -1 }
        end

        it 'returns a limit of -1' do
          expect(limited.options).to eq({ :skip => 5, :limit => -1 })
        end
      end

      context 'when the limit is not -1' do

        let(:spec) do
          { :skip => 5, :limit => 5 }
        end

        it 'returns a limit of -1' do
          expect(limited.options).to eq({ :skip => 5, :limit => -1 })
        end
      end
    end
  end
end
