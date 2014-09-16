require 'spec_helper'

describe Mongo::Operation::Specifiable do

  let(:spec) do
    {}
  end

  let(:specifiable) do
    Class.new do
      include Mongo::Operation::Specifiable
    end.new(spec)
  end

  describe '#==' do

    context 'when the other object is a specifiable' do

      context 'when the specs are equal' do

        let(:other) do
          Class.new do
            include Mongo::Operation::Specifiable
          end.new(spec)
        end

        it 'returns true' do
          expect(specifiable).to eq(other)
        end
      end

      context 'when the specs are not equal' do

        let(:other) do
          Class.new do
            include Mongo::Operation::Specifiable
          end.new({ :db_name => 'test' })
        end

        it 'returns false' do
          expect(specifiable).to_not eq(other)
        end
      end
    end

    context 'when the other object is not a specifiable' do

      it 'returns false' do
        expect(specifiable).to_not eq('test')
      end
    end
  end
end
