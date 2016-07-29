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

  describe '#read' do

    context 'when read is specified' do

      let(:spec) do
        {
          read: { mode: :secondary}
        }
      end

      let(:server_selector) do
        Mongo::ServerSelector.get(spec[:read])
      end

      it 'converts the read option to a ServerSelector' do
        expect(specifiable.read).to be_a(Mongo::ServerSelector::Secondary)
      end

      it 'uses the read option provided' do
        expect(specifiable.read).to eq(server_selector)
      end
    end

    context 'when read is not specified' do

      it 'returns a Primary ServerSelector object' do
        expect(specifiable.read).to eq(Mongo::ServerSelector.get(Mongo::ServerSelector::PRIMARY))
      end
    end
  end
end
