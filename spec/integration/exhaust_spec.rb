require 'spec_helper'

describe 'Exhaust' do
  describe 'inserted_ids' do
    let(:collection) do
      ClientRegistry.instance.global_client('authorized')['exhaust'].with(exhaust_allowed: true)
    end

    it 'works' do
      arr = [{a: 1}]*1000
      #collection.insert_many(arr)
      puts 'finding'
      #byebug
      p collection.find({}, exhaust: true, batch_size: 10).to_a.count
      byebug
      1
    end
  end
end
