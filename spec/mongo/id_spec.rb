# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Id do
  it 'starts with ID 1' do
    class IdA
      include Mongo::Id
    end

    expect(IdA.next_id).to eq(1)
  end

  it 'increases each subsequent ID' do
    class IdB
      include Mongo::Id
    end

    expect(IdB.next_id).to eq(1)
    expect(IdB.next_id).to eq(2)
  end

  it 'correctly generates independent IDs for separate classes' do
    class IdC
      include Mongo::Id
    end

    class IdD
      include Mongo::Id
    end

    expect(IdC.next_id).to eq(1)
    expect(IdD.next_id).to eq(1)
    expect(IdC.next_id).to eq(2)
    expect(IdD.next_id).to eq(2)
  end
end
