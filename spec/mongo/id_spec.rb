# frozen_string_literal: true

require 'lite_spec_helper'

module IdSpecHelpers
  class IdA
    include Mongo::Id
  end

  class IdB
    include Mongo::Id
  end

  class IdC
    include Mongo::Id
  end

  class IdD
    include Mongo::Id
  end
end

describe Mongo::Id do
  it 'starts with ID 1' do
    expect(IdSpecHelpers::IdA.next_id).to eq(1)
  end

  it 'increases each subsequent ID' do
    expect(IdSpecHelpers::IdB.next_id).to eq(1)
    expect(IdSpecHelpers::IdB.next_id).to eq(2)
  end

  it 'correctly generates independent IDs for separate classes' do
    expect(IdSpecHelpers::IdC.next_id).to eq(1)
    expect(IdSpecHelpers::IdD.next_id).to eq(1)
    expect(IdSpecHelpers::IdC.next_id).to eq(2)
    expect(IdSpecHelpers::IdD.next_id).to eq(2)
  end
end
