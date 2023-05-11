# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'truncated UTF-8 in server error messages' do
  let(:rep) do
    '(╯°□°)╯︵ ┻━┻'
  end

  let(:collection) do
    authorized_client['truncated_utf8']
  end

  before(:all) do
    ClientRegistry.instance.global_client('authorized')['truncated_utf8'].indexes.create_one(
      {k: 1}, unique: true)
  end

  it 'works' do
    pending 'RUBY-2560'

    collection.insert_one(k: rep*20)
    collection.insert_one(k: rep*20)
  end
end
