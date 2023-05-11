# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

# This test is a marker used to verify that the test suite runs on
# mmapv1 storage engine.
describe 'mmapv1' do
  require_mmapv1

  context 'standalone' do
    require_topology :single

    it 'is exercised' do
    end
  end

  context 'replica set' do
    require_topology :replica_set

    it 'is exercised' do
    end
  end

  context 'sharded' do
    require_topology :sharded

    it 'is exercised' do
    end
  end
end
