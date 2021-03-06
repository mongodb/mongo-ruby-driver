# frozen_string_literal: true
# encoding: utf-8

require 'lite_spec_helper'

describe Mongo::Utils do
  describe '#shallow_symbolize_keys' do
    it 'symbolizes' do
      described_class.shallow_symbolize_keys(
        'foo' => 'bar',
        'aKey' => 'aValue',
        'a_key' => 'a_value',
        key: :value,
      ).should == {
        foo: 'bar',
        aKey: 'aValue',
        a_key: 'a_value',
        key: :value,
      }
    end
  end

  describe '#shallow_camelize_keys' do
    it 'camelizes' do
      described_class.shallow_camelize_keys(
        'foo' => 'bar',
        'aKey' => 'aValue',
        'aa_key' => 'a_value',
        key: :value,
        sKey: :sValue,
        us_key: :us_value,
      ).should == {
        'foo' => 'bar',
        'aKey' => 'aValue',
        'aaKey' => 'a_value',
        'key' => :value,
        'sKey' => :sValue,
        'usKey' => :us_value,
      }
    end
  end

  describe '#slice_hash' do
    it do
      hash = {'key1' => 1, :key2 => 's', :key3 => true}
      expect(
        described_class.slice_hash(hash, 'key1', :key3)
      ).to eq(
        {
          'key1' => 1,
          :key3 => true
        }
      )
    end
  end
end
