# frozen_string_literal: true
# rubocop:todo all

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
end
