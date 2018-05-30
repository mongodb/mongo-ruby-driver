require 'spec_helper'

describe Mongo::Error::OperationFailure do

  describe '#code' do
    subject do
      described_class.new('not master (10107)', nil,
        :code => 10107, :code_name => 'NotMaster')
    end
    
    it 'returns the code' do
      expect(subject.code).to eq(10107)
    end
  end

  describe '#code_name' do
    subject do
      described_class.new('not master (10107)', nil,
        :code => 10107, :code_name => 'NotMaster')
    end
    
    it 'returns the code name' do
      expect(subject.code_name).to eq('NotMaster')
    end
  end
end
