# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Auth::Aws::CredentialsCache do
  let(:subject) do
    described_class.new
  end

  describe '#fetch' do
    context 'when credentials are not cached' do
      it 'yields to the block' do
        expect { |b| subject.fetch(&b) }.to yield_control
      end

      it 'sets the credentials' do
        credentials = double('credentials')
        subject.fetch { credentials }
        expect(subject.credentials).to eq(credentials)
      end
    end

    context 'when credentials are cached' do
      context 'when credentials are not expired' do
        let(:credentials) do
          double('credentials', expired?: false)
        end

        it 'does not yield to the block' do
          subject.credentials = credentials
          expect { |b| subject.fetch(&b) }.not_to yield_control
        end
      end
    end

    context 'when credentials are expired' do
      let(:credentials) do
        double('credentials', expired?: true)
      end

      it 'yields to the block' do
        subject.credentials = credentials
        expect { |b| subject.fetch(&b) }.to yield_control
      end

      it 'sets the credentials' do
        subject.credentials = credentials
        new_credentials = double('new credentials')
        subject.fetch { new_credentials }
        expect(subject.credentials).to eq(new_credentials)
      end
    end
  end

  describe '#clear' do
    it 'clears the credentials' do
      subject.credentials = double('credentials')
      subject.clear
      expect(subject.credentials).to be nil
    end
  end
end
