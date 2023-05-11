# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Auth::Aws::CredentialsRetriever do
  describe '#credentials' do
    context 'when credentials should be obtained from endpoints' do
      let(:cache) do
        Mongo::Auth::Aws::CredentialsCache.new
      end

      let(:subject) do
        described_class.new(credentials_cache: cache).tap do |retriever|
          allow(retriever).to receive(:credentials_from_environment).and_return(nil)
        end
      end

      context 'when cached credentials are not expired' do
        let(:credentials) do
          double('credentials', expired?: false)
        end

        before(:each) do
          cache.credentials = credentials
        end

        it 'returns the cached credentials' do
          expect(subject.credentials).to eq(credentials)
        end

        it 'does not obtain credentials from endpoints' do
          expect(subject).not_to receive(:obtain_credentials_from_endpoints)
          described_class.new(credentials_cache: cache).credentials
        end
      end

      shared_examples_for 'obtains credentials from endpoints' do
        context 'when obtained credentials are not expired' do
          let(:credentials) do
            double('credentials', expired?: false)
          end

          before(:each) do
            expect(subject)
              .to receive(:obtain_credentials_from_endpoints)
              .and_return(credentials)
          end

          it 'returns the obtained credentials' do
            expect(subject.credentials).not_to be_expired
          end

          it 'caches the obtained credentials' do
            subject.credentials
            expect(cache.credentials).to eq(credentials)
          end
        end

        context 'when cannot obtain credentials from endpoints' do
          before(:each) do
            expect(subject)
              .to receive(:obtain_credentials_from_endpoints)
              .and_return(nil)
          end

          it 'raises an error' do
            expect { subject.credentials }.to raise_error(Mongo::Auth::Aws::CredentialsNotFound)
          end
        end
      end

      context 'when cached credentials expired' do
        before(:each) do
          cache.credentials = double('credentials', expired?: true)
        end

        it_behaves_like 'obtains credentials from endpoints'
      end

      context 'when no credentials cached' do
        before(:each) do
          cache.clear
        end

        it_behaves_like 'obtains credentials from endpoints'
      end
    end
  end
end
