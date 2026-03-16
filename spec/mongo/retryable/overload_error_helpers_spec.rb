# frozen_string_literal: true

require 'lite_spec_helper'

# Placed in retryable/ for logical grouping; uses string describe
# to avoid RSpec/FilePath mismatch with BaseWorker path.
describe 'Mongo::Retryable::BaseWorker overload error helpers' do
  # Create a testable subclass since BaseWorker's helper methods are private
  let(:worker_class) do
    Class.new(Mongo::Retryable::BaseWorker) do
      public :overload_error?, :retryable_overload_error?
    end
  end

  let(:retryable) { instance_double(Mongo::Collection) }
  let(:worker) { worker_class.new(retryable) }

  describe '#overload_error?' do
    context 'when error has SystemOverloadedError label' do
      let(:error) do
        e = Mongo::Error::SocketError.new('test')
        e.add_label('SystemOverloadedError')
        e
      end

      it 'returns true' do
        expect(worker.overload_error?(error)).to be true
      end
    end

    context 'when error does not have SystemOverloadedError label' do
      let(:error) { Mongo::Error::SocketError.new('test') }

      it 'returns false' do
        expect(worker).not_to be_overload_error(error)
      end
    end

    context 'when error does not respond to label?' do
      let(:error) { StandardError.new('test') }

      it 'returns false' do
        expect(worker.overload_error?(error)).to be false
      end
    end
  end

  describe '#retryable_overload_error?' do
    context 'when error has both labels' do
      let(:error) do
        e = Mongo::Error::SocketError.new('test')
        e.add_label('SystemOverloadedError')
        e.add_label('RetryableError')
        e
      end

      it 'returns true' do
        expect(worker.retryable_overload_error?(error)).to be true
      end
    end

    context 'when error has only SystemOverloadedError label' do
      let(:error) do
        e = Mongo::Error::SocketError.new('test')
        e.add_label('SystemOverloadedError')
        e
      end

      it 'returns false' do
        expect(worker.retryable_overload_error?(error)).to be false
      end
    end

    context 'when error has only RetryableError label' do
      let(:error) do
        e = Mongo::Error::SocketError.new('test')
        e.add_label('RetryableError')
        e
      end

      it 'returns false' do
        expect(worker.retryable_overload_error?(error)).to be false
      end
    end

    context 'when error has neither label' do
      let(:error) { Mongo::Error::SocketError.new('test') }

      it 'returns false' do
        expect(worker).not_to be_retryable_overload_error(error)
      end
    end
  end
end
