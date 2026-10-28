# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Crypt::Context do
  let(:binding) { double('Mongo::Crypt::Binding') }
  let(:handle) { double('mongocrypt handle', kms_tls_options: {}) }
  let(:io) { double('encryption io') }

  # Context#initialize needs a real libmongocrypt handle, which is not
  # available in every test environment, so build the object without it and
  # set only the collaborators the methods under test use.
  let(:context) do
    described_class.allocate.tap do |ctx|
      ctx.instance_variable_set(:@mongocrypt_handle, handle)
      ctx.instance_variable_set(:@encryption_io, io)
    end
  end

  # Mongo::Crypt::Binding is autoloaded from libmongocrypt, which is not
  # installed in every test environment. stub_const cannot be used here because
  # resolving the constant to save its original value triggers the autoload,
  # which fails when libmongocrypt is missing. Swap the autoload for a double
  # for the duration of an example, then restore the autoload (or the
  # previously loaded class).
  #
  # rubocop:disable RSpec/RemoveConst
  def with_replaced_binding(replacement)
    autoload_path = Mongo::Crypt.autoload?(:Binding)
    original = Mongo::Crypt.send(:remove_const, :Binding) if Mongo::Crypt.const_defined?(:Binding, false)
    Mongo::Crypt.const_set(:Binding, replacement)
    yield
  ensure
    Mongo::Crypt.send(:remove_const, :Binding)
    if autoload_path
      Mongo::Crypt.autoload(:Binding, autoload_path)
    elsif original
      Mongo::Crypt.const_set(:Binding, original)
    end
  end
  # rubocop:enable RSpec/RemoveConst

  around do |example|
    with_replaced_binding(binding) { example.run }
  end

  describe '#feed_kms' do
    let(:first_kms_context) { double('first kms context') }
    let(:second_kms_context) { double('second kms context') }

    before do
      allow(binding).to receive(:kms_ctx_usleep).and_return(nil)
      allow(binding).to receive(:kms_ctx_get_kms_provider).and_return('aws')
      allow(binding).to receive(:ctx_kms_done)
    end

    it 'recomputes the remaining budget for each KMS request' do
      allow(binding).to receive(:ctx_next_kms_ctx)
        .and_return(first_kms_context, second_kms_context, nil)

      holder = double('timeout holder')
      allow(holder).to receive(:remaining_timeout_ms!).and_return(1_000, 400)

      expect(io).to receive(:feed_kms)
        .with(first_kms_context, {}, timeout_ms: 1_000).ordered
      expect(io).to receive(:feed_kms)
        .with(second_kms_context, {}, timeout_ms: 400).ordered

      context.send(:feed_kms, holder)
    end

    it 'bounds the libmongocrypt backoff by the operation deadline' do
      allow(binding).to receive(:ctx_next_kms_ctx)
        .and_return(first_kms_context, nil)
      # libmongocrypt asks for a ten second backoff.
      allow(binding).to receive(:kms_ctx_usleep).and_return(10_000_000)

      holder = double('timeout holder')
      allow(holder).to receive(:remaining_timeout_sec).and_return(0.25)
      allow(holder).to receive(:remaining_timeout_ms!).and_return(250)

      # The sleep must be clamped to the remaining budget, not the full delay.
      expect(context).to receive(:sleep).with(0.25)
      allow(io).to receive(:feed_kms)

      context.send(:feed_kms, holder)
    end

    it 'raises when the budget is already spent before the first request' do
      allow(binding).to receive(:ctx_next_kms_ctx).and_return(first_kms_context, nil)

      holder = double('timeout holder')
      allow(holder).to receive(:remaining_timeout_ms!)
        .and_raise(Mongo::Error::TimeoutError, 'operation deadline passed')

      expect(io).not_to receive(:feed_kms)

      expect do
        context.send(:feed_kms, holder)
      end.to raise_error(Mongo::Error::TimeoutError)
    end

    it 'recomputes the budget across an in-place retry and stops once it is spent' do
      allow(binding).to receive(:ctx_next_kms_ctx)
        .and_return(first_kms_context, first_kms_context, nil)
      allow(binding).to receive(:kms_ctx_fail).and_return(true)

      holder = double('timeout holder')
      calls = 0
      allow(holder).to receive(:remaining_timeout_ms!) do
        calls += 1
        raise Mongo::Error::TimeoutError, 'operation deadline passed' if calls > 1

        500
      end

      # Exactly one request must be attempted: the retry must see that the
      # budget is spent rather than running with the first attempt's timeout.
      expect(io).to receive(:feed_kms)
        .once
        .and_raise(Mongo::Error::KmsError.new('boom', network_error: true))

      expect do
        context.send(:feed_kms, holder)
      end.to raise_error(Mongo::Error::TimeoutError)
    end

    it 'forwards a nil budget when CSOT is not in effect' do
      allow(binding).to receive(:ctx_next_kms_ctx).and_return(first_kms_context, nil)

      expect(io).to receive(:feed_kms).with(first_kms_context, {}, timeout_ms: nil)

      context.send(:feed_kms, Mongo::CsotTimeoutHolder.new)
    end

    it 'does not forward a zero budget, which downstream code reads as unlimited' do
      allow(binding).to receive(:ctx_next_kms_ctx).and_return(first_kms_context, nil)

      # A real holder with half a millisecond left, whose remaining budget
      # truncates to 0 ms. Forwarding that would disable the socket timeouts.
      allow(Mongo::Utils).to receive(:monotonic_time).and_return(100.0, 100.0005)
      holder = Mongo::CsotTimeoutHolder.new(operation_timeouts: { operation_timeout_ms: 1 })

      expect(io).to receive(:feed_kms).with(first_kms_context, {}, timeout_ms: 1)

      context.send(:feed_kms, holder)
    end
  end

  describe '#run_state_machine' do
    it 'passes the timeout holder to feed_kms when KMS I/O is needed' do
      allow(context).to receive(:state).and_return(:need_kms, :done)

      holder = Mongo::CsotTimeoutHolder.new
      expect(context).to receive(:feed_kms).with(holder)

      expect(context.run_state_machine(holder)).to be_nil
    end
  end
end
