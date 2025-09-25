# frozen_string_literal: true
# encoding: utf-8

module Mrss
  module LiteConstraints

    # Constrain tests that use TimeoutInterrupt to MRI (and Unix).
    def require_mri
      before(:all) do
        unless SpecConfig.instance.mri?
          skip "MRI required, we have #{SpecConfig.instance.platform}"
        end
      end
    end

    def require_jruby
      before(:all) do
        unless BSON::Environment.jruby?
          skip "JRuby required, we have #{SpecConfig.instance.platform}"
        end
      end
    end

    # This is for marking tests that fail on JRuby that should
    # in principle work (as opposed to being fundamentally incompatible
    # with JRuby).
    # Often times these failures happen only in Evergreen.
    def fails_on_jruby(version=nil)
      before(:all) do
        if BSON::Environment.jruby?
          if version
            min_parts = version.split('.').map(&:to_i)
            actual_parts = JRUBY_VERSION.split('.').map(&:to_i)[0...min_parts.length]
            actual = actual_parts.join('.')
            if actual <= version
              skip "Fails on jruby through #{version}"
            end
          else
            skip "Fails on jruby"
          end
        end
      end
    end

    # Indicates that the respective test uses the internet in some capacity,
    # for example the test resolves SRV DNS records.
    def require_external_connectivity
      before(:all) do
        if ENV['EXTERNAL_DISABLED']
          skip "Test requires external connectivity"
        end
      end
    end

    def require_mongo_kerberos
      before(:all) do
        # TODO Use a more generic environment variable name if/when
        # Mongoid tests get Kerberos configurations.
        unless %w(1 yes true).include?(ENV['MONGO_RUBY_DRIVER_KERBEROS']&.downcase)
          skip 'Set MONGO_RUBY_DRIVER_KERBEROS=1 in environment to run Kerberos unit tests'
        end
        require 'mongo_kerberos'
      end
    end

    def require_linting
      before(:all) do
        unless Mongo::Lint.enabled?
          skip "Linting is not enabled"
        end
      end
    end

    # Some tests will fail if linting is enabled:
    # 1. Tests that pass invalid options to client, etc. which the linter
    #    rejects.
    # 2. Tests that set expectations on topologies, server descriptions, etc.
    #    (since setting expectations requires mutating said objects, and when
    #    linting is on those objects are frozen).
    def require_no_linting
      before(:all) do
        if Mongo::Lint.enabled?
          skip "Linting is enabled"
        end
      end
    end

    def require_libmongocrypt
      before(:all) do
        # If FLE is set in environment, the entire test run is supposed to
        # include FLE therefore run the FLE tests.
        if (ENV['LIBMONGOCRYPT_PATH'] || '').empty? && (ENV['FLE'] || '').empty?
          skip 'Test requires path to libmongocrypt to be specified in LIBMONGOCRYPT_PATH env variable'
        end
      end
    end

    def min_libmongocrypt_version(version)
      require_libmongocrypt
      before(:all) do
        actual_version = Utils.parse_version(Mongo::Crypt::Binding.mongocrypt_version(nil))
        min_version = Utils.parse_version(version)
        unless actual_version >= min_version
          skip "libmongocrypt version #{min_version} required, but version #{actual_version} is available"
        end
      end
    end

    def require_no_libmongocrypt
      before(:all) do
        if ENV['LIBMONGOCRYPT_PATH']
          skip 'Test requires libmongocrypt to not be configured'
        end
      end
    end

    def require_aws_auth
      before(:all) do
        unless (ENV['AUTH'] || '') =~ /^aws/
          skip 'This test requires AUTH=aws* and an appropriately configured runtime environment'
        end
      end
    end

    def require_ec2_host
      before(:all) do
        if $have_aws.nil?
          $have_aws = begin
            require 'open-uri'
            begin
              Timeout.timeout(3.81) do
                URI.parse('http://169.254.169.254/latest/meta-data/profile').open.read
              end
              true
            # When trying to use the EC2 metadata endpoint on ECS:
            # Errno::EINVAL: Failed to open TCP connection to 169.254.169.254:80 (Invalid argument - connect(2) for "169.254.169.254" port 80)
            rescue Timeout::Error, Errno::ETIMEDOUT, Errno::EINVAL, OpenURI::HTTPError => $aws_error
              false
            end
          end
        end
        unless $have_aws
          skip "EC2 instance metadata is not available - assuming not running on an EC2 instance: #{$aws_error.class}: #{$aws_error}"
        end
      end
    end

    def require_stress
      before(:all) do
        if !SpecConfig.instance.stress?
          skip 'Set STRESS=1 in environment to run stress tests'
        end
      end
    end

    def require_fork
      before(:all) do
        if !SpecConfig.instance.fork?
          skip 'Set FORK=1 in environment to run fork tests'
        end
      end
    end

    def require_ocsp
      before(:all) do
        if !SpecConfig.instance.ocsp?
          skip 'Set OCSP=1 in environment to run OCSP tests'
        end
      end
    end

    def require_ocsp_verifier
      before(:all) do
        if !SpecConfig.instance.ocsp_verifier?
          skip 'Set OCSP_VERIFIER=1 in environment to run OCSP verifier tests'
        end
      end
    end

    def require_ocsp_connectivity
      before(:all) do
        if !SpecConfig.instance.ocsp_connectivity?
          skip 'Set OCSP_CONNECTIVITY=pass or OCSP_CONNECTIVITY=fail in environment to run OCSP connectivity tests'
        end
      end
    end

    def require_active_support
      before(:all) do
        if !SpecConfig.instance.active_support?
          skip 'This test requires ActiveSupport; set WITH_ACTIVE_SUPPORT=1 in environment'
        end
      end
    end

    def no_active_support
      before(:all) do
        if SpecConfig.instance.active_support?
          skip 'This test requires no ActiveSupport; unset WITH_ACTIVE_SUPPORT in environment'
        end
      end
    end

    def require_fallbacks
      before(:all) do
        unless %w(yes true 1).include?((ENV['TEST_I18N_FALLBACKS'] || '').downcase)
          skip 'Set TEST_I18N_FALLBACKS=1 environment variable to run these tests'
        end
      end
    end

    def require_no_fallbacks
      before(:all) do
        if %w(yes true 1).include?((ENV['TEST_I18N_FALLBACKS'] || '').downcase)
          skip 'Set TEST_I18N_FALLBACKS=0 environment variable to run these tests'
        end
      end
    end

    # This is a macro for retrying flaky tests on CI that occasionally fail.
    # Note that the tests will only be retried on CI.
    #
    # @param [ Integer ] :tries The number of times to retry.
    # @param [ Integer ] :sleep The number of seconds to sleep in between retries.
    #   If nothing, or nil, is passed, we won't wait in between retries.
    def retry_test(tries: 3, sleep: nil)
      if %w(1 yes true).include?(ENV['CI'])
        around do |example|
          if sleep
            example.run_with_retry retry: tries, retry_wait: sleep
          else
            example.run_with_retry retry: tries
          end
        end
      end
    end
  end
end
