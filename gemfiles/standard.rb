# frozen_string_literal: true

# rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Metrics/BlockLength
def standard_dependencies
  gem 'yard', '>= 0.9.35'
  gem 'ffi'

  group :development, :testing do
    gem 'jruby-openssl', platforms: :jruby
    gem 'json', platforms: :jruby
    gem 'rspec', '~> 3.12'
    gem 'activesupport', '<7.1'
    gem 'rake'
    gem 'webrick'

    gem 'byebug', platforms: :mri
    gem 'ruby-debug', platforms: :jruby

    gem 'aws-sdk-core', '~> 3'
    gem 'aws-sdk-cloudwatchlogs'
    gem 'aws-sdk-ec2'
    gem 'aws-sdk-ecs'
    gem 'aws-sdk-iam'
    gem 'aws-sdk-sts'
    gem 'paint'

    # for benchmark tests
    gem 'yajl-ruby', platforms: :mri, require: false
    gem 'celluloid', platforms: :mri, require: false

    gem 'rubocop', '~> 1.45.1'
    gem 'rubocop-performance', '~> 1.16.0'
    gem 'rubocop-rake', '~> 0.6.0'
    gem 'rubocop-rspec', '~> 2.18.1'

    platform :mri do
      # Debugger for VSCode.
      if !ENV['CI'] && !ENV['DOCKER'] && RUBY_VERSION < '3.0'
        gem 'debase'
        gem 'ruby-debug-ide'
      end
    end
  end

  group :testing do
    gem 'timecop'
    gem 'ice_nine'
    gem 'async', '2.23.1', platforms: :mri if RUBY_VERSION.match?(/^3\.1/)
    gem 'rubydns', platforms: :mri
    gem 'rspec-retry'
    gem 'rfc', '~> 0.2.0'
    gem 'fuubar'
    gem 'timeout-interrupt', platforms: :mri
    gem 'concurrent-ruby', platforms: :jruby
    gem 'dotenv'
    gem 'childprocess'
  end

  group :development do
    gem 'ruby-prof', platforms: :mri
    gem 'erubi'
    gem 'tilt'
    # solargraph depends on rbs, which won't build on jruby for some reason
    gem 'solargraph', platforms: :mri
    gem 'ruby-lsp', platforms: :mri
  end

  gem 'libmongocrypt-helper', '~> 1.14.0' if ENV['FLE'] == 'helper'
end
# rubocop:enable Metrics/AbcSize, Metrics/MethodLength, Metrics/BlockLength
