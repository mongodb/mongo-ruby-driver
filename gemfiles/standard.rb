# frozen_string_literal: true
# encoding: utf-8

def standard_dependencies
  gem 'yard'
  gem 'ffi'

  group :development, :testing do
    gem 'jruby-openssl', platforms: :jruby
    gem 'json', platforms: :jruby
    gem 'rspec', '~> 3.10'
    gem 'activesupport'
    gem 'rake'

    gem 'byebug', platforms: :mri
    gem 'ruby-debug', platforms: :jruby

    gem 'aws-sdk-core', '~> 3'
    gem 'aws-sdk-cloudwatchlogs'
    gem 'aws-sdk-ec2'
    gem 'aws-sdk-ecs'
    gem 'aws-sdk-iam'
    gem 'paint'

    # for benchmark tests
    gem 'yajl-ruby', platforms: :mri, require: false
    gem 'celluloid', platforms: :mri, require: false
  end

  group :testing do
    gem 'timecop'
    gem 'ice_nine'
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
  end
end
