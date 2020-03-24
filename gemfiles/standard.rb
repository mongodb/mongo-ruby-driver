def standard_dependencies
  gem 'yard'
  gem 'ffi'

  group :development, :testing do
    gem 'jruby-openssl', platforms: :jruby
    gem 'json', platforms: :jruby
    # Explicitly specify each rspec dependency so that we can use
    # rspec-mocks-diag instead of rspec-mocks
    #gem 'rspec', '~> 3.0'
    gem 'rspec-core', '~> 3.0'
    gem 'activesupport'
    gem 'rake'

    gem 'byebug', platforms: :mri
    gem 'ruby-debug', platforms: :jruby

    # for benchmark tests
    gem 'yajl-ruby', platforms: :mri, require: false
    gem 'celluloid', platforms: :mri, require: false
  end

  group :testing do
    gem 'timecop'
    gem 'ice_nine'
    gem 'rubydns', platforms: :mri
    gem 'rspec-retry'
    gem 'rspec-expectations', '~> 3.0'
    gem 'rspec-mocks-diag', '~> 3.0'
    gem 'rfc', '~> 0.1.0'
    gem 'fuubar'
    gem 'timeout-interrupt', platforms: :mri
    gem 'concurrent-ruby', platforms: :jruby
    gem 'dotenv'
  end

  group :development do
    gem 'ruby-prof', platforms: :mri
  end
end
