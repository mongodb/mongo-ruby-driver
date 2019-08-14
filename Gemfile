source 'https://rubygems.org'

gemspec
gem 'yard'

group :development, :testing do
  gem 'jruby-openssl', :platforms => :jruby
  gem 'json', :platforms => :jruby
  # Explicitly specify each rspec dependency so that we can use
  # rspec-mocks-diag instead of rspec-mocks
  #gem 'rspec', '~> 3.0'
  gem 'rspec-core', '~> 3.0'
  gem 'mime-types', '~> 1.25'
  if RUBY_VERSION >= '2.3'
    gem 'activesupport'
  end
  if RUBY_VERSION < '2.0.0'
    gem 'rake', '~> 12.2.0'
    gem 'httparty', '0.14.0'
  else
    gem 'rake'
    gem 'httparty'
  end
  
  if RUBY_VERSION >= '2.3'
    gem 'byebug', platforms: :mri
  elsif RUBY_VERSION >= '2.0'
    gem 'byebug', '< 11', platforms: :mri
  end
  
  # for benchmark tests
  gem 'yajl-ruby', require: 'yajl', platforms: :mri
  gem 'celluloid', platforms: :mri, require: false
  if RUBY_VERSION < '2.2'
    gem 'timers', '< 4.2'
    gem 'hitimes', '1.3.0'
  else
    gem 'timers'
  end
end

group :testing do
  gem 'timecop'
  gem 'ice_nine'
  gem 'rspec-retry'
  gem 'rspec-expectations', '~> 3.0'
  if RUBY_VERSION >= '2.3'
    gem 'rspec-mocks-diag', '~> 3.0'
  else
    gem 'rspec-mocks', '~> 3.0'
  end
  gem 'rfc'
  gem 'fuubar'
  gem 'timeout-interrupt', platforms: :mri
  
  if RUBY_VERSION < '2.3'
    gem 'ffi', '<1.11'
  end
end

group :development do
  gem 'ruby-prof', :platforms => :mri
  gem 'pry-rescue'
  gem 'pry-nav'
end
