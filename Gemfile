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
  gem 'activesupport'
  gem 'rake'
  gem 'httparty'

  gem 'byebug', platforms: :mri

  # for benchmark tests
  gem 'yajl-ruby', require: 'yajl', platforms: :mri
  gem 'celluloid', platforms: :mri, require: false
  gem 'timers'
end

group :testing do
  gem 'timecop'
  gem 'ice_nine'
  gem 'rspec-retry'
  gem 'rspec-expectations', '~> 3.0'
  gem 'rspec-mocks-diag', '~> 3.0'
  gem 'rfc'
  gem 'fuubar'
  gem 'timeout-interrupt', platforms: :mri
end

group :development do
  gem 'ruby-prof', :platforms => :mri
  gem 'pry-rescue'
  gem 'pry-nav'
end
