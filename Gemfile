source 'https://rubygems.org'

gemspec
gem 'yard'

group :development, :testing do
  gem 'jruby-openssl', :platforms => :jruby
  gem 'json', :platforms => :jruby
  gem 'rspec', '~> 3.0'
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
  
  if RUBY_VERSION >= '2.0.0'
    gem 'byebug', platforms: :mri
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
  gem 'rfc'
  gem 'fuubar'
  gem 'timeout-interrupt', platforms: :mri
end

group :development do
  gem 'ruby-prof', :platforms => :mri
  gem 'pry-rescue'
  gem 'pry-nav'
end
