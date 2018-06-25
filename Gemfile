source 'https://rubygems.org'

gemspec
gem 'yard'

group :development, :testing do
  gem 'jruby-openssl', :platforms => [ :jruby ]
  gem 'json', :platforms => [ :jruby ]
  gem 'rspec', '~> 3.0'
  gem 'mime-types', '~> 1.25'
  if RUBY_VERSION < '2.0.0'
    gem 'rake', '~> 12.2.0'
    gem 'httparty', '0.14.0'
  else
    gem 'rake'
    gem 'httparty'
  end
  gem 'yajl-ruby', require: 'yajl', platforms: :mri
  gem 'celluloid', platforms: :mri
  gem 'fuubar'
  platforms :mri do
    if RUBY_VERSION >= '2.0.0'
      gem 'byebug'
    end
  end
end

group :testing do
  gem 'rspec-retry'
end

group :development do
  gem 'ruby-prof', :platforms => :mri
  gem 'pry-rescue'
  gem 'pry-nav'
end
