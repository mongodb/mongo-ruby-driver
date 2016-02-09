source 'https://rubygems.org'

gemspec
gem 'rake'
gem 'yard'

group :development, :testing do
  gem 'jruby-openssl', :platforms => [ :jruby ]
  gem 'json', :platforms => [ :jruby ]
  gem 'rspec', '~> 3.0'
  gem 'mime-types', '~> 1.25'
  gem 'httparty'
  gem 'yajl-ruby', require: 'yajl', platforms: :mri
  gem 'celluloid', platforms: :mri
end

group :development do
  gem 'ruby-prof', :platforms => :mri
  gem 'pry-rescue'
  gem 'pry-nav'
end
