source 'https://rubygems.org'

gem 'bson', :git => 'git://github.com/mongodb/bson-ruby.git'

group :deploy do
  gem 'git'
  gem 'yard'
  gem 'redcarpet' unless RUBY_PLATFORM =~ /java/
end

group :testing do
  gem 'rake'
  gem 'rspec'
end

group :development do

end

platforms :jruby do
  gem 'jruby-launcher'
  gem 'jruby-jars'
end
