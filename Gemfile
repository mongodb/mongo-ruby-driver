source 'https://rubygems.org'

gem 'json'
gem 'rake', :require => ['rake/testtask']
gem 'rake-compiler', :require => ['rake/extensiontask', 'rake/javaextensiontask']
gem 'activesupport'

group :deploy do
  gem 'git'
  gem 'yard'
  gem 'version_bumper'
  gem 'redcarpet' unless RUBY_PLATFORM =~ /java/
end

group :testing do
  gem 'test-unit'
  gem 'mocha', ">=0.13.0", :require => 'mocha/setup'
  gem 'shoulda', ">=3.3.2"

  gem 'sfl'
  gem 'simplecov', :require => false
end

platforms :jruby do
  gem 'jruby-launcher'
  gem 'jruby-jars'
end
