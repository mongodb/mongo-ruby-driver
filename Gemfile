source :rubygems

gem 'json'
gem 'rake', :require => ['rake/testtask']
gem 'rake-compiler', :require => ['rake/extensiontask', 'rake/javaextensiontask']
gem 'activesupport'

group :deploy do
  gem 'git'
  gem 'yard'
  gem 'rvm'
  gem 'version_bumper'
  gem 'redcarpet' unless RUBY_PLATFORM =~ /java/
end

group :testing do
  gem 'simplecov', :require => false
  gem 'test-unit'
  gem 'mocha', :require => 'mocha/setup'
  gem 'shoulda'
  gem 'sfl'
  gem 'rspec'
end

platforms :jruby do
  gem 'bouncy-castle-java'
  gem 'jruby-launcher'
  gem 'jruby-openssl'
end
