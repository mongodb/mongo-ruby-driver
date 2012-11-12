source :rubygems

gem 'json'
gem 'rake', :require => ['rake/testtask']
gem 'rake-compiler', :require => ['rake/extensiontask', 'rake/javaextensiontask']

group :deployment do
  gem 'git'
  gem 'yard'
  gem 'rdoc'
  gem 'version_bumper'
  gem 'rvm'
  gem 'redcarpet' unless RUBY_PLATFORM =~ /java/
end

group :testing do
  gem 'test-unit'
  gem 'mocha', '0.12.7'
  gem 'shoulda'
  gem 'sfl'
end

platforms :jruby do
  gem 'bouncy-castle-java'
  gem 'jruby-launcher'
  gem 'jruby-openssl'
end