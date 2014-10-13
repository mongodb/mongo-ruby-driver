source 'https://rubygems.org'

gem 'json'
gem 'rake', '10.1.1', :require => ['rake/testtask']
gem 'rake-compiler', :require => ['rake/extensiontask', 'rake/javaextensiontask']
gem 'mime-types', '~> 1.25'
if RUBY_VERSION < '1.9.3'
  gem 'activesupport', '~>3.0'
else
  gem 'activesupport'
end

group :deploy do
  gem 'git'
  gem 'yard'
  gem 'version_bumper'
  gem 'kramdown'
end

group :testing do
  gem 'test-unit', '~>2.0'
  gem 'mocha', ">=0.13.0", :require => 'mocha/setup'
  gem 'shoulda', ">=3.3.2"
  if RUBY_VERSION >= '1.9.2'
    gem 'shoulda-matchers', '~>2.0'
  else
    gem 'shoulda-matchers', '~>1.0'
  end
  gem 'sfl'
  gem 'rest-client', '1.6.8'
  if RUBY_VERSION > '1.8.7' || RUBY_PLATFORM =~ /java/
    gem 'coveralls', :require => false
  end
end

group :development do
  gem 'pry', '~>0.9.0'
  gem 'pry-rescue', '~>1.4.0'
  gem 'pry-nav', '~>0.2.0'
end

platforms :jruby do
  gem 'jruby-launcher'
  gem 'jruby-jars', '1.7.13'
end
