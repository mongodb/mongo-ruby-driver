source :rubygems

# Generic
gem "bundler"
gem "rake"
gem "json"

# Testing
group :test do
  gem "test-unit"
  gem "mocha", ">=0.12.4" #0.12.3 is broken for us
  gem "shoulda"
  gem "rake-compiler"
  gem "sfl"
end

# Deployment
group :deploy do
  gem "git"
  gem "yard"
  gem "redcarpet", "2.2.0" unless RUBY_PLATFORM =~ /java/
end

# Java
platforms :jruby do
  gem "bouncy-castle-java"
  gem "jruby-launcher"
  gem "jruby-openssl"
end