source :rubygems

group :development, :test do
  # Generic
  gem "bundler"
  gem "rake"
  gem "json"

  # Deployment
  gem "git"
  gem "redcarpet", "2.2.0" unless RUBY_PLATFORM =~ /java/
  gem "yard"

  # Testing
  gem "test-unit"
  gem "mocha", ">=0.12.4" #0.12.3 is broken for us
  gem "shoulda"
  gem "rake-compiler"
  gem "sfl"

  # Java
  platforms :jruby do
    gem "bouncy-castle-java"
    gem "jruby-launcher"
    gem "jruby-openssl"
  end
end
