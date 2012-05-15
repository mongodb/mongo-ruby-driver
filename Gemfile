source :rubygems

group :development, :test do
  # Generic
  gem "bundler"
  gem "rake"
  gem "json"

  # Deployment
  gem "git"
  gem "redcarpet"
  gem "yard"

  # Testing
  gem "mocha"
  gem "shoulda"
  gem "test-unit"
  gem "ci_reporter"
  gem "ruby-prof" unless RUBY_PLATFORM =~ /java/
  gem "rake-compiler"

  # Java
  platforms :jruby do
    gem "bouncy-castle-java"
    gem "jruby-launcher"
    gem "jruby-openssl"
  end
end

gem "perftools.rb", :group => :development unless RUBY_PLATFORM =~ /java/

