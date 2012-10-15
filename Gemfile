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
  gem "ci_reporter"
  gem "ruby-prof" unless RUBY_PLATFORM =~ /java/
  gem "rake-compiler"
  # posix-spawn: XCode 4.4 - brew install apple-gcc42; export CC=/usr/local/bin/gcc-4.2 && bundle install
  gem "posix-spawn" if RUBY_PLATFORM =~ /java/

  # Java
  platforms :jruby do
    gem "bouncy-castle-java"
    gem "jruby-launcher"
    gem "jruby-openssl"
  end
end
