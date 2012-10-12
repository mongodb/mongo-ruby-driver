source :rubygems

group :development, :test do
  # Generic
  gem "bundler"
  gem "rake"
  gem "json"

  # Deployment
  gem "git"
  gem "redcarpet" unless RUBY_PLATFORM =~ /java/
  gem "yard"

  # Testing
  gem "test-unit"
  gem "mocha"
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
