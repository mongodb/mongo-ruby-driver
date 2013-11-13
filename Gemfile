source 'https://rubygems.org'

gem 'rake'
gem 'bson', '2.0.0.rc1'

group :release do
  gem 'git'
  gem 'kramdown'
  gem 'yard'
end

group :testing do
  gem 'json', :platforms => [ :ruby_18, :jruby ]
  gem 'rspec', '~> 2.14'
  gem 'mime-types', '~> 1.25'

  platforms :ruby_19, :ruby_20, :jruby do
    gem 'coveralls', :require => false
    gem 'rubocop', '0.15.0' if RUBY_VERSION > '1.9'
  end
end

group :development do
  if RUBY_VERSION > '1.9'
    gem 'rb-fchange', :require => false # Windows
    gem 'rb-fsevent', :require => false # OS X
    gem 'rb-inotify', :require => false # Linux
    gem 'terminal-notifier-guard'

    gem 'guard-bundler'
    gem 'guard-rspec', :platforms => :mri
    gem 'guard-jruby-rspec', :platforms => :jruby
    gem 'guard-rubocop', :platforms => [:ruby_19, :ruby_20, :jruby]
    gem 'guard-yard', :platforms => [:mri_19, :mri_20]
  end

  gem 'ruby-prof', :platforms => :mri
  gem 'pry-rescue'
  gem 'pry-nav'
end
