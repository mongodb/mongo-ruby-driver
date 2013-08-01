source 'https://rubygems.org'

gem 'rake'
gem 'bson', '2.0.0.rc1'

group :release do
  gem 'git'
  gem 'yard'
  gem 'redcarpet', :platforms => :mri
end

group :testing do
  gem 'json', :platforms => [ :ruby_18, :jruby ]
  gem 'rspec', '~> 2.14'

  platforms :ruby_19, :ruby_20, :jruby do
    gem 'coveralls', :require => false
    gem 'rubocop' unless RUBY_VERSION < '1.9'
  end
end

group :development do
  gem 'pry-rescue'
  gem 'pry-nav'
  gem 'guard-rspec'

  gem 'rb-inotify', :require => false # Linux
  gem 'rb-fsevent', :require => false # OS X
  gem 'rb-fchange', :require => false # Windows
  gem 'terminal-notifier-guard'

  gem 'guard-rubocop', :platforms => [:ruby_19, :ruby_20, :jruby]
  gem 'ruby-prof', :platforms => :mri
end
