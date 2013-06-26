source 'https://rubygems.org'

gem 'rake'
gem 'bson', '2.0.0.rc1'

group :release do
  gem 'git'
  gem 'yard'
  gem 'redcarpet' unless RUBY_PLATFORM =~ /java/
end

group :testing do
  gem 'json', :platforms => [ :ruby_18, :jruby ]
  gem 'rspec'
  if RUBY_VERSION > '1.9'
    gem 'tailor', :require => false
    gem 'coveralls', :require => false
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

  gem 'ruby-prof', :platforms => :mri
end
