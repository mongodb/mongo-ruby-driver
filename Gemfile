source 'https://rubygems.org'

gemspec

gem 'rake'

group :deploy do
  gem 'git'
  gem 'yard'
  gem 'redcarpet' unless RUBY_PLATFORM =~ /java/
end

group :testing do
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
