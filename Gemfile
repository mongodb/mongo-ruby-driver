source 'https://rubygems.org'

gemspec
gem 'rake'
gem 'yard'

group :development, :testing do
  gem 'jruby-openssl', :platforms => [ :jruby ]
  gem 'json', :platforms => [ :jruby ]
  gem 'rspec', '~> 3.0'
  gem 'mime-types', '~> 1.25'
  gem 'httparty'

  platforms :ruby_19, :ruby_20, :ruby_21, :jruby do
    gem 'coveralls', :require => false
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
    gem 'guard-yard', :platforms => [:mri_19, :mri_20]
  end

  gem 'ruby-prof', :platforms => :mri
  gem 'pry-rescue'
  gem 'pry-nav'
end
