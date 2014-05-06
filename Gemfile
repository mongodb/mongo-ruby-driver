source 'https://rubygems.org'

gemspec
gem 'rake'

group :release do
  gem 'git'
  gem 'kramdown'
  gem 'yard'
end

platforms :rbx do
  gem 'racc'
  gem 'rubysl', '~> 2.0'
  gem 'psych'
  gem 'rubinius-coverage', github: 'rubinius/rubinius-coverage'
end

group :development, :testing do
  gem 'json', :platforms => [ :jruby ]
  gem 'rspec', '~> 2.14'
  gem 'mime-types', '~> 1.25'

  platforms :ruby_19, :ruby_20, :ruby_21, :jruby do
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
