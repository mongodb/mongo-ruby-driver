require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec)
task :default => :spec

namespace :spec do
  if RUBY_VERSION > '1.9'
    require 'coveralls/rake/task'
    Coveralls::RakeTask.new
    task :ci => [:spec, 'coveralls:push']
  else
    task :ci => [:spec]
  end
end
