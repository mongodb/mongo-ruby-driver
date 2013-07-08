require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec)
task :default => :spec

if RUBY_VERSION > '1.9'
  require 'coveralls/rake/task'
  Coveralls::RakeTask.new
  task :test_with_coveralls => [:spec, 'coveralls:push']
else
  task :test_with_coveralls => [:spec]
end
