require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec)
task :default => :spec

if RUBY_VERSION > '1.9' && RUBY_PLATFORM != 'java'
  require 'tailor/rake_task'
  Tailor::RakeTask.new do |task|
    task.file_set 'lib/**/*.rb'
  end

  require 'coveralls/rake/task'
  Coveralls::RakeTask.new
  task :test_with_coveralls => [:spec, 'coveralls:push']
else
  task :test_with_coveralls => [:spec]
end
