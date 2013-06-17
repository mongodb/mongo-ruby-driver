require 'rspec/core/rake_task'
require 'tailor/rake_task'
require 'coveralls/rake/task'

RSpec::Core::RakeTask.new(:spec)
task :default => :spec

Tailor::RakeTask.new do |task|
  task.file_set 'lib/**/*.rb'
end

Coveralls::RakeTask.new
