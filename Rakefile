require 'fileutils'
require 'rake/testtask'

task :default => [:test]

# NOTE: some of the tests assume Mongo is running
Rake::TestTask.new do |t|
  t.test_files = FileList['tests/test*.rb']
end

task :rdoc do
  FileUtils.rm_rf('doc')
  system "rdoc --main README README `find lib -name '*.rb'`"
end
