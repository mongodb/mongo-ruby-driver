require 'fileutils'
require 'rake/testtask'

task :default => [:test]

# NOTE: some of the tests assume Mongo is running
Rake::TestTask.new do |t|
  t.test_files = FileList['tests/test*.rb']
end

desc "Generate documentation"
task :rdoc do
  FileUtils.rm_rf('doc')
  system "rdoc --main README.rdoc --inline-source --quiet README.rdoc `find lib -name '*.rb'`"
end
