require 'rubygems'
require 'rubygems/specification'
require 'fileutils'
require 'rake'
require 'rake/testtask'
require 'rake/gempackagetask'
begin
  require 'rake/contrib/rubyforgepublisher'
rescue LoadError
end
require 'rbconfig'
include Config

gem_command = "gem"
gem_command = "gem1.9" if $0.match(/1\.9$/) # use gem1.9 if we used rake1.9

# NOTE: some of the tests assume Mongo is running
Rake::TestTask.new do |t|
  t.test_files = FileList['tests/test*.rb']
end

desc "Generate documentation"
task :rdoc do
  version = eval(File.read("mongo-ruby-driver.gemspec")).version
  out = File.join('html', version.to_s)
  FileUtils.rm_rf('html')
  system "rdoc --main README.rdoc --op #{out} --inline-source --quiet README.rdoc `find lib -name '*.rb'`"
end

desc "Publish documentation to mongo.rubyforge.org"
task :publish => [:rdoc] do
  # Assumes docs are in ./html
  Rake::RubyForgePublisher.new(GEM, RUBYFORGE_USER).upload
end

namespace :gem do

  desc "Install the gem locally"
  task :install do
    sh <<EOS
#{gem_command} build mongo-ruby-driver.gemspec &&
    sudo #{gem_command} install mongo-*.gem &&
    rm mongo-*.gem
EOS
  end

  desc "Install the optional c extensions"
  task :install_extensions do
    sh <<EOS
#{gem_command} build mongo-extensions.gemspec &&
    sudo #{gem_command} install mongo_ext-*.gem &&
    rm mongo_ext-*.gem
EOS
  end

end

task :default => :list

task :list do
  system 'rake -T'
end
