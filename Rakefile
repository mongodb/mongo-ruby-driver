require 'rubygems'
require 'rubygems/specification'
require 'fileutils'
require 'rake'
require 'rake/testtask'
require 'rake/gempackagetask'
require 'rake/contrib/rubyforgepublisher'

# NOTE: some of the tests assume Mongo is running
Rake::TestTask.new do |t|
  t.test_files = FileList['tests/test*.rb']
end

desc "Clone or pull (update) the mongo-qa project used for testing"
task :mongo_qa do
  if File.exist?('mongo-qa')
    Dir.chdir('mongo-qa') do
      system('git pull')
    end
  else
    system('git clone git://github.com/mongodb/mongo-qa.git')
  end
end

desc "Generate documentation"
task :rdoc do
  FileUtils.rm_rf('html')
  system "rdoc --main README.rdoc --op html --inline-source --quiet README.rdoc `find lib -name '*.rb'`"
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
gem build mongo-ruby-driver.gemspec &&
    sudo gem install mongo-*.gem &&
    rm mongo-*.gem
EOS
  end

end

task :default => :list

task :list do
  system 'rake -T'
end
