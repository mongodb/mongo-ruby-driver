require 'rubygems'
require 'rubygems/specification'
require 'fileutils'
require 'rake'
require 'rake/testtask'
require 'rake/gempackagetask'

GEM = "mongo"
GEM_VERSION = '0.0.1'
SUMMARY = 'Simple pure-Ruby driver for the 10gen Mongo DB'
DESCRIPTION = 'This is a simple pure-Ruby driver for the 10gen Mongo DB. For more information about Mongo, see http://www.mongodb.org.'
AUTHOR = 'Jim Menard'
EMAIL = 'jimm@io.com'
HOMEPAGE = 'http://www.mongodb.org'
 
spec = Gem::Specification.new do |s|
  s.name = GEM
  s.version = GEM_VERSION
  s.platform = Gem::Platform::RUBY
  s.summary = SUMMARY
  s.description = DESCRIPTION
  
  s.require_paths = ['lib']
  
  s.files = FileList['bin/*', 'lib/**/*.rb', 'tests/**/*.rb', '[A-Z]*'].to_a
  
  s.bindir = 'bin'
  s.has_rdoc = true

  s.author = AUTHOR
  s.email = EMAIL
  s.homepage = HOMEPAGE

  s.rubyforge_project = GEM # GitHub bug, gem isn't being build when this miss
end

# NOTE: some of the tests assume Mongo is running
Rake::TestTask.new do |t|
  t.test_files = FileList['tests/test*.rb']
end

desc "Generate documentation"
task :rdoc do
  FileUtils.rm_rf('doc')
  system "rdoc --main README.rdoc --inline-source --quiet README.rdoc `find lib -name '*.rb'`"
end

namespace :gem do 

  Rake::GemPackageTask.new(spec) do |pkg|
    pkg.gem_spec = spec
  end

  desc "Install the gem locally"
  task :install => [:package] do
    sh %{sudo gem install pkg/#{GEM}-#{GEM_VERSION}}
  end
  
  desc "Install the gem locally with ruby 1.9"
  task :'19:install' => [:package] do
    sh %{sudo gem19 install pkg/#{GEM}-#{GEM_VERSION}}
  end
 
  desc "Create a gemspec file"
  task :make_spec do
    File.open("#{GEM}.gemspec", "w") do |file|
      file.puts spec.to_ruby
    end
  end

end

task :default => :list

task :list do
  system 'rake -T'
end
