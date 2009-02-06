require 'find'

def self.files_in(dir)
  files = []
  Find.find(dir) { |path|
    next if path =~ /\.DS_Store$/
    files << path unless File.directory?(path)
  }
  files
end

PACKAGE_FILES = ['README.rdoc', 'Rakefile', 'mongo-ruby-driver.gemspec'] +
  files_in('bin') + files_in('examples') + files_in('lib')
    
TEST_FILES = files_in('tests')

Gem::Specification.new do |s|
  s.name = 'mongo'
  s.version = '0.5.3'
  s.platform = Gem::Platform::RUBY
  s.summary = 'Simple pure-Ruby driver for the 10gen Mongo DB'
  s.description = 'A pure-Ruby driver for the 10gen Mongo DB. For more information about Mongo, see http://www.mongodb.org.'

  s.require_paths = ['lib']
  
  s.files = PACKAGE_FILES
  s.test_files = TEST_FILES
  
  s.has_rdoc = true
  s.rdoc_options = ['--main', 'README.rdoc', '--inline-source']
  s.extra_rdoc_files = ['README.rdoc']

  s.author = 'Jim Menard'
  s.email = 'jim@10gen.com'
  s.homepage = 'http://www.mongodb.org'
end
