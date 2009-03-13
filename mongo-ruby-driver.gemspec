PACKAGE_FILES = ['README.rdoc', 'Rakefile', 'mongo-ruby-driver.gemspec']
PACKAGE_FILES = PACKAGE_FILES + Dir['bin/*'] + Dir['examples/*.rb'] + Dir['lib/**/*.rb'] + Dir['ext/**/*.(rb|c)']
PACKAGE_FILES.reject! { |fn| fn.include? '.DS' }

TEST_FILES = Dir['tests/*.rb'] + Dir['tests/mongo-qa/*']
TEST_FILES.reject! { |fn| fn.include? '.DS' }

Gem::Specification.new do |s|
  s.name = 'mongo'
  s.version = '0.6.4'
  s.platform = Gem::Platform::RUBY
  s.summary = 'Ruby driver for the 10gen Mongo DB'
  s.description = 'A Ruby driver for the 10gen Mongo DB. For more information about Mongo, see http://www.mongodb.org.'

  s.require_paths = ['lib']

  s.files = PACKAGE_FILES
  s.test_files = TEST_FILES

  s.has_rdoc = true
  s.rdoc_options = ['--main', 'README.rdoc', '--inline-source']
  s.extra_rdoc_files = ['README.rdoc']
#  s.extensions << 'ext/cbson/extconf.rb'

  s.authors = ['Jim Menard', 'Mike Dirolf']
  s.email = 'mongodb-dev@googlegroups.com'
  s.homepage = 'http://www.mongodb.org'
end
