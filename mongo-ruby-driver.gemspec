# This is a copy of the code in the stdlib file 'find.rb'. GitHub doesn't seem
# to allow me to "require 'find'".
module Find
  def find(*paths) # :yield: path
    paths.collect!{|d| d.dup}
    while file = paths.shift
      catch(:prune) do
	yield file.dup.taint
        next unless File.exist? file
	begin
	  if File.lstat(file).directory? then
	    d = Dir.open(file)
	    begin
	      for f in d
		next if f == "." or f == ".."
		if File::ALT_SEPARATOR and file =~ /^(?:[\/\\]|[A-Za-z]:[\/\\]?)$/ then
		  f = file + f
		elsif file == "/" then
		  f = "/" + f
		else
		  f = File.join(file, f)
		end
		paths.unshift f.untaint
	      end
	    ensure
	      d.close
	    end
	  end
        rescue Errno::ENOENT, Errno::EACCES
	end
      end
    end
  end

  def prune
    throw :prune
  end

  module_function :find, :prune
end

# ================================================================

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
  s.version = '0.5.4'
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
