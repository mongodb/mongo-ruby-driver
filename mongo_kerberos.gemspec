Gem::Specification.new do |s|
  s.name = 'mongo_kerberos'
  s.version = File.read(File.join(File.dirname(__FILE__), 'VERSION'))
  s.platform = Gem::Platform::RUBY
  s.authors = [ 'Emily Stolfo', 'Durran Jordan' ]
  s.email = 'mongodb-dev@googlegroups.com'
  s.homepage = 'http://www.mongodb.org'
  s.summary = 'Kerberos authentication support for the MongoDB Ruby driver'
  s.description = 'Adds kerberos authentication via libsasl to the MongoDB Ruby Driver on MRI and JRuby'
  s.rubyforge_project = 'mongo_kerberos'
  s.license = 'Apache License Version 2.0'

  if File.exists?('gem-private_key.pem')
    s.signing_key = 'gem-private_key.pem'
    s.cert_chain  = ['gem-public_cert.pem']
  else
    warn 'Warning: No private key present, creating unsigned gem.'
  end

  s.files = [ 'mongo_kerberos.gemspec', 'LICENSE', 'VERSION' ]
  s.files += [ 'lib/mongo_kerberos.rb' ]
  s.files += Dir[ 'lib/mongo_kerberos/**/*.rb' ]

  if RUBY_PLATFORM =~ /java/
    s.platform = 'java'
    s.files << 'ext/jsasl/target/jsasl.jar'
  else
    s.files += Dir.glob('ext/csasl/**/*.{c,h,rb}')
    s.extensions = [ 'ext/csasl/extconf.rb' ]
  end

  s.add_dependency('mongo', "#{s.version}")
  s.require_paths = ['lib']
  s.has_rdoc = 'yard'
end
