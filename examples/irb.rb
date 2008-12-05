#!/usr/bin/env ruby
org_argv = ARGV.dup
ARGV.clear

require 'irb'

$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'

include XGen::Mongo::Driver

host = org_argv[0] || 'localhost'
port = org_argv[1] || XGen::Mongo::Driver::Mongo::DEFAULT_PORT

puts "Connecting to #{host}:#{port} on DB"
DB = Mongo.new(host, port).db('ruby-mongo-examples-irb')

puts "Starting IRB session..."
IRB.start(__FILE__)