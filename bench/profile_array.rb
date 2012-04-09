#!/usr/bin/env ruby
$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
#
# Note: Ruby 1.9 is faster than 1.8, as expected.
# This suite will be run against the installed version of ruby-mongo-driver.
# The c-extension, bson_ext, will be used if installed.

require 'rubygems'
require 'mongo'
require 'benchmark'
require 'ruby-prof'

def array_size_fixnum(base, power)
  n = base ** power
  return [n, {n.to_s => Array.new(n, n)}]
end

def array_size_fixnum(base, power)
  n = base ** power
  return [n, {n.to_s => Array.new(n, n)}]
end

def insert(coll, h)
  h.delete(:_id) # delete :_id to insert
  coll.insert(h) # note that insert stores :_id in h and subsequent inserts are updates
end

def profile(iterations)
  RubyProf.start
  puts Benchmark.measure {
    iterations.times { yield }
  }
  result = RubyProf.stop

  # Print a flat profile to text
  printer = RubyProf::FlatPrinter.new(result)
  printer.print(STDOUT)

  # Print a graph profile to text
  printer = RubyProf::GraphPrinter.new(result)
  printer.print(STDOUT, {})
end

conn = Mongo::Connection.new

db  = conn['benchmark']
coll = db['profile']

coll.remove
puts "coll.count: #{coll.count}"

n, doc = array_size_fixnum(2, 6)
profile(1000) { insert(coll, doc) }

puts "coll.count: #{coll.count}"
coll.remove

