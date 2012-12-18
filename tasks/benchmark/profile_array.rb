#!/usr/bin/env ruby
$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

def set_mode(mode.to_sym)
  case mode
    when :c
      ENV.delete('TEST_MODE')
      ENV.delete('BSON_EXT_DISABLED')
    when :ruby
      ENV['TEST_MODE']         = 'TRUE'
      ENV['BSON_EXT_DISABLED'] = 'TRUE'
    when :java
      ENV['TEST_MODE'] = 'TRUE'
      ENV.delete('BSON_EXT_DISABLED')
    else
      raise 'Valid modes include: c, ruby, java.'
  end
  return mode
end

$mode = ARGV[0].to_sym if ARGV[0]
set_mode($mode || :c)

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

def benchmark(iterations)
  btms = Benchmark.measure do
    (0...iterations).each do
      yield
    end
  end
  utime = btms.utime
  p ({'ops' => (iterations.to_f / utime.to_f).round(1)})
end

def ruby_prof(iterations)
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

#def perftools(iterations)
#  profile_file_name = '/tmp/profile_array.perftools'
#  PerfTools::CpuProfiler.start(profile_file_name) do
#    iterations.times { yield }
#  end
#  cmd = "pprof.rb --ignore=IO --text \"#{profile_file_name}\""
#  system(cmd)
#end

client = Mongo::MongoClient.new
db  = client['benchmark']
coll = db['profile']

coll.remove
#puts "coll.count: #{coll.count}"

base = 2
power = 6
n, doc = array_size_fixnum(base, power)
p ({'generator' => 'array_size_fixnum', 'operation' => 'insert',  'base' => base, 'power' => power})

benchmark(10000) { insert(coll, doc)} # valgrind --tool=callgrind ruby bench/profile_array.rb; callgrind_annotate ...
#ruby_prof(1000) { insert(coll, doc) }
#perftools(10000) { insert(coll, doc) }

#puts "coll.count: #{coll.count}"
coll.remove

