#!/usr/bin/env ruby
$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

def set_mongo_driver_mode(mode)
  case mode
    when :c
      ENV.delete('TEST_MODE')
      ENV['C_EXT'] = 'TRUE'
    when :ruby
      ENV['TEST_MODE'] = 'TRUE'
      ENV.delete('C_EXT')
    else
      raise 'mode must be :c or :ruby'
  end
  ENV['MONGO_DRIVER_MODE'] = mode.to_s
end

$mode = ARGV[0].to_sym if ARGV[0]
set_mongo_driver_mode($mode || :c)
ENV['HOSTNAME'] = `uname -n`[/([^.]*)/,1]
ENV['OSNAME'] = `uname -s`.strip

# Exploratory/Experimental/Exponential tests for performance tuning

require 'rubygems'
require 'test-unit'
require 'json'
require 'mongo'
require 'benchmark'

$calibration_runtime = 0.1
$target_runtime = 5.0
$db_name = 'benchmark'
$collection_name = 'exp_series'

class Hash
  def store_embedded(key, value)
    case key
      when /([^.]*)\.(.*)/
        store($1, Hash.new) unless fetch($1, nil)
        self[$1].store_embedded($2, value)
      else
        store(key, value)
    end
  end
end

def sys_info
  h = Hash.new
  if FileTest.executable?('/usr/sbin/sysctl')
    text = `/usr/sbin/sysctl -a kern.ostype kern.version kern.hostname hw.machine hw.model hw.cputype hw.busfrequency hw.cpufrequency`
    values = text.split(/\n/).collect{|line| /([^:]*) *[:=] *(.*)/.match(line)[1..2]}
    h = Hash.new
    values.each{|key, value| h.store_embedded(key, value) }
  end
  return h
end

class TestExpPerformance < Test::Unit::TestCase

  def array_nest(base, level, obj)
    return obj if level == 0
    return Array.new(base, array_nest(base, level - 1, obj))
  end

   def hash_nest(base, level, obj)
     return obj if level == 0
     h = Hash.new
     (0...base).each{|i| h[i.to_s] = hash_nest(base, level - 1, obj)}
     return h
   end

  def estimate_iterations(db, coll, setup, teardown)
    start_time = Time.now
    iterations = 1
    utime = 0.0
    while utime <= $calibration_runtime do
      setup.call(db, coll)
      btms = Benchmark.measure do
        (0...iterations).each do
          yield
        end
      end
      utime = btms.utime
      teardown.call(db, coll)
      iterations *= 2
    end
    etime = (Time.now - start_time)
    return [(iterations.to_f * $target_runtime / utime).to_i, etime]
  end

  def measure_iterations(db, coll, setup, teardown, iterations)
    setup.call(db, coll)
    btms = Benchmark.measure { iterations.times { yield } }
    teardown.call(db, coll)
    return [btms.utime, btms.real]
  end

  def valuate(db, coll, setup, teardown)
    iterations, etime = estimate_iterations(db, coll, setup, teardown) { yield }
    utime, rtime = measure_iterations(db, coll, setup, teardown, iterations) { yield }
    return [iterations, utime, rtime, etime]
  end

  def power_test(base, max_power, db, coll, generator, setup, operation, teardown)
    return (0..max_power).collect do |power|
      size, doc = generator.call(base, power)
      iterations, utime, rtime, etime = valuate(db, coll, setup, teardown) { operation.call(coll, doc) }
      result = {
          'base' => base,
          'power' => power,
          'size' => size,
          'exp2' => Math.log2(size).to_i,
          'generator' => generator.name.to_s,
          'operation' => operation.name.to_s,
          'iterations' => iterations,
          'utime' => utime.round(2),
          'etime' => etime.round(2),
          'rtime' => rtime.round(2),
          'ops' => (iterations.to_f / utime.to_f).round(1),
          'usec' => (1000000.0 * utime.to_f / iterations.to_f).round(1),
          'mongo_driver_mode' => ENV['MONGO_DRIVER_MODE'],
          'hostname' => ENV['HOSTNAME'],
          'osname' => ENV['OSNAME'],
          # 'git' => git, # thinking
          # 'datetime' +> Time.now, # thinking
          # 'nbench-int' => nbench.int, # thinking
      }
      STDERR.puts result.inspect
      STDERR.flush
      result
    end
  end

  def value_string_size(base, power)
    n = base ** power
    return [n, {n.to_s => ('*' * n)}]
  end

  def key_string_size(base, power)
    n = base ** power
    return [n, {('*' * n) => n}]
  end

  def hash_size_fixnum(base, power)
    n = base ** power
    h = Hash.new
    (0...n).each { |i| h[i.to_s] = i }
    return [n, {n.to_s => h}] # embedded like array_size_fixnum
  end

  def array_size_fixnum(base, power)
    n = base ** power
    return [n, {n.to_s => Array.new(n, n)}]
  end

  def array_nest_fixnum(base, power)
    n = base ** power
    return [n, {n.to_s => array_nest(base, power, n)}]
  end

  def hash_nest_fixnum(base, power)
    n = base ** power
    return [n, {n.to_s => hash_nest(base, power, n)}]
  end

  def null_setup(db, coll)

  end

  def insert(coll, h)
    h.delete(:_id) # delete :_id to insert
    coll.insert(h) # note that insert stores :_id in h and subsequent inserts are updates
  end

  def default_teardown(db, coll)
    coll.remove
    #cmd = Hash.new.store('compact', $collection_name)
    #db.command(cmd)
  end

  def test_array_nest
    assert_equal(1, array_nest(2,0,1))
    assert_equal([1, 1], array_nest(2,1,1))
    assert_equal([[1, 1], [1, 1]], array_nest(2,2,1))
    assert_equal([[[1, 1], [1, 1]], [[1, 1], [1, 1]]], array_nest(2,3,1))
    assert_equal(1, array_nest(4,0,1))
    assert_equal([1, 1, 1, 1], array_nest(4,1,1))
    assert_equal([[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]], array_nest(4,2,1))
    assert_equal(1, array_nest(8,0,1))
    assert_equal([1, 1, 1, 1, 1, 1, 1, 1], array_nest(8,1,1))
  end

  def test_hash_nest # incomplete
    assert_equal(1, hash_nest(2, 0, 1))
    assert_equal({"0"=>1, "1"=>1}, hash_nest(2, 1, 1))
    assert_equal({"0"=>{"0"=>1, "1"=>1}, "1"=>{"0"=>1, "1"=>1}}, hash_nest(2, 2, 1))
    assert_equal({"0"=>{"0"=>{"0"=>1, "1"=>1}, "1"=>{"0"=>1, "1"=>1}},
                  "1"=>{"0"=>{"0"=>1, "1"=>1}, "1"=>{"0"=>1, "1"=>1}}}, hash_nest(2, 3, 1))
    assert_equal(1, hash_nest(4,0,1))
    assert_equal({"0"=>1, "1"=>1, "2"=>1, "3"=>1}, hash_nest(4,1,1))
    assert_equal({"0"=>{"0"=>1, "1"=>1, "2"=>1, "3"=>1},
                  "1"=>{"0"=>1, "1"=>1, "2"=>1, "3"=>1},
                  "2"=>{"0"=>1, "1"=>1, "2"=>1, "3"=>1},
                  "3"=>{"0"=>1, "1"=>1, "2"=>1, "3"=>1}}, hash_nest(4,2,1))
    assert_equal(1, hash_nest(8,0,1))
    assert_equal({"0"=>1, "1"=>1, "2"=>1, "3"=>1, "4"=>1, "5"=>1, "6"=>1, "7"=>1}, hash_nest(8,1,1))
  end

  # Performance Tuning Engineering
  ## Completed
  ### How to measure and compare pure Ruby versus C extension performance
  ## Current Work Items
  ### Profiling of C extension
  ## Overall Strategy
  ### Prioritize/Review Ruby 1.9.3, JRuby 1.6.7, Ruby 1.8.7
  ### Run spectrum of exploratory performance tests
  ### Graph results with flot in HTML wrapper - http://code.google.com/p/flot/
  ### Select test for profiling
  ### Find where time is being spent
  ### Construct specific performance test
  ### Iteratively tune specific performance test
  ### Iterate selection of test for profiling
  ## Notes
  ### Start with Create/insert, writing comes first
  ### Then Read/find, reading comes next. both findOne and find-cursor
  ### Update is primarily server load with minimal driver load for conditions
  ### Delete/remove is primarily server load with minimal driver load for conditions
  ## Benefits
  ### Performance Improvements
  ### Knowledge of Ruby driver and techniques
  ### Perhaps architecture and design improvements
  ### Lessons transferable to other drivers
  ## HW Info
  ### Linux - /proc/cpuinfo
  ### Mac OS X - sysctl -a hw

  def test_zzz_exp_blanket
    puts
    puts sys_info

    conn = Mongo::Connection.new
    conn.drop_database($db_name)
    db = conn.db($db_name)
    coll = db.collection($collection_name)
    coll.remove

    tests = [
        # Create/insert
        [2, 15, :value_string_size, :null_setup, :insert, :default_teardown],
        [2, 15, :key_string_size, :null_setup, :insert, :default_teardown],
        [2, 14, :array_size_fixnum, :null_setup, :insert, :default_teardown],
        [2, 17, :hash_size_fixnum, :null_setup, :insert, :default_teardown],
        [2, 12, :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [4, 6, :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [8, 4, :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [16, 3, :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [32, 2, :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [2, 15, :hash_nest_fixnum, :null_setup, :insert, :default_teardown ],
        [4, 8, :hash_nest_fixnum, :null_setup, :insert, :default_teardown ],
        [8, 4, :hash_nest_fixnum, :null_setup, :insert, :default_teardown ],
        [16, 4, :hash_nest_fixnum, :null_setup, :insert, :default_teardown ],
        [32, 3, :hash_nest_fixnum, :null_setup, :insert, :default_teardown ],

        # synthesized mix, real-world data pending

        # Read/findOne/find pending

        # Update pending

        # Delete/remove pending

    ]
    results = []
    tests.each do |base, max_power, generator, setup, operation, teardown|
      # consider moving 'method' as permitted by scope
      results += power_test(base, max_power, db, coll, method(generator), method(setup), method(operation), method(teardown))
    end
    # consider inserting the results into a database collection
    # Test::Unit::TestCase pollutes STDOUT, so write to a file
    File.open("exp_series-#{Time.now.strftime('%Y%m%d-%H%M')}.js", 'w'){|f|
      f.puts("#{results.to_json.gsub(/\[/, "").gsub(/(}[\],])/, "},\n")}")
    }

    conn.drop_database($db_name)
  end

end


