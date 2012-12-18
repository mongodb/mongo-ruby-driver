#!/usr/bin/env ruby
$LOAD_PATH.unshift(File.expand_path("../../../lib", __FILE__))

require 'rubygems'
require 'getoptlong'
require 'json'
require 'benchmark'
require 'test-unit'

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

$description         = 'Exploratory/Experimental/Exponential tests for Ruby-driver performance tuning'
$calibration_runtime = 0.5
$target_runtime      = 15.0
$db_name             = 'benchmark'
$collection_name     = 'exp_series'
$mode                = set_mode('c')
$hostname            = `uname -n`[/([^.]*)/,1]
$osname              = `uname -s`.strip
$tag                 = `git log -1 --format=oneline`.split[0]
$date                = Time.now.strftime('%Y%m%d-%H%M')
$file_name_opt       = nil

options_with_help = [
    [ '--help', '-h', GetoptLong::NO_ARGUMENT, '', 'show help' ],
    [ '--file', '-f', GetoptLong::OPTIONAL_ARGUMENT, ' file', 'output file name' ],
    [ '--mode', '-m', GetoptLong::OPTIONAL_ARGUMENT, ' mode', 'set mode to "c" or "ruby" (c)' ],
    [ '--tag', '-t', GetoptLong::OPTIONAL_ARGUMENT, ' tag', 'set tag for run, default is git commit key' ]
]
options = options_with_help.collect{|option|option[0...3]}

GetoptLong.new(*options).each do |opt, arg|
  case opt
    when '--help'
      puts "#{$0} -- #{$description}\n"
      puts "usage: #{$0} [performance-options] [-- test-unit-options]"
      puts "example: #{$0} --mode c --tag with-c-ext -- --verbose verbose --name test_insert"
      puts "performance-options:"
      options_with_help.each{|option| puts "#{option[0]}#{option[3]}, #{option[1]}#{option[3]}\n\t#{option[4]}"}
      exit 0
    when '--file'
      $file_name_opt = arg
    when '--mode'
      $mode = set_mode(arg)
    when '--tag'
      $tag = arg
  end
end

$file_name = $file_name_opt ? $file_name_opt : "exp_series_#{$date}_#{$tag}_#{$mode}.js"

require 'mongo' # must be after option processing

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
    text = `/usr/sbin/sysctl -a kern.ostype kern.version kern.hostname hw.machine hw.model hw.cputype hw.cpusubtype hw.busfrequency hw.cpufrequency hw.memsize hw.physicalcpu hw.logicalcpu hw.packages`
    values = text.split(/\n/).collect{|line| /([^:]*) *[:=] *(.*)/.match(line)[1..2]}
    h = Hash.new
    values.each{|key, value| h.store_embedded(key, value) }
  end
  return h
end

class TestExpPerformance < Test::Unit::TestCase
  setup :setup_test_set
  teardown :teardown_test_set

  def array_nest(base, level, obj)
    return obj if level == 0
    return Array.new(base, array_nest(base, level - 1, obj))
  end

  def test__array_nest
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

  def hash_nest(base, level, obj)
    return obj if level == 0
    h = Hash.new
    (0...base).each{|i| h[i.to_s] = hash_nest(base, level - 1, obj)}
    return h
  end

  def test__hash_nest
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

  def multi_doc(multi_power, doc)
    return doc if multi_power == -1
    return (2 ** multi_power).times.collect{doc.dup}
  end

  def test_multi_doc
    doc = {'a' => 1}
    assert_equal({"a"=>1}, multi_doc(-1, doc))
    assert_equal([{"a"=>1}], multi_doc(0, doc))
    assert_equal([{"a"=>1}, {"a"=>1}], multi_doc(1, doc))
    assert_equal([{"a"=>1}, {"a"=>1}, {"a"=>1}, {"a"=>1}], multi_doc(2, doc))
    assert_equal(8, multi_doc(3, doc).size)
    assert_equal(16, multi_doc(4, doc).size)
    assert_equal(32, multi_doc(5, doc).size)
    mdoc = multi_doc(2, doc)
    mdoc[0]['b'] = 2
    assert_equal([{"a"=>1, "b"=>2}, {"a"=>1}, {"a"=>1}, {"a"=>1}], mdoc, 'non-dup doc will fail for insert many safe')
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

  def setup_test_set
    @client = Mongo::MongoClient.new
    @client.drop_database($db_name)
    @db = @client.db($db_name)
    @coll = @db.collection($collection_name)
    @coll.remove
    @results = []
    puts
    puts JSON.pretty_generate({'mode' => $mode , 'hostname' => $hostname, 'osname' => $osname, 'date' => $date, 'tag' => $tag})
    puts JSON.pretty_generate(sys_info)
  end

  def teardown_test_set
   # consider inserting the results into a database collection
    # Test::Unit::TestCase pollutes STDOUT, so write to a file
    File.open($file_name, 'a'){|f|
      f.puts(@results.to_json.gsub(/\[/, "").gsub(/}[\],]/, "},\n")) if @results != []
    }
    @client.drop_database($db_name)
  end

  def estimate_iterations(db, coll, doc, setup, teardown)
    start_time = Time.now
    iterations = 1
    utime = 0.0
    while utime <= $calibration_runtime do
      iterations *= 2
      setup.call(db, coll, doc, iterations)
      btms = Benchmark.measure do
        (0...iterations).each do
          yield
        end
      end
      utime = btms.utime
      teardown.call(db, coll)
    end
    etime = (Time.now - start_time)
    return [(iterations.to_f * $target_runtime / utime).to_i, etime]
  end

  def measure_iterations(db, coll, doc, setup, teardown, iterations)
    setup.call(db, coll, doc, iterations)
    btms = Benchmark.measure { iterations.times { yield } }
    teardown.call(db, coll)
    return [btms.utime, btms.real]
  end

  def valuate(db, coll, doc, setup, teardown)
    @client.drop_database($db_name) # hack to reduce paging
    iterations, etime = estimate_iterations(db, coll, doc, setup, teardown) { yield }
    @client.drop_database($db_name) # hack to reduce paging
    utime, rtime = measure_iterations(db, coll, doc, setup, teardown, iterations) { yield }
    return [iterations, utime, rtime, etime]
  end

  def power_test(args)
    base, power_enum, multi_enum, generator, setup, operation, teardown = args
    generator, setup, operation, teardown = method(generator), method(setup), method(operation), method(teardown)
    return power_enum.collect do |power|
      multi_enum.collect do |multi_power|
        status = 'OK'
        size, doc = generator.call(base, power)
        doc = multi_doc(multi_power, doc)
        multi_size = (doc.class == Array) ? doc.size : 1;
        iterations, utime, rtime, etime = begin
          valuate(@db, @coll, doc, setup, teardown) { operation.call(@coll, doc) }
        rescue Mongo::InvalidOperation => e
          status = "#{e.class}: #{e.message}"
          [0, 1.0, 1.0, 1.0]
        end
        multi_iterations = multi_size.to_f * iterations.to_f
        user_ops = (multi_iterations / utime.to_f).round(1)
        real_ops = (multi_iterations / rtime.to_f).round(1)
        result = {
            'base'        => base,
            'power'       => power,
            'size'        => size,
            'exp2'        => Math.log2(size).to_i,
            'multi_power' => multi_power,
            'multi_size'  => multi_size,
            'generator'   => generator.name.to_s,
            'operation'   => operation.name.to_s,
            'iterations'  => iterations,
            'user_time'   => utime.round(2),
            'real_time'   => rtime.round(2),
            'user_ops'    => user_ops.nan? ? 0 : user_ops,
            'real_ops'    => real_ops.nan? ? 0 : real_ops,
            'user_usec'   => (1000000.0 * utime.to_f / [multi_iterations, 1].max).round(1),
            'real_usec'   => (1000000.0 * rtime.to_f / [multi_iterations, 1].max).round(1),
            'est_time'    => etime.round(2),
            'mode'        => $mode,
            'hostname'    => $hostname,
            'osname'      => $osname,
            'date'        => $date,
            'tag'         => $tag,
            'status'      => status,
            # 'nbench-int' => nbench.int, # thinking
        }
        STDERR.puts result.to_json #result.inspect
        STDERR.flush
        result
      end
    end.flatten
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

  def null_setup(db, coll, doc, iterations)

  end

  def default_teardown(db, coll)
    coll.remove
    raise 'coll not removed' if coll.size > 0
  end

  def clear_ids(doc) # delete :_id to really insert, required for safe
    if doc.class == Array
      doc.each{|d|d.delete(:_id)}
    else
      doc.delete(:_id)
    end
  end

  def insert(coll, doc)
    clear_ids(doc)
    coll.insert(doc) # note that insert stores :_id in doc and subsequent inserts are updates
  end

  def insert_safe(coll, doc)
    clear_ids(doc)
    coll.insert(doc) # note that insert stores :_id in doc and subsequent inserts with :_id are updates
  end

  def find_one_setup(db, coll, doc, iterations)
    insert(coll, doc)
  end

  def find_one(coll, doc)
    h = coll.find_one
    raise "find_one failed" unless h
  end

  def find_many_setup(db, coll, doc, iterations)
    (0...(iterations - coll.size)).each{insert(coll, doc)} #TODO - insert many
    @cursor = coll.find
    @queries = 1
  end

  def find_many(coll, doc)
    h = @cursor.next
    unless h
      @cursor = coll.find
      @queries += 1
      @cursor.next
    end
  end

  def find_many_teardown(db, coll)
    puts "queries: #{@queries}" if @queries > 1
    default_teardown(db, coll)
  end

  def test_null
    # just setup/teardown for options parsing and sys_info
  end

  def test_array_fast
    [
        [2, (0..14), (-1..-1), :array_size_fixnum, :null_setup, :insert, :default_teardown],
        [2, (0..17), (-1..-1), :hash_size_fixnum, :null_setup, :insert, :default_teardown],
        [2, (0..12), (-1..-1), :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [2, (0..15), (-1..-1), :hash_nest_fixnum, :null_setup, :insert, :default_teardown],
    ].each{|args| @results += power_test(args)}
  end

  def test_insert_one
    [
        [2, (0..15), (-1..-1), :value_string_size, :null_setup, :insert, :default_teardown],
        [2, (0..15), (-1..-1), :key_string_size, :null_setup, :insert, :default_teardown],
        [2, (0..14), (-1..-1), :array_size_fixnum, :null_setup, :insert, :default_teardown],
        [2, (0..17), (-1..-1), :hash_size_fixnum, :null_setup, :insert, :default_teardown],
        [2, (0..12), (-1..-1), :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [2, (0..15), (-1..-1), :hash_nest_fixnum, :null_setup, :insert, :default_teardown],
    ].each{|args| @results += power_test(args)}
  end

  def test_insert_one_safe
    [
        [2, (0..15), (-1..-1), :value_string_size, :null_setup, :insert_safe, :default_teardown],
        [2, (0..15), (-1..-1), :key_string_size, :null_setup, :insert_safe, :default_teardown],
        [2, (0..14), (-1..-1), :array_size_fixnum, :null_setup, :insert_safe, :default_teardown],
        [2, (0..17), (-1..-1), :hash_size_fixnum, :null_setup, :insert_safe, :default_teardown],
        [2, (0..12), (-1..-1), :array_nest_fixnum, :null_setup, :insert_safe, :default_teardown],
        [2, (0..15), (-1..-1), :hash_nest_fixnum, :null_setup, :insert_safe, :default_teardown],
    ].each{|args| @results += power_test(args)}
  end

  def test_insert_many
    sizes = [0, 2, 4, 6, 8, 10, 12]
    multi = [0, 2, 4, 6, 8, 10, 12]
    [
        [2, sizes, multi, :value_string_size, :null_setup, :insert, :default_teardown],
        [2, sizes, multi, :key_string_size, :null_setup, :insert, :default_teardown],
        [2, sizes, multi, :hash_size_fixnum, :null_setup, :insert, :default_teardown],
        [2, sizes, multi, :array_size_fixnum, :null_setup, :insert, :default_teardown],
        [2, sizes, multi, :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [2, sizes, multi, :hash_nest_fixnum, :null_setup, :insert, :default_teardown],
    ].each{|args| @results += power_test(args)}
  end

  def test_insert_many_safe
    sizes = [0, 2, 4, 6, 8, 10, 12]
    multi = [0, 2, 4, 6, 8, 10, 12]
    [
        [2, sizes, multi, :value_string_size, :null_setup, :insert_safe, :default_teardown],
        [2, sizes, multi, :key_string_size, :null_setup, :insert_safe, :default_teardown],
        [2, sizes, multi, :hash_size_fixnum, :null_setup, :insert_safe, :default_teardown],
        [2, sizes, multi, :array_size_fixnum, :null_setup, :insert_safe, :default_teardown],
        [2, sizes, multi, :array_nest_fixnum, :null_setup, :insert_safe, :default_teardown],
        [2, sizes, multi, :hash_nest_fixnum, :null_setup, :insert_safe, :default_teardown],
    ].each{|args| @results += power_test(args)}
  end

  def test_find_one
    [
        [2, (0..15), (-1..-1), :value_string_size, :find_one_setup, :find_one, :default_teardown],
        [2, (0..15), (-1..-1), :key_string_size, :find_one_setup, :find_one, :default_teardown],
        [2, (0..14), (-1..-1), :array_size_fixnum, :find_one_setup, :find_one, :default_teardown],
        [2, (0..17), (-1..-1), :hash_size_fixnum, :find_one_setup, :find_one, :default_teardown],
        [2, (0..12), (-1..-1), :array_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
        [2, (0..15), (-1..-1), :hash_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
    ].each{|args| @results += power_test(args)}
  end

  def test_find_many
    [
        [2, (0..15), (-1..-1), :value_string_size, :find_many_setup, :find_many, :find_many_teardown],
        [2, (0..15), (-1..-1), :key_string_size, :find_many_setup, :find_many, :find_many_teardown],
        [2, (0..14), (-1..-1), :array_size_fixnum, :find_many_setup, :find_many, :find_many_teardown],
        [2, (0..17), (-1..-1), :hash_size_fixnum, :find_many_setup, :find_many, :find_many_teardown],
        [2, (0..12), (-1..-1), :array_nest_fixnum, :find_many_setup, :find_many, :find_many_teardown],
        [2, (0..15), (-1..-1), :hash_nest_fixnum, :find_many_setup, :find_many, :find_many_teardown],
    ].each{|args| @results += power_test(args)}
  end

  def test_insert_one_nest_full
    [
        [4, (0..6), (-1..-1), :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [8, (0..4), (-1..-1), :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [16, (0..3), (-1..-1), :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [32, (0..2), (-1..-1), :array_nest_fixnum, :null_setup, :insert, :default_teardown],
        [4, (0..8), (-1..-1), :hash_nest_fixnum, :null_setup, :insert, :default_teardown],
        [8, (0..4), (-1..-1), :hash_nest_fixnum, :null_setup, :insert, :default_teardown],
        [16, (0..4), (-1..-1), :hash_nest_fixnum, :null_setup, :insert, :default_teardown],
        [32, (0..3), (-1..-1), :hash_nest_fixnum, :null_setup, :insert, :default_teardown],
    ].each{|args| @results += power_test(args)}
  end

  def test_find_one_nest_full
    [
        [4, (0..6), (-1..-1), :array_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
        [8, (0..4), (-1..-1), :array_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
        [16, (0..3), (-1..-1), :array_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
        [32, (0..2), (-1..-1), :array_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
        [4, (0..8), (-1..-1), :hash_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
        [8, (0..4), (-1..-1), :hash_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
        [16, (0..4), (-1..-1), :hash_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
        [32, (0..3), (-1..-1), :hash_nest_fixnum, :find_one_setup, :find_one, :default_teardown],
    ].each{|args| @results += power_test(args)}
  end

  def xtest_update
    [
        # pending
    ].each{|args| @results += power_test(args)}
  end

  def xtest_remove
    [
        # pending
    ].each{|args| @results += power_test(args)}
  end

  # synthesized mix, real-world data pending

end


