desc  "Run benchmarks (ex: benchmark, benchmark[c], benchmark[ruby])"
task :benchmark, :suite do |t, args|
  unless args[:suite]
    puts "[BENCHMARK] Running ALL Benchmark Suites..."
    Rake::Task['benchmark:c'].invoke
    Rake::Task['benchmark:ruby'].invoke
  else
    task = "benchmark:#{args[:suite]}"
    if Rake::Task.task_defined?(task)
      puts "[BENCHMARK] Running the #{args[:suite].upcase} Benchmark Suites..."
      Rake::Task[task].invoke
    end
  end
end

namespace :benchmark do

  $suite = {
      :suite_insert_one =>  [ :test_insert_one, :test_insert_one_safe ],
      :suite_insert_many => [ :test_insert_many, :test_insert_many_safe ],
      :suite_find =>        [ :test_find_one, :test_find_many ],
      :suite_nest =>        [ :test_insert_one_nest_full, :test_find_one_nest_full ],
  }
  $date = Time.now.strftime('%Y%m%d-%H%M')

  def suite_series_name(suite)
    "tasks/benchmark/exp_series_#{suite}_#{ENV['MODE']}"
  end

  def suite_file_name(suite)
    suite_series_name(suite) + '.js'
  end

  def suite_file_name_temp(suite)
    suite_file_name(suite) + '.tmp'
  end


  task :prepare do
    require 'benchmark'

    $suite.each do |suite, tests|
      tests.each do |t|
        task t do
          system "ruby tasks/benchmark/exp_series.rb --file #{suite_file_name_temp(suite)} --mode #{ENV['MODE']} --tag #{suite} -- --name #{t}"
        end
      end

      desc "#{suite} - #{$suite[suite].join(', ')}"
      task suite do |t|
        File.open(suite_file_name_temp(suite), 'w'){|f| f.puts("#{suite_series_name(suite)} = [")}
        btms = Benchmark.measure do
          $suite[suite].each do |pre|
            Rake::Task[pre].execute
          end
        end
        File.open(suite_file_name_temp(suite), 'a'){|f| f.puts("]; // #{(btms.real/60.0).round} minutes")}
        system "mv #{suite_file_name_temp(suite)} #{suite_file_name(suite)}"
      end
    end
  end

  # Run the C benchmark suites
  task :c => :prepare do
    ENV['MODE'] = 'c'
    $suite.each_key do |key|
      Rake::Task[key].invoke
    end
  end

  # Run the Ruby benchmark suites
  task :ruby => :prepare do
    ENV['MODE'] = 'ruby'
    $suite.each_key do |suite|
      Rake::Task[suite].execute
    end
  end

  desc "Cleanup from benchmark runs"
  task :cleanup do
    system "rm -f tasks/benchmark/exp_series_suite_*.js"
  end

end