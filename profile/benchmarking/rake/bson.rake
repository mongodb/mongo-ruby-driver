# frozen_string_literal: true

desc 'Run the full BSON benchmarking suite'
task :bson do
  puts 'BSON BENCHMARK SUITE'
  Mongo::Benchmarking.report({
    bson: Mongo::Benchmarking::BSON.run_all({
      flat: %i[ encode decode ],
      deep: %i[ encode decode ],
      full: %i[ encode decode ],
    })
  })
end

namespace :bson do
  desc 'Learn how to run the BSON benchmarks'
  task :help do
    puts <<~HELP
      The BSON micro benchmarks require a set of data files that are stored in
      the specifications repository, here:

        https://github.com/mongodb/specifications/tree/master/source/benchmarking/data

      Download the `extended_bson.tgz` file and extract its contents. It should
      contain a single folder (`extended_bson`) with several files in it. Move
      those files to:

        #{Mongo::Benchmarking::DATA_PATH}

      Once there, you may run any of the BSON benchmarking tasks:

        $ rake benchmark:bson:flat:encode

      Tasks may be run in aggregate, as well, by specifying the namespace
      directly:

        $ rake benchmark:bson:flat # runs all flat BSON benchmarks
        $ rake benchmark:bson:deep # runs all deep BSON benchmarks
        $ rake benchmark:bson:full # runs all full BSON benchmarks
        # rake benchmark:bson      # runs all BSON benchmarks
    HELP
  end

  desc 'Run the `flat` BSON benchmarking suite'
  task :flat do
    puts 'BSON BENCHMARK :: FLAT'
    Mongo::Benchmarking.report({
      bson: Mongo::Benchmarking::BSON.run_all({ flat: %i[ encode decode ] })
    })
  end

  namespace :flat do
    desc 'Run the `flat` encoding BSON benchmark'
    task :encode do
      puts 'BSON BENCHMARK :: FLAT :: ENCODE'
      Mongo::Benchmarking.report({ bson: { flat: { encode: Mongo::Benchmarking::BSON.run(:flat, :encode) } } })
    end

    desc 'Run the `flat` decoding BSON benchmark'
    task :decode do
      puts 'BSON BENCHMARK :: FLAT :: DECODE'
      Mongo::Benchmarking.report({ bson: { flat: { decode: Mongo::Benchmarking::BSON.run(:flat, :decode) } } })
    end
  end

  desc 'Run the `deep` BSON benchmarking suite'
  task :deep do
    puts 'BSON BENCHMARK :: DEEP'
    Mongo::Benchmarking.report({
      bson: Mongo::Benchmarking::BSON.run_all({ deep: %i[ encode decode ] })
    })
  end

  namespace :deep do
    desc 'Run the `deep` encoding BSON benchmark'
    task :encode do
      puts 'BSON BENCHMARK :: DEEP :: ENCODE'
      Mongo::Benchmarking.report({ bson: { deep: { encode: Mongo::Benchmarking::BSON.run(:deep, :encode) } } })
    end

    desc 'Run the `deep` decoding BSON benchmark'
    task :decode do
      puts 'BSON BENCHMARK :: DEEP :: DECODE'
      Mongo::Benchmarking.report({ bson: { deep: { decode: Mongo::Benchmarking::BSON.run(:deep, :decode) } } })
    end
  end

  desc 'Run the `full` BSON benchmarking suite'
  task :full do
    puts 'BSON BENCHMARK :: FULL'
    Mongo::Benchmarking.report({
      bson: Mongo::Benchmarking::BSON.run_all({ full: %i[ encode decode ] })
    })
  end

  namespace :full do
    desc 'Run the `full` encoding BSON benchmark'
    task :encode do
      puts 'BSON BENCHMARK :: FULL :: ENCODE'
      Mongo::Benchmarking.report({ bson: { full: { encode: Mongo::Benchmarking::BSON.run(:full, :encode) } } })
    end

    desc 'Run the `full` decoding BSON benchmark'
    task :decode do
      puts 'BSON BENCHMARK :: FULL :: DECODE'
      Mongo::Benchmarking.report({ bson: { full: { decode: Mongo::Benchmarking::BSON.run(:full, :decode) } } })
    end
  end
end
