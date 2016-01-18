module Mongo
  module Benchmarking

    CURRENT_PATH = File.expand_path(File.dirname(__FILE__))
    MICRO_TESTS_PATH = "#{CURRENT_PATH}/benchmarking/data/micro/"

    require 'benchmark'
    require_relative 'benchmarking/helper'
    require_relative 'benchmarking/micro'
    require_relative 'benchmarking/single_doc'
  end
end
