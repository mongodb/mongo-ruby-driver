# frozen_string_literal: true

require_relative 'single_doc/find_one_by_id'
require_relative 'single_doc/insert_one'
require_relative 'single_doc/run_command'

module Mongo
  module DriverBench
    module SingleDoc
      ALL = [ FindOneByID, *InsertOne::ALL, RunCommand ].freeze

      # SingleBench consists of all Single-doc micro-benchmarks
      # except "Run Command"
      BENCH = (ALL - [ RunCommand ]).freeze
    end
  end
end
