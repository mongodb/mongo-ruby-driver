# frozen_string_literal: true

require_relative '../base'

module Mongo
  module DriverBench
    module SingleDoc
      module InsertOne
        class Base < Mongo::DriverBench::SingleDoc::Base
          attr_reader :repetitions

          def before_task
            collection.drop
            collection.create
          end

          def do_task
            repetitions.times do |i|
              collection.insert_one(dataset)
            end
          end
        end
      end
    end
  end
end
