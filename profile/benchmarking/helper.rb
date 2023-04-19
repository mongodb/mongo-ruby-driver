# frozen_string_literal: true
# rubocop:todo all

module Mongo

  # Helper functions used by benchmarking tasks
  module Benchmarking

    extend self

    # Load a json file and represent each document as a Hash.
    #
    # @example Load a file.
    #   Benchmarking.load_file(file_name)
    #
    # @param [ String ] The file name.
    #
    # @return [ Array ] A list of extended-json documents.
    #
    # @since 2.2.3
    def load_file(file_name)
      File.open(file_name, "r") do |f|
        f.each_line.collect do |line|
          parse_json(line)
        end
      end
    end

    # Load a json document as a Hash and convert BSON-specific types.
    # Replace the _id field as an BSON::ObjectId if it's represented as '$oid'.
    #
    # @example Parse a json document.
    #   Benchmarking.parse_json(document)
    #
    # @param [ Hash ] The json document.
    #
    # @return [ Hash ] An extended-json document.
    #
    # @since 2.2.3
    def parse_json(document)
      JSON.parse(document).tap do |doc|
        if doc['_id'] && doc['_id']['$oid']
          doc['_id'] = BSON::ObjectId.from_string(doc['_id']['$oid'])
        end
      end
    end

    # Get the median of values in a list.
    #
    # @example Get the median.
    #   Benchmarking.median(values)
    #
    # @param [ Array ] The values to get the median of.
    #
    # @return [ Numeric ] The median of the list.
    #
    # @since 2.2.3
    def median(values)
      values.sort![values.size/2-1]
    end
  end
end
