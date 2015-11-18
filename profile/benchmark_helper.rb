require 'mongo'
require 'json'

class BenchmarkHelper

  # TODO: do these helper methods need tests? Do the benchmarks need tests?

  attr_accessor :database, :collection
  # Initializes a client connection and create a collection.
  #
  # @example Initialize a client connection and a collection.
  #   BenchmarkHelper.initialize_collection("testing")
  #
  # @param [ String, Symbol ] collection_name The name of the collection.
  # @param [ String, Symbol ] database_name The name of the database.
  #
  # @return [ Mongo::Collection ] The collection.
  #
  # @since 2.2.1
  def initialize(database_name, collection_name)
    Mongo::Logger.level = Logger::INFO
    @client = Mongo::Client.new(
        [ '127.0.0.1:27017' ],
        database: database_name,
        write: { :w => 1 }
    )
    @database = @client.database
    @collection = @client[collection_name]
  end


  # Load JSON document data from a file line by line into an array
  #
  # @example Load the file into an array
  #   BenchmarkHelper.load_array_from_file("TWITTER.txt")
  #
  # @param [ String ] data_file_name The name of the data file.
  #
  # @return [ Array<Hash> ] An array of document hashes.
  #
  # @since 2.2.1
  def self.load_array_from_file(data_file_name)
    # TODO: there must be an agreed upon format for datasets in order to load it from the file correctly
    data_array = []
    File.open('dataset.txt', "r") do |f| # TODO: change 'dataset.txt' to data_file_name parameter
      f.each_line do |line|
        data_array << JSON.parse(line)
      end
    end
    data_array
  end


  # Load a file into a string
  #
  # @example Load a file into a string
  #   BenchmarkHelper.load_string_from_file("GRIDFS_LARGE.txt")
  #
  # @param [ String ] data_file_name The name of the data file.
  #
  # @return [ String ] A string of all the file data.
  #
  # @since 2.2.1
  def self.load_string_from_file(data_file_name)
    file = File.open('dataset.txt', "rb") # TODO: change 'dataset.txt' to data_file_name parameter
    contents = file.read
    file.close
    contents
  end


  # Calculates the MMABench composite score for a number of scores.
  # Uses simple averages with equal weight
  #
  # @example Calculate the composite score
  #   BenchmarkHelper.mmabench_composite_score(3,5,6,2)
  #
  # @param [ Array<Double> ] scores The numbers to be averaged.
  #
  # @return [ Double ] The composite score.
  #
  # @since 2.2.1
  def self.mmabench_composite_score(scores)
    scores.inject(0.0) { |sum, score| sum + score } / scores.size
  end


  # Determines the percentile
  #
  # @example Get the median value
  #   BenchmarkHelper.get_median(3,5,6,2)
  #
  # @param [ Integer ] percentile The desired percentile
  # @param [ Array<Double> ] scores The set of scores from which to obtain the percentile.
  #
  # @return [ Double ] The median of the numbers.
  #
  # @since 2.2.1
  def self.percentile_value(percentile, scores)
    scores.sort[ ( (percentile / 100) * scores.size ) - 1 ]
  end


  # Determines the median value of the given numbers.
  #
  # @example Get the median value
  #   BenchmarkHelper.get_median(3,5,6,2)
  #
  # @param [ Array<Double> ] numbers The set of numbers from which to get the median.
  #
  # @return [ Double ] The median of the numbers.
  #
  # @since 2.2.1
  def self.median(numbers)
    sorted_numbers = numbers.sort
    mid_index = numbers.size / 2
    numbers.size % 2 == 0 ? (sorted_numbers[ mid_index ] + sorted_numbers[ mid_index + 1 ]) / 2 : sorted_numbers[ mid_index ]
  end

end
