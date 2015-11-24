require 'mongo'
require 'json'

# Benchmark helper class.
# Common operations that are used in benchmarking.
#
# @since 2.2.1
class BenchmarkHelper
  # Accessor methods for
  attr_reader :database, :collection


  # Initializes a client connection, creating a database and a collection,
  # setting the max pool size, and setting the write concern to 1.
  #
  # @example Initialize a client connection.
  #   BenchmarkHelper.initialize_collection("testing")
  #
  # @param [ String, Symbol ] database_name The name of the database.
  # @param [ String, Symbol ] collection_name The name of the collection.
  # @param [ Integer ] pool_size Max number of simultaneous DB connections allowed.
  #
  # @since 2.2.1
  def initialize(database_name, collection_name, pool_size = 5)
    Mongo::Logger.level = Logger::INFO
    @client = Mongo::Client.new(
        [ '127.0.0.1:27017' ],
        database: database_name,
        write: { :w => 1 },
        max_pool_size: pool_size
    )
    @database = @client.database
    @collection = @client[collection_name]
  end


  class << self


    # Load a file into a string
    #
    # @example Load a file into a string
    #   BenchmarkHelper.load_string_from_file("GRIDFS_LARGE.txt")
    #
    # @param [ String ] file_name The name of the data file.
    #
    # @return [ String ] A string of all the file data.
    #
    # @since 2.2.1
    def load_string_from_file(file_name)
      File.read(file_name)
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
    def load_array_from_file(data_file_name)
      data_array = []
      File.open(data_file_name, "r") do |f|
        f.each_line do |line|
          data_array << JSON.parse(line)
        end
      end
      data_array
    end


    # Write documents from an array into the specified file.
    # One document written per line in the file
    #
    # @example Write the data into the specified file
    #   BenchmarkHelper.write_documents_to_file("foo.txt", data)
    #
    # @param [ String ] path The path to the file to which to write the data.
    # @param [ Array<Hash> ] data An array of document data.
    #
    # @since 2.2.1
    def write_documents_to_file(path, data)
      dir = File.dirname(path)
      FileUtils.mkdir_p(dir) unless File.directory?(dir)

      File.open(path, 'w') { |f| f.puts(data) } if data
    end


    # Make a file directory with the given directory name
    #
    # @example Make a directory
    #   BenchmarkHelper.make_directory("tmp")
    #
    # @param [ String ] directory_name The name of the file directory.
    #
    # @return [ Array<String>, nil ] An array of directories created, or nil if none were created.
    #
    # @since 2.2.1
    def make_directory(directory_name)
      FileUtils.mkdir_p(directory_name) unless File.directory?(directory_name)
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
    def mmabench_composite_score(scores)
      scores.inject(0.0) { |sum, score| sum + score } / scores.size
    end


    # Determines the median value of the given numbers.
    # The median value is currently defined as the 50th percentile value
    #
    # @example Get the median value
    #   BenchmarkHelper.get_median(3,5,6,2)
    #
    # @param [ Array<Double> ] numbers The set of numbers from which to get the median.
    #
    # @return [ Double ] The median of the numbers.
    #
    # @since 2.2.1
    def median(numbers)
      percentile_value(50, numbers)
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
    def percentile_value(percentile, scores)
      scores.sort[ ((percentile / 100.0) * scores.size  - 1).ceil ]
    end


    # Calculate the 10th, 25th, 50th, 75th, 90th, 95th, 98th and 99th percentiles for the
    # given scores
    #
    # @example Get the median value
    #   BenchmarkHelper.get_median(3,5,6,2)
    #
    # @param [ Array<Double> ] scores The set of scores from which to obtain percentiles.
    #
    # @return [ Array<Double> ] The 10th, 25th, 50th, 75th, 90th, 95th, 98th and 99th percentiles.
    #
    # @since 2.2.1
    def percentile_values(scores)
      percentiles = [10,20,50,75,90,95,98,99]
      percentiles.map { |percentile| percentile_value(percentile, scores) }
    end


    # Calculate MBs per Second, using the median (50th percentile) time data result.
    #
    # @example Get the median value
    #   BenchmarkHelper.get_median(3,5,6,2)
    #
    # @param [ Array<Double> ] scores The set of scores from which to obtain percentiles.
    # @param [ Double ] mb The number of MBs of data
    #
    # @return [ Array<Double> ] The 10th, 25th, 50th, 75th, 90th, 95th, 98th and 99th percentiles.
    #
    # @since 2.2.1
    def MB_per_second(scores, mb)
      data_percentiles = percentile_values(scores)
      median = data_percentiles[2]
      mb / median
    end


  end
end
