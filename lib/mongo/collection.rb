# Copyright (C) 2013 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo

  # Represents a collection in the database and operations that can directly be
  # applied to one.
  #
  # @since 2.0.0
  class Collection

    # @!attribute database
    #   @return [ Mongo::Database ] The database the collection resides in.
    # @!attribute name
    #   @return [ String ] The name of the collection.


    attr_reader :database, :name

    # @todo: durran: implement.
    def initialize(database, name)
      raise InvalidName.new unless name
      @database = database
      @name = name.to_s
    end

    # Exception that is raised when trying to create a collection with no name.
    #
    # @since 2.0.0
    class InvalidName < RuntimeError

      # The message is constant.
      #
      # @since 2.0.0
      MESSAGE = 'nil is an invalid collection name. ' +
        'Please provide a string or symbol.'

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Collection::InvalidName.new
      #
      # @since 2.0.0
      def initialize
        super(MESSAGE)
      end
    end
  end
end
