# Copyright (C) 2009-2013 MongoDB, Inc.
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
  module Support

    include Mongo::Conversions
    extend self

    def validate_db_name(db_name)
      unless [String, Symbol].include?(db_name.class)
        raise TypeError, "db_name must be a string or symbol"
      end

      [" ", ".", "$", "/", "\\"].each do |invalid_char|
        if db_name.include? invalid_char
          raise Mongo::InvalidNSName, "database names cannot contain the character '#{invalid_char}'"
        end
      end
      raise Mongo::InvalidNSName, "database name cannot be the empty string" if db_name.empty?
      db_name
    end

    def format_order_clause(order)
      case order
        when Hash, BSON::OrderedHash then hash_as_sort_parameters(order)
        when String, Symbol then string_as_sort_parameters(order)
        when Array then array_as_sort_parameters(order)
        else
          raise InvalidSortValueError, "Illegal sort clause, '#{order.class.name}'; must be of the form " +
            "[['field1', '(ascending|descending)'], ['field2', '(ascending|descending)']]"
      end
    end

    def normalize_seeds(seeds)
      pairs = Array(seeds)
      pairs = [ seeds ] if pairs.last.is_a?(Fixnum)
      pairs = pairs.collect do |hostport|
        if hostport.is_a?(String)
          if hostport[0,1] == '['
            host, port = hostport.split(']:') << MongoClient::DEFAULT_PORT
            host = host.end_with?(']') ? host[1...-1] : host[1..-1]
          else
            host, port = hostport.split(':') << MongoClient::DEFAULT_PORT
          end
          [ host, port.to_i ]
        else
          hostport
        end
      end
      pairs.length > 1 ? pairs : pairs.first
    end

    def is_i?(value)
      return !!(value =~ /^\d+$/)
    end

    # Determine if a database command has succeeded by
    # checking the document response.
    #
    # @param [Hash] doc
    #
    # @return [Boolean] true if the 'ok' key is either 1 or *true*.
    def ok?(doc)
      ok = doc['ok']
      ok == 1 || ok == 1.0 || ok == true
    end
  end
end
