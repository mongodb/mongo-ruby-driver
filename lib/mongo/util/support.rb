# encoding: UTF-8

# --
# Copyright (C) 2008-2012 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

require 'digest/md5'

module Mongo
  module Support

    include Mongo::Conversions
    extend self

    # Commands that may be sent to replica-set secondaries, depending on
    # read preference and tags. All other commands are always run on the primary.
    SECONDARY_OK_COMMANDS = [
      'group',
      'aggregate',
      'collstats',
      'dbstats',
      'count',
      'distinct',
      'geonear',
      'geosearch',
      'geowalk',
      'mapreduce',
      'replsetgetstatus',
      'ismaster',
    ]

    # Generate an MD5 for authentication.
    #
    # @param [String] username
    # @param [String] password
    # @param [String] nonce
    #
    # @return [String] a key for db authentication.
    def auth_key(username, password, nonce)
      Digest::MD5.hexdigest("#{nonce}#{username}#{hash_password(username, password)}")
    end

    # Return a hashed password for auth.
    #
    # @param [String] username
    # @param [String] plaintext
    #
    # @return [String]
    def hash_password(username, plaintext)
      Digest::MD5.hexdigest("#{username}:mongo:#{plaintext}")
    end

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

    def secondary_ok?(selector)
      command = selector.keys.first.to_s.downcase

      if command == 'mapreduce'
        out = selector.select { |k, v| k.to_s.downcase == 'out' }.first.last
        # mongo looks at the first key in the out object, and doesn't
        # look at the value
        out.is_a?(Hash) && out.keys.first.to_s.downcase == 'inline' ? true : false
      else
        SECONDARY_OK_COMMANDS.member?(command)
      end
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
      doc['ok'] == 1.0 || doc['ok'] == true
    end
  end
end
