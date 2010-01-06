# --
# Copyright (C) 2008-2009 10gen Inc.
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

module Mongo

  # @deprecated this class is deprecated. Methods defined here will 
  #   henceforth be available in Mongo::DB.
  class Admin

    def initialize(db)
      warn "The Admin class has been DEPRECATED. All admin methods now exist in DB."
      @db = db
    end

    # Return the current database profiling level.
    #
    # @return [Symbol] :off, :slow_only, or :all
    #
    # @deprecated please use DB#profiling_level instead.
    def profiling_level
      warn "Admin#profiling_level has been DEPRECATED. Please use DB#profiling_level instead."
      oh = OrderedHash.new
      oh[:profile] = -1
      doc = @db.command(oh)
      raise "Error with profile command: #{doc.inspect}" unless @db.ok?(doc) && doc['was'].kind_of?(Numeric)
      case doc['was'].to_i
      when 0
        :off
      when 1
        :slow_only
      when 2
        :all
      else
        raise "Error: illegal profiling level value #{doc['was']}"
      end
    end

    # Set database profiling level to :off, :slow_only, or :all.
    #
    # @deprecated please use DB#profiling_level= instead.
    def profiling_level=(level)
      warn "Admin#profiling_level= has been DEPRECATED. Please use DB#profiling_level= instead."
      oh = OrderedHash.new
      oh[:profile] = case level
                     when :off
                       0
                     when :slow_only
                       1
                     when :all
                       2
                     else
                       raise "Error: illegal profiling level value #{level}"
                     end
      doc = @db.command(oh)
      raise "Error with profile command: #{doc.inspect}" unless @db.ok?(doc)
    end

    # Returns an array containing current profiling information.
    #
    # @deprecated please use DB#profiling_info instead.
    def profiling_info
      warn "Admin#profiling_info has been DEPRECATED. Please use DB#profiling_info instead."
      Cursor.new(Collection.new(@db, DB::SYSTEM_PROFILE_COLLECTION), :selector => {}).to_a
    end

    # Validate a named collection by raising an exception if there is a
    # problem or returning an interesting hash (see especially the
    # 'result' string value) if all is well.
    #
    # @deprecated please use DB#validate_collection instead.
    def validate_collection(name)
      warn "Admin#validate_collection has been DEPRECATED. Please use DB#validate_collection instead."
      doc = @db.command(:validate => name)
      raise "Error with validate command: #{doc.inspect}" unless @db.ok?(doc)
      result = doc['result']
      raise "Error with validation data: #{doc.inspect}" unless result.kind_of?(String)
      raise "Error: invalid collection #{name}: #{doc.inspect}" if result =~ /\b(exception|corrupt)\b/i
      doc
    end

  end
end
