# encoding: UTF-8

# --
# Copyright (C) 2008-2011 10gen Inc.
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
  module WriteConcern

    attr_reader :legacy_write_concern

    @@safe_warn = nil
    def write_concern_from_legacy(opts)
      # Warn if 'safe' parameter is being used,
      if opts.key?(:safe) && !@@safe_warn && !ENV['TEST_MODE']
        warn "[DEPRECATED] The 'safe' write concern option has been deprecated in favor of 'w'."
        @@safe_warn = true
      end

      # nil:   set :w => 0
      # false: set :w => 0
      # true:  set :w => 1
      # hash:  set :w => 0 and merge with opts

      unless opts.has_key?(:w)
        opts[:w] = 0 # legacy default, unacknowledged
        safe     = opts.delete(:safe)
        if(safe && safe.is_a?(Hash))
          opts.merge!(safe)
        elsif(safe == true)
          opts[:w] = 1
        end
      end
    end

    # todo: throw exception for conflicting write concern options
    def get_write_concern(opts, parent=nil)
      write_concern_from_legacy(opts) if opts.key?(:safe) || @legacy_write_concern
      write_concern = {
        :w        => 1,
        :j        => false,
        :fsync    => false,
        :wtimeout => nil
      }
      write_concern.merge!(parent.write_concern) if parent
      write_concern.merge!(opts.reject {|k,v| !write_concern.keys.include?(k)})
      write_concern
    end

    def self.gle?(write_concern)
      (write_concern[:w].is_a? Symbol) ||
      (write_concern[:w].is_a? String) ||
      write_concern[:w] > 0 ||
      write_concern[:j] ||
      write_concern[:fsync] ||
      write_concern[:wtimeout]
    end

  end
end