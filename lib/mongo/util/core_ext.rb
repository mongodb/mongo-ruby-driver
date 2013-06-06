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

#:nodoc:
class Object

  #:nodoc:
  def tap
    yield self
    self
  end unless respond_to? :tap

end

#:nodoc:
class Hash

  #:nodoc:
  def assert_valid_keys(*valid_keys)
    unknown_keys = keys - [valid_keys].flatten
    raise(ArgumentError, "Unknown key(s): #{unknown_keys.join(", ")}") unless unknown_keys.empty?
  end

end

#:nodoc:
class String

  #:nodoc:
  def to_bson_code
    BSON::Code.new(self)
  end

end

#:nodoc:
class Class
  def mongo_thread_local_accessor name, options = {}
    m = Module.new
    m.module_eval do
      class_variable_set :"@@#{name}", Hash.new {|h,k| h[k] = options[:default] }
    end
    m.module_eval %{

      def #{name}
        @@#{name}[Thread.current.object_id]
      end

      def #{name}=(val)
        @@#{name}[Thread.current.object_id] = val
      end
    }

    class_eval do
      include m
      extend m
    end
  end
end
