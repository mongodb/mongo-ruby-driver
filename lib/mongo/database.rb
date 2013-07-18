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

  class Database

    attr_reader :name

    def [](collection_name)
      Collection.new(collection_name)
    end

    def initialize(name)
      raise InvalidName.new unless name
      @name = name.to_s
    end

    class InvalidName < RuntimeError

      MESSAGE = 'nil is an invalid database name. ' +
        'Please provide a string or symbol.'

      def initialize
        super(MESSAGE)
      end
    end
  end
end
