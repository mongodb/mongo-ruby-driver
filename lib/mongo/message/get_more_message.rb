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

require 'mongo/message/message'
require 'mongo/message/opcodes'

module Mongo

  class GetMoreMessage < Message

    def initialize(db_name, collection_name, cursor)
      super(OP_GET_MORE)
      write_int(0)
      write_string("#{db_name}.#{collection_name}")
      write_int(0)              # num to return; leave it up to the db for now
      write_long(cursor)
    end
  end
end
