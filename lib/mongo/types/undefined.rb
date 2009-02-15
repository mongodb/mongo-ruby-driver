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

module XGen
  module Mongo
    module Driver

      # A special "undefined" type to match Mongo's storage of UNKNOWN values.
      # "UNKNOWN" comes from JavaScript.
      #
      # NOTE: this class does not attempt to provide ANY of the semantics an
      # "unknown" object might need. It isn't nil, it isn't special in any
      # way, and there isn't any singleton value.
      class Undefined < Object; end

    end
  end
end
