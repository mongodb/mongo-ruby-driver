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

  # A Regexp that can hold on to extra options and ignore them. Mongo
  # regexes may contain option characters beyond 'i', 'm', and 'x'. (Note
  # that Mongo only uses those three, but that regexes coming from other
  # languages may store different option characters.)
  #
  # Note that you do not have to use this class at all if you wish to
  # store regular expressions in Mongo. The Mongo and Ruby regex option
  # flags are the same. Storing regexes is discouraged, in any case.
  # 
  # @deprecated
  class RegexpOfHolding < Regexp

    attr_accessor :extra_options_str

    # @deprecated we're no longer supporting this.
    # +str+ and +options+ are the same as Regexp. +extra_options_str+
    # contains all the other flags that were in Mongo but we do not use or
    # understand.
    def initialize(str, options, extra_options_str)
      warn "RegexpOfHolding is deprecated; the modifiers i, m, and x will be stored automatically as BSON." +
        "If you're only storing the options i, m, and x, you can safely ignore this message."
      super(str, options)
      @extra_options_str = extra_options_str
    end
  end

end
