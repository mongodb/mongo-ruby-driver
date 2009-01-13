# --
# Copyright (C) 2008-2009 10gen Inc.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License, version 3, as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# ++

module XGen
  module Mongo
    module Driver

      # A Regexp that can hold on to extra options and ignore them. Mongo
      # regexes may contain option characters beyond 'i', 'm', and 'x'. (Note
      # that Mongo only uses those three, but that regexes coming from other
      # languages may store different option characters.)
      #
      # Note that you do not have to use this class at all if you wish to
      # store regular expressions in Mongo. The Mongo and Ruby regex option
      # flags are the same. Storing regexes is discouraged, in any case.
      class RegexpOfHolding < Regexp

        attr_accessor :extra_options_str

        # +str+ and +options+ are the same as Regexp. +extra_options_str+
        # contains all the other flags that were in Mongo but we do not use or
        # understand.
        def initialize(str, options, extra_options_str)
          super(str, options)
          @extra_options_str = extra_options_str
        end
      end

    end
  end
end
