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
