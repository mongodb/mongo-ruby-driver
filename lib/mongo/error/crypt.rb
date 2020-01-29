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
  class Error

    # An error related to field-level encryption
    class CryptError < Mongo::Error
      # Create a new CryptError
      #
      # @param [ Integer | nil ] code The optional libmongocrypt error code
      # @param [ String ] message The error message
      def initialize(code, message)
        msg = message
        msg += " (libmongocrypt error code #{code})" if code
        super(msg)
      end
    end
  end
end
