require './test/test_helper'
require 'rubygems'
require 'json'

class JSONTest < Test::Unit::TestCase

  def test_object_id_as_json
    id = BSON::ObjectId.new
    p id.to_json

    obj = {'_id' => id}
    assert_equal "{\"_id\":#{id.to_json}}", obj.to_json
  end

end
