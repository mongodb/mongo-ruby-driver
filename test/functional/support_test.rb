require 'test_helper'

class SupportTest < Test::Unit::TestCase

  def test_command_response_succeeds
    assert Support.ok?('ok' => 1)
    assert Support.ok?('ok' => 1.0)
    assert Support.ok?('ok' => true)
  end

  def test_command_response_fails
    assert !Support.ok?('ok' => 0)
    assert !Support.ok?('ok' => 0.0)
    assert !Support.ok?('ok' => 0.0)
    assert !Support.ok?('ok' => 'str')
    assert !Support.ok?('ok' => false)
  end

  def test_array_of_pairs
    hps = [["localhost", 27017], ["localhost", 27018], ["localhost", 27019]]
    assert_equal [["localhost", 27017], ["localhost", 27018], ["localhost", 27019]], Support.normalize_seeds(hps)
  end

  def test_array_of_strings
    hps = ["localhost:27017", "localhost:27018", "localhost:27019"]
    assert_equal [["localhost", 27017], ["localhost", 27018], ["localhost", 27019]], Support.normalize_seeds(hps)
  end

  def test_single_string_with_host_port
    hps = "localhost:27017"
    assert_equal ["localhost", 27017], Support.normalize_seeds(hps)
  end

  def test_single_string_missing_port
    hps = "localhost"
    assert_equal ["localhost", 27017], Support.normalize_seeds(hps)
  end

  def test_single_element_array_missing_port
    hps = ["localhost"]
    assert_equal ["localhost", 27017], Support.normalize_seeds(hps)
  end

  def test_pair_doesnt_get_converted
    hps = ["localhost", 27017]
    assert_equal ["localhost", 27017], Support.normalize_seeds(hps)
  end
end
