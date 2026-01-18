from include.etl.transformation.utils import generate_key


def test_generate_key_single_arg():
    """Test single arg produces consistent hash."""
    
    key1 = generate_key("test")
    key2 = generate_key("test")
    assert key1 == key2
    assert len(key1) == 16


def test_generate_key_multiple_args():
    """Test multiple arguments combined produce consistent hash."""

    key1 = generate_key("arg1", "arg2", "arg3")
    key2 = generate_key("arg1", "arg2", "arg3")
    assert key1 == key2
    assert len(key1) == 16
    assert len(key2) == 16


def test_generate_key_case_insensitive():
    """Test keys are case-insensitive for strings."""

    key1 = generate_key("TEST")
    key2 = generate_key("test")
    assert key1 == key2


def test_generate_key_none_values_ignored():
    """Test none values are excluded from hash calculation."""
    
    key1 = generate_key("test", None, "value")
    key2 = generate_key("test", "value")
    assert key1 == key2


def test_generate_key_numeric_args():
    """Numeric arguments are converted to strings."""

    key1 = generate_key("study", 123, 456.78)
    key2 = generate_key("study|123|456.78")
    assert key1 == key2
    assert len(key1) == 16
    assert len(key2) == 16


def test_generate_key_different_inputs_different_keys():
    """Different inputs produce different keys."""
    key1 = generate_key("input1")
    key2 = generate_key("input2")
    assert key1 != key2
