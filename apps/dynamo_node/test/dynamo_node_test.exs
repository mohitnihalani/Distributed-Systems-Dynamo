defmodule DynamoNodeTest do
  use ExUnit.Case
  doctest DynamoNode

  test "greets the world" do
    assert DynamoNode.hello() == :world
  end
end
