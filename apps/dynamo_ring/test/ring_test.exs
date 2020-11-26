defmodule RingTest do
  use ExUnit.Case
  doctest Ring

  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "check ring creating and adding server" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    ring = Ring.new()
    assert Ring.get_node_count(ring) == 0
    tree = :gb_trees.empty
    ring = Ring.add_nodes(ring, [:a,:b])
    assert Ring.get_node_count(ring) == 2
    assert :gb_trees.size(ring.ring) == 256
    result = Ring.nodes_for_key(ring, "foo", 2)
  after
    Emulation.terminate()
  end

  test "Test Remove Node" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    ring = Ring.new()
    assert Ring.get_node_count(ring) == 0
    tree = :gb_trees.empty
    ring = Ring.add_nodes(ring, [:a, :b])
    assert Ring.get_node_count(ring) == 2
    ring = Ring.remove_node(ring, :a)
    assert Ring.get_node_count(ring) == 1
  after
    Emulation.terminate()
  end

  test "Test Node for key" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    ring = Ring.new()
    ring = Ring.add_nodes(ring, [:a, :b, :c, :d])
    preference_list = Ring.nodes_for_key(ring, "foo", 3)
    Enum.each(preference_list, fn node -> IO.puts(node) end)
  after
    Emulation.terminate()
  end

  test "Check Sync Ring" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    ring1 = Ring.new()
    ring2 = Ring.new()
    ring1 = Ring.add_nodes(ring1, [:a, :b, :c])
    ring2 = Ring.add_nodes(ring2, [:a, :b, :d])
    ring1 = Ring.sync_rings(ring1, ring2)
    assert Ring.get_node_count(ring1) == 4
    assert MapSet.equal?(MapSet.new([:a, :b, :c, :d]), Ring.get_nodes_list(ring1)) == true
    assert MapSet.equal?(MapSet.new([:a, :b, :c, :d]), MapSet.new(Ring.get_nodes_list(ring1))) == true
  after
    Emulation.terminate()
  end
end
