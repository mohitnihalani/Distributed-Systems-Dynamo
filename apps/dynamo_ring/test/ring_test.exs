defmodule RingTest do
  use ExUnit.Case
  doctest Ring

  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "greets the world" do
    assert Ring.hello() == :world
  end

  test "check ring creating and adding server" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    ring = Ring.new()
    assert Ring.get_node_count(ring) == 0
    tree = :gb_trees.empty
    ring = Ring.add_nodes(ring, [:a])
  after
    Emulation.terminate()
  end

end
