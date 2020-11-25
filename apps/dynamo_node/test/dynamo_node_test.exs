defmodule DynamoNodeTest do
  use ExUnit.Case
  doctest DynamoNode

  import Emulation, only: [spawn: 2, send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]


  test "Check Node Creation" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    spawn(:a, fn -> DynamoNode.lauch_node(DynamoNode.init()) end)
    spawn(:b, fn -> DynamoNode.lauch_node(DynamoNode.init()) end)
    spawn(:c, fn -> DynamoNode.lauch_node(DynamoNode.init()) end)
    spawn(:d, fn -> DynamoNode.lauch_node(DynamoNode.init()) end)

    caller = self()
    client = spawn(:c1, fn ->
      client = DynamNode.Client.new_client(:c1)
      assert DynamNode.Client.check_node_status(client, :a) == :ok
      assert DynamNode.Client.check_node_status(client, :b) == :ok
      assert DynamNode.Client.check_node_status(client, :c) == :ok
      assert DynamNode.Client.check_node_status(client, :d) == :ok
    end)
  after
    Emulation.terminate()
  end

  test "Test Gossip State Sharing" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    spawn(:a, fn -> DynamoNode.lauch_node(DynamoNode.init()) end)
    spawn(:b, fn -> DynamoNode.lauch_node(DynamoNode.init()) end)

    caller = self()
    client = spawn(:c1, fn ->
      client = DynamNode.Client.new_client(:c1)
      assert DynamNode.Client.check_node_status(client, :a) == :ok
      assert DynamNode.Client.check_node_status(client, :b) == :ok

       # Give things a bit of time to settle down.
       receive do
       after
         2_000 -> :ok
       end

       node_ring = Dynamo.Client.client_get_state(client, :b)
       assert Ring.get_node_count(node_ring) == 2
    end)
  after
    Emulation.terminate()
  end

end
