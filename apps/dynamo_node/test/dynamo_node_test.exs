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


    client = spawn(:c1, fn ->
      client = DynamoNode.Client.new_client(:c1)
      assert DynamoNode.Client.check_node_status(client, :a) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :b) == {:ok, client}
    end)

    handle = Process.monitor(client)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> assert false
    end
  after
    Emulation.terminate()
  end



  test "Test Gossip State Sharing" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    spawn(:a, fn -> DynamoNode.lauch_node(DynamoNode.init()) end)
    spawn(:b, fn -> DynamoNode.lauch_node(DynamoNode.init(:a)) end)
    spawn(:c, fn -> DynamoNode.lauch_node(DynamoNode.init(:b)) end)
    spawn(:d, fn -> DynamoNode.lauch_node(DynamoNode.init(:c)) end)

    caller = self()
    client = spawn(:c1, fn ->
      client = DynamoNode.Client.new_client(:c1)
      assert DynamoNode.Client.check_node_status(client, :a) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :b) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :c) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :d) == {:ok, client}

       # Give things a bit of time to settle down.
       receive do
       after
         10_000 -> :ok
       end

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :a)
       assert Ring.get_node_count(node_ring) == 4

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :b)
       assert Ring.get_node_count(node_ring) == 4

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :c)
       assert Ring.get_node_count(node_ring) == 4

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :d)
       assert Ring.get_node_count(node_ring) == 4

    end)

    handle = Process.monitor(client)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> assert false
    end

  after
    Emulation.terminate()
  end

  test "Test Put Request" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    spawn(:a, fn -> DynamoNode.lauch_node(DynamoNode.init()) end)
    spawn(:b, fn -> DynamoNode.lauch_node(DynamoNode.init(:a)) end)
    spawn(:c, fn -> DynamoNode.lauch_node(DynamoNode.init(:b)) end)
    spawn(:d, fn -> DynamoNode.lauch_node(DynamoNode.init(:c)) end)

    caller = self()
    client = spawn(:c1, fn ->
      client = DynamoNode.Client.new_client(:c1)
      assert DynamoNode.Client.check_node_status(client, :a) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :b) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :c) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :d) == {:ok, client}

       # Give things a bit of time to settle down.
       receive do
       after
         7_000 -> :ok
       end

       assert DynamoNode.Client.put_request(client, "foo", 200, nil, :b) == {:ok, client}
      #{node_ring, ^client} = DynamoNode.Client.client_get_state(client, :d)
      #assert MapSet.equal?(MapSet.new([:a, :b, :c, :d]), Ring.get_nodes_list(node_ring)) == true

    end)

    handle = Process.monitor(client)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> assert false
    end

  after
    Emulation.terminate()
  end

end
