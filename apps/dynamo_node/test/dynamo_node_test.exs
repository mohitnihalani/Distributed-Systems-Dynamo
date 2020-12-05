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
    spawn(:b, fn -> DynamoNode.lauch_node(DynamoNode.init(:a)) end)


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

      assert DynamoNode.Client.put_request(client, "foo", 200, nil, :b) == {:ok, client,_}}
      assert DynamoNode.Client.put_request(client, "mah", 200, nil, :c) == {:ok, client, _}
      assert DynamoNode.Client.put_request(client, "lsk", 200, nil, :c) == {:ok, client, _}
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

  test "Test Replicate with increase w request" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    n = 4
    r = 2
    w = 2
    spawn(:a, fn -> DynamoNode.lauch_node(DynamoNode.init(n, r, w)) end)
    spawn(:b, fn -> DynamoNode.lauch_node(DynamoNode.init(:a, n, r, w)) end)
    spawn(:c, fn -> DynamoNode.lauch_node(DynamoNode.init(:b, n, r, w)) end)
    spawn(:d, fn -> DynamoNode.lauch_node(DynamoNode.init(:c, n, r, w)) end)
    spawn(:e, fn -> DynamoNode.lauch_node(DynamoNode.init(:b, n, r, w)) end)
    spawn(:f, fn -> DynamoNode.lauch_node(DynamoNode.init(:c, n, r, w)) end)

    caller = self()
    client = spawn(:c1, fn ->
      client = DynamoNode.Client.new_client(:c1)
      assert DynamoNode.Client.check_node_status(client, :a) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :b) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :c) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :d) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :e) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :f) == {:ok, client}

       # Give things a bit of time to settle down.
       receive do
       after
         7_000 -> :ok
       end

      assert DynamoNode.Client.put_request(client, "foo", 200, nil, :b) == {:ok, client, _}
      assert DynamoNode.Client.put_request(client, "mah", 200, nil, :c) == {:ok, client, _}
      assert DynamoNode.Client.put_request(client, "lsk", 200, nil, :c) == {:ok, client, _}
      assert DynamoNode.Client.put_request(client, "hellowoorld", 200, nil, :c) == {:ok, client, _}
      assert DynamoNode.Client.put_request(client, "distributed", 200, nil, :c) == {:ok, client, _}


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


  test "Test Simple Get Request" do
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

      assert DynamoNode.Client.put_request(client, "foo", 200, nil, :b) == {:ok, client}

      {:ok, {_, [200 | _tail ], [vhead | _vtail]}} = DynamoNode.Client.get_request(client, "foo", :c)

      assert DynamoNode.Client.put_request(client, "foo", 10, vhead, :b) == {:ok, client}
      {:ok, {_, [value | _ ], [vhead | vtail]}} = DynamoNode.Client.get_request(client, "foo", :c)

      assert value == 10
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

  test "Test Get with increase R request" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    n = 5
    r = 3
    w = 2
    spawn(:a, fn -> DynamoNode.lauch_node(DynamoNode.init(n, r, w)) end)
    spawn(:b, fn -> DynamoNode.lauch_node(DynamoNode.init(:a, n, r, w)) end)
    spawn(:c, fn -> DynamoNode.lauch_node(DynamoNode.init(:b, n, r, w)) end)
    spawn(:d, fn -> DynamoNode.lauch_node(DynamoNode.init(:c, n, r, w)) end)
    spawn(:e, fn -> DynamoNode.lauch_node(DynamoNode.init(:b, n, r, w)) end)
    spawn(:f, fn -> DynamoNode.lauch_node(DynamoNode.init(:c, n, r, w)) end)

    caller = self()
    client = spawn(:c1, fn ->
      client = DynamoNode.Client.new_client(:c1)
      assert DynamoNode.Client.check_node_status(client, :a) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :b) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :c) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :d) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :e) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :f) == {:ok, client}

       # Give things a bit of time to settle down.
       receive do
       after
         10_000 -> :ok
       end

      assert DynamoNode.Client.put_request(client, "foo", 200, nil, :b) == {:ok, client}
      assert DynamoNode.Client.put_request(client, "mah", 200, nil, :c) == {:ok, client}
      assert DynamoNode.Client.put_request(client, "lsk", 100, nil, :c) == {:ok, client}
      assert DynamoNode.Client.put_request(client, "hellowoorld", 200, nil, :c) == {:ok, client}
      assert DynamoNode.Client.put_request(client, "distributed", 200, nil, :c) == {:ok, client}

      #####
      {:ok, {_, [200 | _tail ], [vhead | _vtail]}} = DynamoNode.Client.get_request(client, "foo", :c)
      assert DynamoNode.Client.put_request(client, "foo", 10, vhead, :b) == {:ok, client}
      {:ok, {_, [value | _ ], [vhead | vtail]}} = DynamoNode.Client.get_request(client, "foo", :c)
      assert value = 10

      #####
      {:ok, {_, [200 | _tail ], [vhead | _vtail]}} = DynamoNode.Client.get_request(client, "distributed", :e)
      assert DynamoNode.Client.put_request(client, "distributed", 93, vhead, :b) == {:ok, client}
      {:ok, {_, [value | _ ], [vhead | vtail]}} = DynamoNode.Client.get_request(client, "distributed", :f)
      assert value = 93

      ####
      {:ok, {_, [10 | _tail ], [vhead | _vtail]}} = DynamoNode.Client.get_request(client, "foo", :a)
      assert DynamoNode.Client.put_request(client, "foo", 90, vhead, :e) == {:ok, client}
      {:ok, {_, [value | _ ], [vhead | vtail]}} = DynamoNode.Client.get_request(client, "foo", :d)
      assert value = 90
      ######
      {:ok, {_, [100 | _tail ], [vhead | _vtail]}} = DynamoNode.Client.get_request(client, "lsk", :f)
      assert DynamoNode.Client.put_request(client, "lsk", 18, vhead, :f) == {:ok, client}
      {:ok, {_, [value | _ ], [vhead | vtail]}} = DynamoNode.Client.get_request(client, "lsk", :a)
      assert value == 18
    end)

    handle = Process.monitor(client)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      50_000 -> assert false
    end

  after
    Emulation.terminate()
  end

  test "Test Failure Detection" do
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

       {:ok, ^client} = DynamoNode.Client.fail_node(client, :d)

       receive do
       after
         10_000 -> :ok
       end

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :a)
       assert Ring.get_node_count(node_ring) == 3

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :b)
       assert Ring.get_node_count(node_ring) == 3

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :c)
       assert Ring.get_node_count(node_ring) == 3
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

  test "Test Vector Clock" do
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
       IO.inspect(Ring.get_vector_clock(node_ring))

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :b)
       IO.inspect(Ring.get_vector_clock(node_ring))

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :c)
       IO.inspect(Ring.get_vector_clock(node_ring))

       {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :d)
       IO.inspect(Ring.get_vector_clock(node_ring))
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

  test "Test Failure Detection With More Nodes" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    n = 5
    r = 3
    w = 2
    spawn(:a, fn -> DynamoNode.lauch_node(DynamoNode.init(n, r, w)) end)
    spawn(:b, fn -> DynamoNode.lauch_node(DynamoNode.init(:a, n, r, w)) end)
    spawn(:c, fn -> DynamoNode.lauch_node(DynamoNode.init(:b, n, r, w)) end)
    spawn(:d, fn -> DynamoNode.lauch_node(DynamoNode.init(:c, n, r, w)) end)
    spawn(:e, fn -> DynamoNode.lauch_node(DynamoNode.init(:b, n, r, w)) end)
    spawn(:f, fn -> DynamoNode.lauch_node(DynamoNode.init(:c, n, r, w)) end)

    caller = self()
    client = spawn(:c1, fn ->
      client = DynamoNode.Client.new_client(:c1)
      assert DynamoNode.Client.check_node_status(client, :a) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :b) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :c) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :d) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :e) == {:ok, client}
      assert DynamoNode.Client.check_node_status(client, :f) == {:ok, client}

       # Give things a bit of time to settle down.
       receive do
       after
         10_000 -> :ok
       end

       {:ok, ^client} = DynamoNode.Client.fail_node(client, :d)

      receive do
      after
        15_000 -> :ok
      end

      {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :a)
      assert Ring.get_node_count(node_ring) == 5

      {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :b)
      assert Ring.get_node_count(node_ring) == 5

      {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :c)
      assert Ring.get_node_count(node_ring) == 5

      {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :e)
      assert Ring.get_node_count(node_ring) == 5

      {node_ring, ^client} = DynamoNode.Client.client_get_state(client, :f)
      assert Ring.get_node_count(node_ring) == 5

    end)

    handle = Process.monitor(client)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      50_000 -> assert false
    end

  after
    Emulation.terminate()
  end


end
