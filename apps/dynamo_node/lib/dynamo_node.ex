defmodule DynamoNode do
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  require Ring
  """
  Dynamo Node
  Each node has it's view of the state, which basically means each node has its own ring
  Node can be initiated using the seed node or without the seed node
  Seed node is used so that node can connect to the ring when joined initially
  If seed node is provided in the confguration, it will try to connect to the seed node using gossip protocol
  and join the states.

  - State -> hash_ring
  - pid
  - seed_node
  """
  # TODO -> Moving version inside the ring so we can have one state which consists on the ring and vector clock for the node
  # It would be easier to merge states, as we can only pass the ring when calling :share_state

  # Token Calls
  # :share_state -> for share state request
  # :get
  defstruct(
    state: nil,
    pid: nil,
    kv: %DynamoNode.KV{},
    heartbeat_timer: nil,
    heartbeat_timeout: 1000,
    seed_node: nil,
    n: 3, # (N,R,W) for quorum
    r: 2,
    w: 1,
    version: 0
  )


  def init() do
    hash_ring = Ring.new(whoami())
    %DynamoNode{state: hash_ring, pid: whoami()}
  end

  """
  With Seed Node
  """
  def init(seed_node) do
    hash_ring = Ring.new()
    hash_ring = Ring.add_nodes(hash_ring, [whoami(), seed_node])
    node = %DynamoNode{state: hash_ring, pid: whoami(), seed_node: seed_node}
  end

  @spec get_gossip_timeout(%DynamoNode{}) :: non_neg_integer()
  defp get_gossip_timeout(%DynamoNode{heartbeat_timeout: heartbeat_timeout}=state) do
    :rand.uniform(heartbeat_timeout) + 500
  end

  defp save_heartbeat_timer(node, timer) do
    %{node | heartbeat_timer: timer}
  end


  @spec reset_gossip_timeout(%DynamoNode{}) :: %DynamoNode{}
  defp reset_gossip_timeout(%DynamoNode{heartbeat_timer: heartbeat_timer} = node) do
    if heartbeat_timer, do: Emulation.cancel_timer(heartbeat_timer)
    gossip_timeout = get_gossip_timeout(node)
    save_heartbeat_timer(node, Emulation.timer(node.heartbeat_timeout))
  end

  @spec handle_share_state_request(%Ring{}, {atom(), %Ring{}}) :: %Ring{}
  defp handle_share_state_request(state, {sender, otherstate}) do
    # Find Better Version
    # Vector clock is inside %Ring{ring: ring, version: vectorclock()}
    # TODO
    # Compare vector clocks to find most recent state
    # After that call join_ring(state, otherstate) with otherstate as most recent state
    # Make sure you update state at the call with the new ring
    updated_ring = join_states(state, otherstate)
    send(sender, DynamoNode.ShareStateResponse.new(updated_ring))
    updated_ring
  end

  @spec join_states(%Ring{}, %Ring{}) :: %Ring{}
  def join_states(state, otherstate) do
    # Synch both hash rings
    Ring.sync_rings(state, otherstate)
  end

  """
  Method to run gossip protocol
  """
  def run_gossip_protocol(node, extra_state) do
    :rand.seed(:exrop, {101, 102, 103})
    random_node = Enum.random(node.state.nodes)
    send(random_node,DynamoNode.ShareStateResponse.new(node.state))
    timeout = Emulation.timer(200)
    IO.puts("#{whoami()} Initiated Gossip Protocl Request to #{node}")

    receive do


      {^node, %DynamoNode.ShareStateRequest{
        state: state
      }} ->
        IO.puts("#{whoami()} Received Share State Response from #{node}")
        #node = %{node | state: join_states(node.state, otherstate)}
        Emulation.cancel_timer(timeout)
        node = reset_gossip_timeout(node)
        run_node(node, extra_state)

      :timer ->
        IO.puts("#{whoami()} Gossip Protocol Request Timeout #{node}")
        run_node(node, extra_state)
        node = reset_gossip_timeout(node)
    end
  end

  @spec get_preference_list(%Ring{}, any(), non_neg_integer()) :: [atom()]
  def get_preference_list(ring, key, n) do
    Ring.nodes_for_key(ring, key, n)
  end

  def send_requests(node_list, message) do
    Enum.map(node_list, fn pid -> send(pid, message) end)
  end


  def handle_put_request(node, extra_state, client, key, value, context) do
    preference_list = get_preference_list(node.state, key, node)
    pid = whoami()
    case preference_list do
      [^pid | tail]->
        # If Coordinator Node, store and send to other nodes
        node = %{node | kv: DynamoNode.KV.put(node.kv, node.version, key, context, value)}
        send_requests(tail, DynamoNode.PutEntryRequest.new(key, context, value))
        {node, extra_state}
      [head | tail]->
        # If not, send the the preferred node for the given key
        send(head, {:redirect_put, {client, key, value, context}})
        {node, extra_state}
      _ ->
        # I don't know about this
        {node, extra_state}
    end
  end


  @spec lauch_node(%DynamoNode{}) :: no_return()
  def lauch_node(node) do
    node = reset_gossip_timeout(node)
    IO.puts(Ring.get_node_count(node.state))
    run_node(node, %{})
  end

  """
  This is where most recursion happend maintaing state
  """
  defp run_node(node, extra_state) do
    IO.puts("Running Node  #{whoami()}")
    receive do

      :timer ->
        if Ring.get_node_count(node.state) >= 2 do
          run_gossip_protocol(node, extra_state)
        else
          run_node(node, extra_state)
        end

      # Share State Request For Gossip Protocol
      {sender, %DynamoNode.ShareStateRequest{state: otherstate}} ->
        IO.puts("#{whoami()} Rreceived Share State request from #{sender}")

        node = %{node | state: handle_share_state_request(node.state, {sender, otherstate})}
        run_node(node, extra_state)
        # code

      {sender, %DynamoNode.GetEntryRequest{}} ->
        # Handle  GetEntry for replication
        # TODO
        run_node(node, extra_state)

      {sender, %DynamoNode.GetEntryResponse{}} ->
        # Handle  GetEntry Response
        # Merge the context and send response to the client
        # TODO
        run_node(node, extra_state)

      {sender, %DynamoNode.PutEntryRequest{}} ->
        # Handle Put Entry for replication
        #TODO
        run_node(node, extra_state)

      {sender, %DynamoNode.PutEntryResponse{}} ->
        # Handle Put Entry for response for the replication
        #TODO
        run_node(node, extra_state)

      # ---------------  Handle Redirect Client Request -----------------------------#

      # Put Request for the client
      {sender, {:redirect_put, {client, key, value, context}}} ->
        # TODO Put given key, with this value and context
        # First Check If you are the preferred coordinator
        run_node(node, extra_state)

      # Get Request for the client
      {sender, {:redirect_get, {client, key}}} ->
        #TODO Handle Client Request
        run_node(node, extra_state)

      # ---------------  Handle Client Request -----------------------------#

      # Put Request for the client
      {sender, {:put, {key, value, context}}} ->
        # TODO Put given key, with this value and context
        # First Check If you are the preferred coordinator
        run_node(node, extra_state)

      # Get Request for the client
      {sender, {:get, key}} ->
        #TODO Handle Client Request
        run_node(node, extra_state)

      # --------------- Testing ---------------------------------------- #
      {sender, :check} ->
        #IO.puts(sender)
        #send(sender, :ok)
        run_node(node, extra_state)

      {sender, :get_state} ->
        send(sender, node.state)
        run_node(node, extra_state)
    end

  end
end


defmodule DynamoNode.Client do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  defstruct(
    client_id: nil
  )

  @spec new_client(atom()) :: %Client{}
  def new_client(client_id) do
    %Client{client_id: client_id}
  end

  @spec check_node_status(%Client{}, atom()) :: {:fail,%Client{}} | {:ok,%Client{}}
  def check_node_status(client, node) do
    send(node, :check)
    receive do
      {^node, :ok} -> {:ok,client}
    after
      1000 -> {:fail,client}
    end
  end

  def client_get_state(client, node) do
    send(node, :get_state)
    receive do
      {^node, state} -> {state,client}
    after
      1000 -> {:fail,client}
    end
  end

  @spec put_request(%Client{}, any(), any(), any(), atom()) :: {:ok, %Client{}}
  def put_request(client, key, value, context, node) do
    send(node, {:put, {key, value, context}})
  end

  def get(client, key, node) do

  end
end
