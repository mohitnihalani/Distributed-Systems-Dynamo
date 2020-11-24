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
    N: 3, # (N,R,W) for quorum
    R: 2,
    W: 1,
  )


  def init() do
    hash_ring = Ring.new(whoami())
    %DynamoNode{state: hash_ring, pid: whoami()}
  end

  """
  With Seed Node
  """
  def init(seed_node) do
    hash_ring = Ring.new(whoami())
    node = %DynamoNode{state: hash_ring, pid: whoami(), seed_node: seed_node}

  end

  @spec get_gossip_timeout(%DynamoNode{}) :: non_neg_integer()
  defp get_gossip_timeout(%DynamoNode{heartbeat_timeout: heartbeat_timeout}=state) do
    :rand.uniform(heartbeat_timeout) + 5000
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
    send(sender, {:share_state_reply, updated_ring})
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
  defp run_gossip_protocol(node, extra_state) do
    :rand.seed(:exrop, {101, 102, 103})
    random_node = Enum.random(node.state.nodes)
    send(random_node, {:share_state, node.state})
    node = reset_gossip_timeout(node)

    receive do
      {_sender, {:share_state_reply, %Ring{} = joined_state}} ->
        node = %{node | state: join_states(node.state, joined_state)}
        run_node(node, extra_state)
      :timer ->
        run_node(node, extra_state)
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
        run_gossip_protocol(node, extra_state)

      # Share State Request For Gossip Protocol
      {sender, %DynamoNode.ShareStateRequest{state: otherstate}} ->
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

      # ---------------  Handle Client Request -----------------------------#

      # Put Request for the client
      {sender, {:put, {key, value, context}}} ->
        #TODO Put given key, with this value and context
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


defmodule DynamNode.Client do
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

  @spec check_node_status(%Client{}, atom()) :: :fail | :ok
  def check_node_status(client, node) do
    send(node, :check)
    receive do
      {^node, :ok} -> :ok
    after
      1000 -> :fail
    end
  end
end
