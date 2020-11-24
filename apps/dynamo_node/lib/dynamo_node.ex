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
    heartbeat_timer: nil,
    seed_node: nil,
    heartbeat_timeout: 1000,
    N: 3, # (N,R,W) for quorum
    R: 2,
    W: 1,
  )


  def init() do
    hash_ring = Ring.new()
    node = %DynamoNode{state: hash_ring, pid: whoami()}
  end

  """
  With Seed Node
  """
  def init(seed_node) do
    hash_ring = Ring.new()
    node = %DynamoNode{state: hash_ring, pid: whoami(), seed_node: seed_node}
  end

  @spec get_gossip_timeout(%DynamoNode{}) :: non_neg_integer()
  defp get_gossip_timeout(%DynamoNode{heartbeat_timeout: heartbeat_timeout}=state) do
    :rand.uniform(heartbeat_timeout) + 5000
  end

  defp save_heartbeat_timer(node, timer) do
    %{node | heartbeat_timer: timer}
  end


  defp reset_gossip_timeout(node) do
    if node.heartbeat_timer, do: Emulation.cancel_timer(node.heartbeat_timer)
    gossip_timeout = get_gossip_timeout(node)
    save_heartbeat_timer(node, Emulation.timer(node.heartbeat_timeout))
  end

  #@spec handle_share_state_request(%Ring{}, {any(), any()}) :: %Ring{}
  defp handle_share_state_request(state, {sender, otherstate}) do
    # Find Better Version
    # Vector clock is inside %Ring{ring: ring, version: vectorclock()}
    # TODO Compare vector clocks to find most recent state
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
  def run_gossip_protocol(node) do
    :rand.seed(:exrop, {101, 102, 103})
    random_node = Enum.random(node.state.nodes)
    send(random_node, {:share_state, node.state})
    node = reset_gossip_timeout(node)

    receive do
      {_sender, {:share_state_reply, %Ring{} = joined_state}} ->
        node = %{node | state: join_states(node.state, joined_state)}
        run_node(node)
      :timer ->
        run_node(node)
    end
  end

  @spec lauch_node(%DynamoNode{}) :: no_return()
  def lauch_node(node) do
    if node.seed_node != nil do

    end
  end

  """
  This is where most recursion happend maintaing state
  """
  defp run_node(node) do
    receive do
      :timer ->
        run_gossip_protocol(node)
      {sender, {:share_state, otherstate}} ->
        node = %{node | state: handle_share_state_request(node.state, {sender, otherstate})}
        run_node(node)
        # code
      {sender, {:put, {key, value, context}}} ->
        #TODO Put given key, with this value and context
        run_node(node)
      {sender, {:share_state, otherstate}} ->
        #TODO Get given key, with this value and context
        run_node(node)
    end

  end
end
