defmodule Ring do
  @moduledoc """
  Documentation for `Ring`.
  """
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  defstruct(
    ring: nil,
    nodes: nil,
    virtual_nodes: 128,  # Default Virutal Nodes of the server,
    vector_clock: nil,
    suspect_nodes: nil
  )

  @hash_range trunc(:math.pow(2, 32) - 1)

  @spec new :: %Ring{}
  def new() do
    # TODO
    # Initialize vector clock
    %Ring{ring: :gb_trees.empty , nodes: MapSet.new(), suspect_nodes: %{}}
  end

  @spec new(atom()) :: %Ring{}
  def new(node) do
    hash_ring = new()
    add_node(hash_ring, node)
  end

  @spec add_node(%Ring{}, atom()) :: %Ring{}
  defp add_node(ring, node_key) do

    cond do
      MapSet.member?(ring.nodes, node_key) ->
        ring
      :else ->
        ring = %{ring | nodes: MapSet.put(ring.nodes, node_key)}
        Enum.reduce(1..ring.virtual_nodes, ring, fn i, %Ring{ring: r} = acc ->
          hash_key = :erlang.phash2({node_key, i}, @hash_range)
          try do
            %{acc | ring: :gb_trees.insert(hash_key, node_key, r)}
          catch
            :error, {:key_exists, _} ->
              acc
          end
        end)
    end
  end

  @spec add_node(%Ring{}, [atom()]) :: %Ring{}
  def add_nodes(ring, nodes) do
    ring = Enum.reduce(nodes, ring, fn node, acc -> add_node(acc, node) end)
    ring
  end

  def get_node_count(%Ring{nodes: nodes}) do
    Enum.count(nodes)
  end

  def nodes_for_key(%Ring{ring: r} = ring, key, count) do
    hash = :erlang.phash2(key, @hash_range)
    count = min(get_node_count(ring), count)
    case :gb_trees.iterator_from(hash, r) do
      [{_key, node, _, _} | _] = iter ->
        get_nodes_from_iter(iter, count - 1, [node])
      _ ->
        {_key, node} = :gb_trees.smallest(r)
        [node]
    end
  end

  @spec remove_node(%Ring{}, any()) :: %Ring{}
  def remove_node(%Ring{nodes: nodes, ring: ring} = hash_ring, key) do

    if MapSet.member?(nodes, key) do
      tree2 = :gb_trees.to_list(ring)
      |> Enum.filter(fn {_key, ^key} -> false; _ -> true end)
      |> :gb_trees.from_orddict()
      %{hash_ring | nodes: MapSet.delete(nodes, key), ring: tree2}
    else
      hash_ring
    end
  end

  defp get_nodes_from_iter(iter, count, results) do
    if count == 0 do
      Enum.reverse(results)
    else
      case :gb_trees.next(iter) do
        {_key, node, iter} ->
          if node in results do
            get_nodes_from_iter(iter, count, results)
          else
            [node | results]
            get_nodes_from_iter(iter, count - 1, [node | results])
          end
        _ ->
          results
      end
    end
  end

  def increment_vector_clock(ring, node) do
    #TODO
    ring
  end
  def get_node_version(ring, node) do
    0
  end

  def get_node_incarnation(ring, node) do
    # First Check if contains node, if not send -1
    #TODO
    # Return node_incarnation for indirect probe
    get_node_version(ring, node)
  end

  def update_node_version(%Ring{vector_clock: version} = ring, node, incarnation) do
    #TODO
    # Call vector clock to update version for this node

  end

  def update_node_incarnation(ring, node, incarnation) do
    update_node_version(ring, node, incarnation)
  end

  """
  Check if node is a suspect node
  """
  def is_suspect_node(%Ring{suspect_nodes: suspect_nodes}, suspect_node) do
    Map.has_key?(suspect_nodes, suspect_node)
  end

  """
  Add node as the suspect node
  """
  def add_suspect_node(%Ring{suspect_nodes: suspect_nodes} = ring, suspect_node, incarnation, time) do
    cond do
      is_suspect_node(ring, suspect_node) && get_node_incarnation(ring, suspect_node) < incarnation ->
        ring = update_node_incarnation(ring, suspect_node, incarnation)
        %{ring | suspect_nodes: Map.put(suspect_nodes, suspect_node, time)}
      !is_suspect_node(ring, suspect_node) ->
        ring = update_node_incarnation(ring, suspect_node, incarnation)
        %{ring | suspect_nodes: Map.put_new(suspect_nodes, suspect_node, time)}
      true ->
        ring
    end
  end

  def handle_node_alive(%Ring{suspect_nodes: suspect_nodes} = ring, suspect_node) do
    if is_suspect_node(ring, suspect_node) do
      %{ring | suspect_nodes: Map.delete(suspect_nodes, suspect_node)}
    else
      ring
    end
  end

  @spec sync_rings(%Ring{}, %Ring{}) :: %Ring{}
  def sync_rings(%Ring{ring: ring1, nodes: nodes1, vector_clock: version1}, %Ring{ring: ring2, nodes: nodes2, vector_clock: version2}) do
    tree1 =:gb_trees.to_list(ring1)
    |> Enum.map(fn {key, value} -> {key, value} end)
    |> Enum.into(%{})


    tree2 =:gb_trees.to_list(ring2)
    |> Enum.map(fn {key, value} -> {key, value} end)
    |> Enum.into(%{})

    tree = Map.merge(tree1, tree2)
    |> Map.to_list()
    |> :gb_trees.from_orddict()

    # TODO
    # Sync Version Vector
    version = nil
    %Ring{ring: tree, nodes: MapSet.union(nodes1, nodes2), vector_clock: version}
  end
  @spec get_nodes_list(%Ring{}) :: MapSet
  def get_nodes_list(%Ring{nodes: nodes}) do
      nodes
  end

end
