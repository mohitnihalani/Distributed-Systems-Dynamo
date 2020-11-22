defmodule Replication do
  @moduledoc """
  Documentation for `Replication`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> Replication.hello()
      :world

  """
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger
  defstruct(
    hash_ring: nil,
    nodes: %{}
  )

  def init() do
    replication_mananger = %Replication{hash_ring: Ring.new()}
    start_replication_manager(replication_mananger)
  end

  def start_replication_manager(replication_mananger) do
    receive do
      {sender, :ok} ->
        send(sender, :ok)
        start_replication_manager(replication_mananger)
    end
  end

  def hello do
    :world
  end
end
