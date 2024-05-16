defmodule MapReduce do
  def number_of_native_threads do
    System.schedulers()
    # System.schedulers_online()
  end

  #DIVIDIR DATASET

  defp recebe_map() do
    #Map here 
    # EX: doubled_list = Enum.map(list, fn x -> x * 2 end)

    end

  defp recebe_reduce() do
    #Reduce here
    #EX: sum = Enum.reduce(list, fn x, acc -> x + acc end)
    end

  defp recebe_threads_map() do

    #recebe threads da map
    end

  defp shuffle(list) when is_list(list) do
    #ordenar com base na chave

    end



end
