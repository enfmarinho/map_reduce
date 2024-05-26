defmodule MapReduce do
  def number_of_native_threads do
    System.schedulers()
    # System.schedulers_online()
  end
 
  def receber_msgs(msgs \\ [], num_msgs) when num_msgs > 0 do
    receive do
       msg -> receber_msgs([msg | msgs], num_msgs - 1)
    end
  end
  def receber_msgs(msgs, num_msgs)  do
    msgs
  end

  def concatena([], curr) do
     curr
  end 
  def concatena([head | tail], curr \\ []) do
    concatena(tail, head ++ curr)
  end

  # Divide a lista de termos em uma lista de listas
  def formar_listas(list, num) when num >= length(list) do
    [list]
  end
  def formar_listas(list, num) do
    nova_list =
      list
      |> Enum.split(num)
    [nova_list |> elem(0)] ++ (nova_list |> elem(1) |> formar_listas(num))
  end

  defp split_lista(list) do
    # multi = IO.gets("Digite o multiplicador: ")
    #  |> String.replace("\n", "")   # Remove o '\n'
    #  |> String.to_integer
    num = div(length(list),number_of_native_threads())
    formar_listas(list, num)
  end

  # Essa funcao é usada para o caso de contagem de palavras
  def dividir_dataset(file_path)  do
    file_path
    |> File.read()
    |> elem(1)
    |> String.replace([",", ";", ".", ":", "(", ")"], "") # Tira pontuação
    |> String.split(" ")
  end

  defp master(list, fun_map, fun_reduce,acc) do
    aux = length(list)  # TODO substituir
    map_manager(list, fun_map)
    receber_msgs(length(list))
    |> concatena()
    |> shuffle_sort(:id)
    |> formar_listas(4)  #TODO Mudar o numero de listas dps
    # |> reduce_manager(fun_reduce, acc)
    # receber_msgs(aux)

    # # TODO terminar a parte do map
    # reduce_manager(particoes, fun_reduce, acc)
  end

  def recebe_map([], fun) do
    []
  end
  def recebe_map([h|t], fun) do
    [fun.(h)] ++ recebe_map(t, fun)
  end

  
  # Esaa é a chamada equivalnete a primeira chamada do reduce, em que teremos que calcular
  # o primeiro temro da lista com o 'acc'
  def recebe_reduce([h|t], fun, acc, bit \\ true) when bit == true do
    # Separa os dois primeiros elementos da lista
    fun.(acc, h) |> fun.(recebe_reduce(t, fun, acc,false))
  end
  
  def recebe_reduce(list, fun, acc, bit) when length(list) >= 2 do
    # Separa os dois primeiros elementos da lista
    list1 = list |> Enum.split(2) |> elem(0)
    list2 = list |> Enum.split(2) |> elem(1)
    fun.(Enum.at(list, 0), Enum.at(list, 1)) |> fun.(recebe_reduce(list2, fun, acc, false))
  end

  def recebe_reduce(list, fun, acc, _) when length(list) == 1 do
    fun.(hd(list),acc)
  end
  def recebe_reduce([], fun, acc, _) do
    %{}
  end


  def shuffle_sort(maps , keys) do
    maps 
    |> Enum.shuffle()
    |> Enum.sort_by(&Map.get(&1, keys))
  end

  def dividir_dataset([], _, listaLista, lista), do: listaLista ++ lista
  def dividir_dataset(data, anterior, listaLista, lista) do
    {first, second} = Enum.split(data, 1)
    if anterior == first do
      dividir_dataset(second, first, listaLista, lista ++ first)
    else
      dividir_dataset(second, first, listaLista ++ lista, [first])
    end
  end

  # 'pid_list' é uma lista contendo os PIDs das threads utlizadas (será necesssario para receber as mensagens)
  def map_manager([], _) do
    :ok
  end
  def map_manager([h|t], fun_map) do
    # Adiciona o novo PID à lista  
    spawn(MapReduce, :concurrent_map, [h, fun_map, self])
    map_manager(t, fun_map)
  end

  def concurrent_map(list ,fun_map, pid) do
    send pid, recebe_map(list, fun_map)
  end

  def reduce_manager([], _, _) do
    :ok
  end
  def reduce_manager([h|t], fun_red, acc) do
    spawn(MapReduce, :concurrent_reduce, [h, fun_red, acc, self])
    reduce_manager(t, fun_red, acc)
  end
  def concurrent_reduce(list, fun_red, acc,pid) do
    send pid, recebe_reduce(list,fun_red, acc)
  end

  def map_words(word) do
    %{id: word, value: 1}
  end

  def reduce_words(word_map1, word_map2) do
    %{
        id: word_map1[:id],
        content: word_map1[:content] + word_map2[:content]
      }
  end
  def reduce_words(%{}, word_map2) do
    word_map2
  end
  def reduce_words(word_map1 ,%{} ) do
    word_map1
  end
 


  # Chamada para o caso geral (com lista)
  def main(list, map_func, reduce_func, acc) when is_list(list) do
    list
    |> split_lista()
    # |> master(map_func, reduce_func, acc)
  # |> saida()
  end
  # Chamada recebendo o caminho de um arquivo (para o caso de fazer a contagem de palavras)
  def main(file_path \\ "test.txt", map_func \\ &map_words/1, reduce_func \\ &reduce_words/2, acc \\ %{}) do
    dividir_dataset(file_path)
    |> main(map_func, reduce_func, acc)
  end



end
