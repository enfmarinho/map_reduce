defmodule MapReduce do
  def number_of_native_threads do
    System.schedulers()
    # System.schedulers_online()
  end

  # DIVIDIR DATASET

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
    multi = IO.gets("Digite o multiplicador: ")
     |> String.replace("\n", "")   # Remove o '\n'
     |> String.to_integer
    num = number_of_native_threads() * multi
    formar_listas(list, num)
  end

  def dividir_dataset(file_path)  do
    file_path
    |> File.read()
    |> elem(1)
    |> String.replace([",", ";", ".", ":", "(", ")"], "") # Tira pontuação
    |> String.split(" ")
  end

  defp master() do
    url_caminho = IO.gets("Digite a url....: ")
    # Lê os dados do arquivo e retorna uma lista de listas
    dividir_dataset(url_caminho)
    |> split_lista

    # JOGAR NOS MAPS

    # ENviar para os escravos

    # TERMINOU?

    # Receber da fila de mensagens dos escravos

    # SHUFFLE
    # Ordenar pela chaves

    # TERMINOU O SHUFFLE?
    # REDUCE
    # Manda os escravos realizarem os reduces
  end

  defp recebe_map() do
    # Map here
    # EX: doubled_list = Enum.map(list, fn x -> x * 2 end)
  end

  defp recebe_reduce() do
    # Reduce here
    # EX: sum = Enum.reduce(list, fn x, acc -> x + acc end)
  end

  defp recebe_threads_map() do
    # recebe threads da map
  end

  defp shuffle(list) when is_list(list) do
  end
  # == Essa seria a função pública principal
  # Chamada para o caso geral (com lista)
  def main(list, map_func, reduce_func) when is_list(list) do 
    list |> split_lista
  end
  # Chamada recebendo o caminho de um arquivo (para o caso de fazer a contagem de palavras)
  def main(file_path \\ "test.txt", map_func, reduce_func) do
    dividir_dataset(file_path)
    |> split_lista

  end

end
