"""
Este script gera gráficos a partir do ficheiro CSV 'consumption_times.csv'.
Ele cria dois tipos de visualizações:
1. Gráfico de Linhas: Mostra a evolução dos tempos de consumo ao longo das execuções para cada base de dados.
2. Gráfico de Barras: Mostra a média e o desvio padrão dos tempos de consumo, ordenado do menor para o maior.

Requisitos:
- pandas
- matplotlib

Uso:
    python build_graph_consumption.py

O script procura pelo ficheiro 'consumption_times.csv' no diretório atual.
Se não o encontrar, exibirá uma mensagem de erro.
"""


import pandas as pd
import matplotlib.pyplot as plt
import os

def build_graph_consumption():
    csv_file = 'consumption_times.csv'
    output_file = 'consumption_times_graph.png'

    if not os.path.exists(csv_file):
        print(f"Erro: {csv_file} não encontrado.")
        return

    try:
        # Ler o ficheiro CSV
        df = pd.read_csv(csv_file)

        # Definir a coluna 'Database' como índice
        if 'Database' in df.columns:
            df.set_index('Database', inplace=True)
        else:
            print("Erro: coluna não encontrada no CSV.")
            return

        # Transpor o DataFrame para que:
        # - O índice passa a ser as Execuções (Execution_1, Execution_2) -> Eixo X
        # - As colunas passam a ser as Bases de Dados -> Linhas
        df_transposed = df.T

        # Criar o gráfico de linhas
        # figsize: largura, altura em polegadas
        # marker='o' adiciona pontos aos dados para melhor visibilidade
        ax = df_transposed.plot(kind='line', figsize=(12, 8), marker='o')

        # Adicionar etiquetas e título
        plt.title('Tempos de Consumo na Base de Dados por Execução', fontsize=16)
        plt.xlabel('Execução', fontsize=12)
        plt.ylabel('Tempo (segundos)', fontsize=12)
        
        # Adicionar linhas de grelha
        plt.grid(True, linestyle=('--'), alpha=0.7)
        
        # Mover a legenda para fora se houver muitas bases de dados, ou manter o melhor ajuste
        plt.legend(title='Bases de Dados', bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Ajustar o layout para evitar cortes na legenda
        plt.tight_layout()

        # Guardar o gráfico
        plt.savefig(output_file)
        print(f"Gráfico guardado com sucesso em {output_file}")

        # --- Novo gráfico com Média e Desvio Padrão ---
        stats_output_file = 'consumption_stats_graph.png'
        
        # Calcular Média e Desvio Padrão (ignorando a coluna que era índice e foi movida, mas aqui o índice é Database)
        # O DataFrame original 'df' tem Database como índice e colunas como execuções (valores numéricos)
        df['Mean'] = df.mean(axis=1)
        df['StdDev'] = df.std(axis=1)
        
        # Ordenar pela média (do menor para o maior)
        df_sorted = df.sort_values(by='Mean')
        
        # Criar o gráfico de barras
        plt.figure(figsize=(12, 8))
        # Criar gráfico de barras com barras de erro (yerr)
        # Capsize adiciona os "tampinhos" nas barras de erro
        bars = plt.bar(df_sorted.index, df_sorted['Mean'], yerr=df_sorted['StdDev'], capsize=5, color='lightgreen', alpha=0.9, edgecolor='black')
        
        # Adicionar etiquetas e título
        plt.title('Média e Desvio Padrão dos Tempos de Consumo (Menor para Maior)', fontsize=16)
        plt.xlabel('Base de Dados', fontsize=12)
        plt.ylabel('Tempo Médio (segundos)', fontsize=12)
        
        # Rodar os labels do eixo X para melhor leitura se forem muitos
        plt.xticks(rotation=45, ha='right')
        
        # Adicionar linhas de grelha (apenas horizontal para bar chart faz mais sentido geralmente, mas grid geral ok)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        plt.tight_layout()
        
        # Guardar o novo gráfico
        plt.savefig(stats_output_file)
        print(f"Gráfico de estatísticas guardado com sucesso em {stats_output_file}")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    build_graph_consumption()
