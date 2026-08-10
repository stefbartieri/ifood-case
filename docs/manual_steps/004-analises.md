# Guia manual 004 — Executar as análises (EDA + P1 + P2)

> Análises. Importe os 3 notebooks de `analysis/`, execute em ordem
> (01 → 02 → 03) no serverless e rode os 2 `.sql` no SQL Editor.
> Pré-requisito: gold + views criadas.

## 1. Importar os 3 notebooks

Workspace → sua pasta → **Import** → **File**, três vezes, para:

```text
C:\Users\andre\OneDrive\Área de Trabalho\ifood\analysis\01_eda_nyc_taxi.py
C:\Users\andre\OneDrive\Área de Trabalho\ifood\analysis\02_resposta_p1_media_total_amount_mes.py
C:\Users\andre\OneDrive\Área de Trabalho\ifood\analysis\03_resposta_p2_media_passageiros_hora_maio.py
```

## 2. Executar o notebook 01 (EDA)

**Run all** no serverless. Validações:

- [ ] Tabela de volumetria (célula 1) com os status OK/DIVERGE por mês;
- [ ] As conclusões CONFIRMADA/REFUTADA das 6 hipóteses de anomalia (A–F);
- [ ] A saída do cruzamento com dq_metrics (reconciliação OK por escopo).

## 3. Executar o notebook 02 (P1)

**Run all**. Validações:

- [ ] A tabela de comparação (paridade SQL vs PySpark + benchmark) com os
      flags — esperado: paridade OK nos 5 meses, desvio ≤ ±0,5;
- [ ] A tabela markdown da célula final (5 linhas).

## 4. Executar o notebook 03 (P2)

**Run all**. Validações:

- [ ] A linha final da célula de paridade (esperado: "PARIDADE: OK nos 48
      valores");
- [ ] Os 4 picos impressos na seção de dupla leitura (ocupação vs demanda,
      frota e yellow);
- [ ] A tabela markdown da célula final (24 linhas).

## 5. Executar os 2 arquivos SQL no SQL Editor

Abra cada arquivo local, copie o conteúdo e execute no **SQL Editor**
(warehouse serverless):

```text
C:\Users\andre\OneDrive\Área de Trabalho\ifood\analysis\sql\pergunta1_media_total_amount_mes.sql
C:\Users\andre\OneDrive\Área de Trabalho\ifood\analysis\sql\pergunta2_media_passageiros_hora_maio.sql
```

- [ ] Saída da pergunta 1 com 5 linhas e da pergunta 2 com 48 linhas.

## 6. Confirmação final

- [ ] Os 3 notebooks executaram ponta a ponta sem erro.
