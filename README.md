# Busca de Empresas

Encontra telefone e email de empresas ativas a partir dos dados públicos da Receita Federal.

- **Por nicho:** escolha a atividade (ex.: dentista), o estado e, se quiser, a cidade. A lista sai
  com telefone, WhatsApp e email, e pode ser exportada para planilha.
- **Por CNPJ:** consulta online (CNPJá → BrasilAPI → Minha Receita) com os dados mais atuais.

Custo zero: a página fica no **GitHub Pages**, o índice no **Hugging Face** e a atualização mensal
roda no **GitHub Actions**. Nada roda no seu computador.

## Como funciona

```
Receita Federal (dados abertos, mensal)
        │  GitHub Actions — 2x por semana confere se há mês novo
        ▼
pipeline/atualizar_base.py
  • guarda só empresas ATIVAS com telefone ou email
  • gera um arquivo por estado + atividade (CNAE)
        │
        ▼
Hugging Face (dataset público)  ◄── a página lê daqui
        ▲
GitHub Pages — index.html
```

## Configuração (uma vez só)

1. **Hugging Face**
   1. Crie uma conta em https://huggingface.co.
   2. Em *Settings → Access Tokens*, crie um token do tipo **Write**.
   3. Escolha o nome do conjunto de dados, por exemplo `seu-usuario/base-cnpj`
      (ele é criado automaticamente na primeira execução).
2. **GitHub**
   1. Crie um repositório **público** e envie estes arquivos.
   2. *Settings → Secrets and variables → Actions*:
      - aba **Secrets**: `HF_TOKEN` = o token do Hugging Face;
      - aba **Variables**: `HF_REPO` = `seu-usuario/base-cnpj`.
   3. *Settings → Pages → Source*: **GitHub Actions**.
3. **Primeira carga**
   1. *Actions → Atualizar base de empresas → Run workflow* (leva cerca de 1 hora).
   2. *Actions → Publicar página → Run workflow*.
   3. Abra `https://seu-usuario.github.io/nome-do-repositorio/`.

Depois disso, a base se atualiza sozinha quando a Receita publica um mês novo.

## Teste local (sem publicar nada)

```bash
pip install duckdb
python pipeline/atualizar_base.py --arquivos-locais pasta_com_zips --sem-upload --mes 2026-10
# na pasta "saida", crie atual.json com {"mes":"2026-10","pasta":"2026-10"} e copie o index.html
cd saida && python -m http.server 8765
# abra http://localhost:8765/index.html?dados=http://localhost:8765
```

## Limitações

- A lista usa a **atividade principal** da empresa (atividades secundárias ficam de fora).
- Telefone e email são os declarados à Receita e podem estar desatualizados. Para conferir, clique
  no CNPJ: a página consulta os dados mais recentes.
- A consulta por CNPJ gratuita (CNPJá) aceita cerca de 5 consultas por minuto.
