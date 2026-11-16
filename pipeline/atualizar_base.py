#!/usr/bin/env python3
"""
Monta o índice de busca por nicho a partir dos dados abertos de CNPJ da Receita Federal
e publica no Hugging Face.

Fluxo:
  1. Descobre o mês mais recente publicado pela Receita.
  2. Se esse mês já está publicado no Hugging Face, encerra (a não ser com --forcar).
  3. Baixa os arquivos um por um (baixa -> processa -> apaga), guardando só
     estabelecimentos ATIVOS que tenham telefone ou email.
  4. Junta com a razão social (arquivos de Empresas) usando DuckDB.
  5. Grava um arquivo .csv.gz por estado + atividade (CNAE), mais os índices
     auxiliares (manifest.json, cnaes.json, municipios.json).
  6. Envia para o Hugging Face numa pasta do mês, troca o ponteiro atual.json
     e apaga os meses antigos.

Uso típico (GitHub Actions):   python pipeline/atualizar_base.py
Teste local sem enviar:         python pipeline/atualizar_base.py --arquivos-locais DIR --sem-upload
"""
import argparse
import base64
import csv
import gzip
import io
import json
import os
import re
import shutil
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

# Pasta pública de dados abertos do CNPJ (Receita Federal / SERPRO, via WebDAV).
SHARE_TOKEN = 'YggdBLfdninEJX9'
WEBDAV = 'https://arquivos.receitafederal.gov.br/public.php/webdav/'
AUTH = 'Basic ' + base64.b64encode(f'{SHARE_TOKEN}:'.encode()).decode()

UFS = set('AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO'.split())
COLUNAS_SAIDA = ['cnpj', 'razao', 'fantasia', 'municipio', 'bairro', 'endereco', 'cep',
                 'tel1', 'tel2', 'email', 'abertura', 'matriz', 'porte']

csv.field_size_limit(10 ** 7)


def log(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)


# ---------------------------------------------------------------- Receita (download)

def propfind(caminho=''):
    req = urllib.request.Request(WEBDAV + caminho, method='PROPFIND',
                                 headers={'Depth': '1', 'Authorization': AUTH})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read().decode('utf-8', 'replace')


def ultimo_mes():
    meses = sorted(set(re.findall(r'/webdav/(\d{4}-\d{2})/', propfind())))
    if not meses:
        raise SystemExit('Nenhum mês encontrado no repositório da Receita.')
    return meses[-1]


def arquivos_do_mes(mes):
    return set(re.findall(r'/webdav/\d{4}-\d{2}/([A-Za-z0-9_]+\.zip)', propfind(mes + '/')))


def baixar(mes, nome, destino: Path, tentativas=8):
    """Baixa com retomada (Range) — o servidor da Receita costuma ser instável."""
    url = f'{WEBDAV}{mes}/{nome}'
    parcial = destino.with_name(destino.name + '.part')
    for t in range(1, tentativas + 1):
        try:
            pos = parcial.stat().st_size if parcial.exists() else 0
            headers = {'Authorization': AUTH}
            if pos:
                headers['Range'] = f'bytes={pos}-'
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=300) as r:
                if pos and r.status != 206:
                    pos = 0  # servidor ignorou o Range: recomeça
                with open(parcial, 'ab' if pos else 'wb') as f:
                    shutil.copyfileobj(r, f, 4 << 20)
            with zipfile.ZipFile(parcial) as z:  # garante que o zip veio inteiro
                z.infolist()
            parcial.replace(destino)
            return destino
        except zipfile.BadZipFile:
            log(f'  {nome} veio corrompido, baixando de novo')
            parcial.unlink(missing_ok=True)
        except Exception as e:  # noqa: BLE001
            log(f'  falha ao baixar {nome} (tentativa {t}/{tentativas}): {e}')
        time.sleep(min(60, 5 * t))
    raise SystemExit(f'Não foi possível baixar {nome}')


class Fonte:
    """Entrega os .zip: baixando da Receita ou lendo de uma pasta local (testes)."""

    def __init__(self, mes, trabalho: Path, pasta_local: Path | None):
        self.mes, self.trabalho, self.local = mes, trabalho, pasta_local

    def obter(self, nome) -> Path | None:
        if self.local:
            p = self.local / nome
            return p if p.exists() else None
        log(f'Baixando {nome}')
        return baixar(self.mes, nome, self.trabalho / nome)

    def descartar(self, caminho: Path):
        if not self.local:
            caminho.unlink(missing_ok=True)


def linhas_zip(caminho: Path):
    with zipfile.ZipFile(caminho) as z:
        for info in z.infolist():
            with z.open(info) as f:
                texto = io.TextIOWrapper(f, encoding='latin-1', newline='')
                yield from csv.reader((l.replace('\0', '') for l in texto), delimiter=';', quotechar='"')


# ---------------------------------------------------------------- limpeza de campos

def limpa(s):
    s = ' '.join(s.split())
    return '' if not s.strip('*.-_ 0') else s


def telefone(ddd, numero):
    d = re.sub(r'\D', '', ddd) + re.sub(r'\D', '', numero)
    d = d.lstrip('0')
    return d if 10 <= len(d) <= 11 else ''


RE_EMAIL = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


def email(s):
    s = s.strip().lower()
    return s if RE_EMAIL.match(s) else ''


# ---------------------------------------------------------------- etapas

def processar_estabelecimentos(fonte: Fonte, saida: Path):
    """Filtra ativos com contato. Retorna o conjunto de CNPJs básicos (8 primeiros caracteres)."""
    basicos = set()
    lidos = gravados = 0
    with gzip.open(saida, 'wt', encoding='utf-8', newline='', compresslevel=1) as f:
        w = csv.writer(f, delimiter=';', lineterminator='\n')
        for i in range(10):
            z = fonte.obter(f'Estabelecimentos{i}.zip')
            if not z:
                continue
            log(f'Processando {z.name}')
            for r in linhas_zip(z):
                lidos += 1
                if len(r) < 28 or r[5] != '02':  # 02 = ATIVA
                    continue
                uf = r[19].strip()
                cnae = r[11].strip()
                if uf not in UFS or not re.fullmatch(r'\d{7}', cnae):
                    continue
                t1, t2, em = telefone(r[21], r[22]), telefone(r[23], r[24]), email(r[27])
                if t2 == t1:
                    t2 = ''
                if not t1 and t2:
                    t1, t2 = t2, ''
                if not (t1 or em):
                    continue
                endereco = limpa(' '.join(x for x in (r[13], r[14]) if x.strip()))
                if r[15].strip():
                    endereco += ', ' + limpa(r[15])
                if r[16].strip():
                    endereco += ' ' + limpa(r[16])
                basico = r[0].strip()
                w.writerow([basico, basico + r[1].strip() + r[2].strip(), limpa(r[4]), uf, cnae,
                            r[20].strip(), limpa(r[17]), endereco.strip(' ,'), re.sub(r'\D', '', r[18]),
                            t1, t2, em, r[10].strip(), r[3].strip()])
                basicos.add(basico)
                gravados += 1
            fonte.descartar(z)
            log(f'  {lidos:,} lidos, {gravados:,} ativos com contato até aqui')
    return basicos


def processar_empresas(fonte: Fonte, basicos: set, saida: Path):
    with gzip.open(saida, 'wt', encoding='utf-8', newline='', compresslevel=1) as f:
        w = csv.writer(f, delimiter=';', lineterminator='\n')
        for i in range(10):
            z = fonte.obter(f'Empresas{i}.zip')
            if not z:
                continue
            log(f'Processando {z.name}')
            for r in linhas_zip(z):
                if len(r) >= 6 and r[0] in basicos:
                    w.writerow([r[0], limpa(r[1]), r[5].strip()])
            fonte.descartar(z)


def tabela_simples(fonte: Fonte, nome):
    z = fonte.obter(nome)
    if not z:
        return {}
    d = {r[0].strip(): limpa(r[1]) for r in linhas_zip(z) if len(r) >= 2}
    fonte.descartar(z)
    return d


def gerar_indice(trabalho: Path, destino: Path, memoria: str):
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{memoria}'")
    con.execute(f"SET temp_directory='{(trabalho / 'duckdb_tmp').as_posix()}'")
    est_cols = "{'basico':'VARCHAR','cnpj':'VARCHAR','fantasia':'VARCHAR','uf':'VARCHAR','cnae':'VARCHAR'," \
               "'municipio':'VARCHAR','bairro':'VARCHAR','endereco':'VARCHAR','cep':'VARCHAR','tel1':'VARCHAR'," \
               "'tel2':'VARCHAR','email':'VARCHAR','abertura':'VARCHAR','matriz':'VARCHAR'}"
    emp_cols = "{'basico':'VARCHAR','razao':'VARCHAR','porte':'VARCHAR'}"
    opts = "delim=';', quote='\"', escape='\"', header=false"
    log('Juntando estabelecimentos com razão social (DuckDB)')
    cur = con.execute(f"""
        SELECT e.uf, e.cnae, e.cnpj, m.razao, e.fantasia, e.municipio, e.bairro, e.endereco, e.cep,
               e.tel1, e.tel2, e.email, e.abertura, e.matriz, m.porte
        FROM read_csv('{(trabalho / 'est.csv.gz').as_posix()}', {opts}, columns={est_cols}) e
        LEFT JOIN read_csv('{(trabalho / 'emp.csv.gz').as_posix()}', {opts}, columns={emp_cols}) m
          ON m.basico = e.basico
        ORDER BY e.uf, e.cnae, e.municipio, coalesce(nullif(e.fantasia, ''), m.razao)
    """)

    shards, municipios_usados = {}, set()
    total = com_tel = com_email = 0
    atual, arq, w = None, None, None
    while True:
        lote = cur.fetchmany(200_000)
        if not lote:
            break
        for row in lote:
            uf, cnae = row[0], row[1]
            if (uf, cnae) != atual:
                if arq:
                    arq.close()
                atual = (uf, cnae)
                (destino / uf).mkdir(parents=True, exist_ok=True)
                arq = gzip.open(destino / uf / f'{cnae}.csv.gz', 'wt', encoding='utf-8', newline='', compresslevel=9)
                w = csv.writer(arq, delimiter=';', lineterminator='\n')
                shards.setdefault(uf, {})[cnae] = 0
            w.writerow(['' if v is None else v for v in row[2:]])
            shards[uf][cnae] += 1
            municipios_usados.add((uf, row[5]))
            total += 1
            com_tel += bool(row[9])
            com_email += bool(row[11])
    if arq:
        arq.close()
    log(f'{total:,} empresas no índice, {sum(len(v) for v in shards.values()):,} arquivos')
    return shards, municipios_usados, {'total': total, 'com_telefone': com_tel, 'com_email': com_email}


# ---------------------------------------------------------------- Hugging Face

def url_hf(repo, caminho):
    return f'https://huggingface.co/datasets/{repo}/resolve/main/{caminho}'


def mes_publicado(repo):
    try:
        with urllib.request.urlopen(url_hf(repo, 'atual.json'), timeout=60) as r:
            return json.load(r).get('mes')
    except Exception:  # noqa: BLE001 — repositório novo/vazio
        return None


README_HF = """---
license: other
language: [pt]
pretty_name: Índice de empresas ativas com contato (CNPJ)
---
# Índice de empresas ativas com contato

Gerado automaticamente a partir dos **dados abertos do CNPJ da Receita Federal**
(estabelecimentos com situação ATIVA que possuem telefone ou email).

Estrutura: `AAAA-MM/UF/CNAE.csv.gz` (separador `;`, sem cabeçalho; colunas em `manifest.json`).
O arquivo `atual.json` aponta para o mês vigente.
"""


def publicar(repo, raiz: Path, mes):
    from huggingface_hub import HfApi

    api = HfApi(token=os.environ['HF_TOKEN'])
    api.create_repo(repo, repo_type='dataset', exist_ok=True)
    log(f'Enviando {mes} para huggingface.co/datasets/{repo}')
    api.upload_large_folder(repo_id=repo, repo_type='dataset', folder_path=str(raiz),
                            allow_patterns=[f'{mes}/**'])
    (raiz / 'README.md').write_text(README_HF, encoding='utf-8')
    (raiz / 'atual.json').write_text(json.dumps({'mes': mes, 'pasta': mes}), encoding='utf-8')
    api.upload_folder(repo_id=repo, repo_type='dataset', folder_path=str(raiz),
                      allow_patterns=['README.md', 'atual.json'],
                      commit_message=f'Base {mes} publicada')
    antigos = [e.path for e in api.list_repo_tree(repo, repo_type='dataset')
               if re.fullmatch(r'\d{4}-\d{2}', e.path) and e.path != mes]
    for p in antigos:
        log(f'Removendo mês antigo {p}')
        api.delete_folder(p, repo_id=repo, repo_type='dataset', commit_message=f'Remove {p}')
    # Compacta o histórico para não acumular gigabytes de versões antigas.
    api.super_squash_history(repo_id=repo, repo_type='dataset')


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--mes', help='AAAA-MM (padrão: o mais recente da Receita)')
    ap.add_argument('--forcar', action='store_true', help='reconstrói mesmo se o mês já estiver publicado')
    ap.add_argument('--hf-repo', default=os.environ.get('HF_REPO'), help='ex.: usuario/base-cnpj')
    ap.add_argument('--sem-upload', action='store_true', help='só gera os arquivos localmente')
    ap.add_argument('--trabalho', default='trabalho', help='pasta temporária')
    ap.add_argument('--saida', default='saida', help='pasta onde o índice é gerado')
    ap.add_argument('--arquivos-locais', help='usar .zip desta pasta em vez de baixar (testes)')
    ap.add_argument('--memoria', default='10GB', help='limite de memória do DuckDB')
    a = ap.parse_args()

    if not a.sem_upload and (not a.hf_repo or not os.environ.get('HF_TOKEN')):
        raise SystemExit('Defina HF_REPO e HF_TOKEN (ou use --sem-upload).')

    mes = a.mes or (ultimo_mes() if not a.arquivos_locais else 'local')
    log(f'Mês da Receita: {mes}')
    if not a.sem_upload and not a.forcar and mes_publicado(a.hf_repo) == mes:
        log('Esse mês já está publicado. Nada a fazer.')
        sinalizar(False, mes)
        return

    if not a.arquivos_locais:
        faltando = {f'{p}{i}.zip' for p in ('Empresas', 'Estabelecimentos') for i in range(10)} - arquivos_do_mes(mes)
        if faltando:
            raise SystemExit(f'Mês {mes} incompleto na Receita (faltam {sorted(faltando)}). Tente mais tarde.')

    trabalho, raiz = Path(a.trabalho), Path(a.saida)
    shutil.rmtree(raiz / mes, ignore_errors=True)
    trabalho.mkdir(parents=True, exist_ok=True)
    destino = raiz / mes
    destino.mkdir(parents=True)
    fonte = Fonte(mes, trabalho, Path(a.arquivos_locais) if a.arquivos_locais else None)

    inicio = time.time()
    basicos = processar_estabelecimentos(fonte, trabalho / 'est.csv.gz')
    processar_empresas(fonte, basicos, trabalho / 'emp.csv.gz')
    del basicos
    cnaes = tabela_simples(fonte, 'Cnaes.zip')
    nomes_mun = tabela_simples(fonte, 'Municipios.zip')

    shards, mun_usados, totais = gerar_indice(trabalho, destino, a.memoria)

    municipios = {}
    for uf, cod in sorted(mun_usados):
        municipios.setdefault(uf, []).append([cod, nomes_mun.get(cod, cod)])
    for uf in municipios:
        municipios[uf].sort(key=lambda x: x[1])

    manifest = {'mes': mes, 'gerado_em': datetime.now(timezone.utc).isoformat(timespec='seconds'),
                'colunas': COLUNAS_SAIDA, **totais, 'shards': shards}
    for nome, obj in (('manifest.json', manifest), ('cnaes.json', cnaes), ('municipios.json', municipios)):
        (destino / nome).write_text(json.dumps(obj, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')

    tamanho = sum(p.stat().st_size for p in destino.rglob('*') if p.is_file())
    log(f'Índice gerado em {destino} ({tamanho / 2**20:,.0f} MB) em {(time.time() - inicio) / 60:.0f} min')

    if not a.sem_upload:
        publicar(a.hf_repo, raiz, mes)
        log('Publicado.')
    sinalizar(True, mes)


def sinalizar(atualizado, mes):
    """Informa o GitHub Actions se houve atualização (para registrar no repositório)."""
    out = os.environ.get('GITHUB_OUTPUT')
    if out:
        with open(out, 'a') as f:
            f.write(f'atualizado={"true" if atualizado else "false"}\nmes={mes}\n')


if __name__ == '__main__':
    sys.exit(main())
