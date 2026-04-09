/**
 * Cliente Sankhya — 2 Camadas de Acesso (OAuth 2.0)
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  CAMADA 1 — GATEWAY (mge-dwf)                                       │
 * │  URL: https://api.sankhya.com.br/gateway/v1/mge/service.sbr        │
 * │  Auth: OAuth 2.0 Bearer                                             │
 * │  Uso:  Parceiro, Produto, ItemNota, GrupoProduto, Vendedor, Empresa │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │  CAMADA 2 — REST v1 (endpoints RESTful modernos)                    │
 * │  URL: https://api.sankhya.com.br/v1/{recurso}                      │
 * │  Auth: OAuth 2.0 Bearer                                             │
 * │  Uso:  Financeiro, Pedidos, NFC-e, HCM, Logística, Produtos...     │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * Exporta: query, save, remove, execute, rest
 */

import axios from 'axios';
import { obterToken, invalidarToken } from './auth.js';

// ─── URLs Base ────────────────────────────────────────────────────────────────
const URL_GATEWAY        = 'https://api.sankhya.com.br/gateway/v1/mge/service.sbr';
const URL_GATEWAY_MGECOM = 'https://api.sankhya.com.br/gateway/v1/mgecom/service.sbr';
const URL_GATEWAY_MGEFIN = 'https://api.sankhya.com.br/gateway/v1/mgefin/service.sbr';
const URL_REST           = 'https://api.sankhya.com.br/v1';

// Entidades que devem tentar mgecom + mgefin antes de mge-dwf
// (apenas entidades confirmadas que NÃO existem no mge-dwf padrão)
const ENTIDADES_MGECOM = new Set([
  'CabecNota','CabecalhoNota','TipoOperacao','PedidoVenda','ItemPedido','Duplicata',
  // ItemNota foi REMOVIDO — funciona no mge-dwf e falha no mgecom
  'Financeiro', // tenta mgecom + mgefin + mge para suportar update via DatasetSP.save
]);

// Status Sankhya que indicam falha (devem acionar fallback entre módulos)
const STATUS_ERRO = new Set(['1','3','4','5']);

// ─────────────────────────────────────────────────────────────────────────────
// CAMADA 1 — GATEWAY
// ─────────────────────────────────────────────────────────────────────────────

async function chamarUrl(url, serviceName, outputType, requestBody) {
  const modulo = url.includes('mgecom') ? 'mgecom' : 'mge';
  const token  = await obterToken();
  let resposta;
  try {
    resposta = await axios.post(url, { serviceName, requestBody }, {
      params:  { serviceName, outputType },
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    });
  } catch (err) {
    const detalhe = err.response?.data?.statusMessage || err.message;
    throw new Error(`[${modulo}] HTTP Error: ${detalhe}`);
  }
  const dados   = resposta.data;
  const excecao = dados?.responseBody?.tsException?.message;
  if (excecao) throw new Error(`[${modulo}] ${excecao}`);
  // status != '0' indica erro no Sankhya (1=erro, 3=não autorizado, etc.)
  if (dados?.status && dados.status !== '0') {
    throw new Error(`[${modulo}] ${dados.statusMessage || `status ${dados.status}`}`);
  }
  // Status '0' mas statusMessage com exceção Java = erro real disfarçado de sucesso
  const msgStatus = dados?.statusMessage || '';
  if (msgStatus.includes('Exception') || msgStatus.includes('java.lang') || msgStatus.includes('NullPointer')) {
    throw new Error(`[${modulo}] ${msgStatus}`);
  }
  return dados;
}

async function chamarGateway(serviceName, outputType = 'json', requestBody = {}, entidade = '') {
  const urls = ENTIDADES_MGECOM.has(entidade)
    ? [URL_GATEWAY_MGECOM, URL_GATEWAY_MGEFIN, URL_GATEWAY]  // notas fiscais: tenta todos os módulos
    : [URL_GATEWAY];                                          // demais: só mge-dwf

  let ultimoErro;
  for (const url of urls) {
    try {
      return await chamarUrl(url, serviceName, outputType, requestBody);
    } catch (err) {
      console.log(`[gateway] falhou (${url.includes('mgecom') ? 'mgecom' : 'mge'}) para "${entidade}": ${err.message}`);
      ultimoErro = err;
    }
  }
  throw ultimoErro;
}

/**
 * Busca via CRUDServiceProvider.loadRecords (entidades padrão do gateway).
 */
async function queryCRUD(entidade, campos, criteria, pagina, itensPorPagina) {
  const dataSet = {
    rootEntity: entidade,
    ignoreCalculatedFields: 'true',
    useFileBasedPagination: 'true',
    includePresentationFields: 'N',
    tryJoinedFields: 'true',
    offsetPage: String(pagina - 1),
    entity: [{ path: '', fieldset: { list: campos.join(', ') } }],
  };
  if (criteria) dataSet.criteria = { expression: { $: criteria } };

  const resposta  = await chamarGateway('CRUDServiceProvider.loadRecords', 'json', { dataSet }, entidade);
  const entities  = resposta?.responseBody?.entities;
  if (!entities) return resposta;

  const nomesCampos    = (entities.metadata?.fields?.field ?? []).map((f) => f.name);
  const listaEntidades = entities.entity
    ? (Array.isArray(entities.entity) ? entities.entity : [entities.entity])
    : [];

  return {
    total:         entities.total,
    hasMoreResult: entities.hasMoreResult,
    offsetPage:    entities.offsetPage,
    registros:     listaEntidades.map((row) =>
      Object.fromEntries(nomesCampos.map((nome, i) => [nome, row[`f${i}`]?.$ ?? null]))
    ),
  };
}

/**
 * Converte critério SQL-like para o formato DatasetSP (campos com prefixo "this.").
 * Ex: "CODEMP = 2 AND DTNEG >= '01/02/2026'"
 *  → "this.CODEMP = 2 AND this.DTNEG >= '01/02/2026'"
 */
function criteriaParaDataset(criteria) {
  if (!criteria) return criteria;
  // Palavras reservadas SQL que NÃO devem receber "this."
  const reservadas = new Set([
    'AND','OR','NOT','IN','BETWEEN','LIKE','IS','NULL','TRUE','FALSE',
    'SELECT','FROM','WHERE','ORDER','BY','GROUP','HAVING','DISTINCT',
    'ASC','DESC','AS','ON','JOIN','LEFT','RIGHT','INNER','OUTER',
  ]);
  // Adiciona "this." antes de identificadores em MAIÚSCULAS que não sejam reservadas
  // e que não estejam já precedidos de "this."
  return criteria.replace(/\b([A-Z_][A-Z0-9_]*)\b(?!\s*\()/g, (match) => {
    if (reservadas.has(match)) return match;
    return `this.${match}`;
  });
}

/**
 * Busca via DatasetSP.loadRecords (formato alternativo — suporta CabecalhoNota e
 * outras entidades não disponíveis no CRUDServiceProvider).
 */
async function queryDataset(entidade, campos, criteria, pagina) {
  const datasetRequest = {
    rootEntity:                entidade,
    includePresentationFields: 'N',
    offsetPage:                String(pagina - 1),
    entity: { fieldset: { list: campos.join(', ') } },
  };
  if (criteria) datasetRequest.criteria = { expression: { $: criteriaParaDataset(criteria) } };

  const resposta  = await chamarGateway('DatasetSP.loadRecords', 'json', { datasetRequest }, entidade);
  const entities  = resposta?.responseBody?.entities;
  if (!entities) return resposta;

  const nomesCampos    = (entities.metadata?.fields?.field ?? []).map((f) => f.name);
  const listaEntidades = entities.entity
    ? (Array.isArray(entities.entity) ? entities.entity : [entities.entity])
    : [];

  return {
    total:         entities.total,
    hasMoreResult: entities.hasMoreResult,
    offsetPage:    entities.offsetPage,
    registros:     listaEntidades.map((row) =>
      Object.fromEntries(nomesCampos.map((nome, i) => [nome, row[`f${i}`]?.$ ?? null]))
    ),
    _servico: 'DatasetSP.loadRecords',
  };
}

/**
 * Busca registros de uma entidade via Gateway OAuth.
 *
 * Tenta primeiro CRUDServiceProvider.loadRecords (padrão).
 * Se falhar, tenta automaticamente DatasetSP.loadRecords com o mesmo nome
 * e também com o alias "CabecalhoNota" quando a entidade for "CabecNota".
 */
export async function query(entidade, campos, criteria = null, pagina = 1, itensPorPagina = 50) {
  // Tentativa 1 — CRUD padrão
  try {
    return await queryCRUD(entidade, campos, criteria, pagina, itensPorPagina);
  } catch (errCRUD) {
    console.log(`[query] CRUDServiceProvider falhou para "${entidade}": ${errCRUD.message}`);

    // Tentativa 2 — DatasetSP com mesmo nome
    const tentativas = [entidade];

    // Tentativa 3 — alias conhecidos (CabecNota ↔ CabecalhoNota)
    if (entidade === 'CabecNota')     tentativas.push('CabecalhoNota');
    if (entidade === 'CabecalhoNota') tentativas.push('CabecNota');

    let ultimoErro = errCRUD;
    for (const nome of tentativas) {
      try {
        const resultado = await queryDataset(nome, campos, criteria, pagina);
        if (nome !== entidade) resultado._aliasUsado = nome;
        console.log(`[query] DatasetSP OK para "${nome}"`);
        return resultado;
      } catch (errDS) {
        console.log(`[query] DatasetSP falhou para "${nome}": ${errDS.message}`);
        ultimoErro = errDS;
      }
    }

    throw ultimoErro;
  }
}

/**
 * Insere ou atualiza registros via Gateway (DatasetSP.save).
 */
export async function save(entidade, registros) {
  const campos  = Object.keys(registros[0]);
  const records = registros.map((reg) => ({
    values: Object.values(reg).reduce((acc, val, idx) => {
      acc[String(idx)] = String(val);
      return acc;
    }, {}),
  }));

  return chamarGateway('DatasetSP.save', 'json', {
    entityName: entidade,
    standAlone: true,
    fields:     campos,
    records,
    ignoreListenerMethods: '',
  });
}

/**
 * Deleta registros via Gateway (DatasetSP.remove).
 */
export async function remove(entidade, chaves) {
  const campos  = Object.keys(chaves[0]);
  const records = chaves.map((chave) => ({
    values: Object.values(chave).reduce((acc, val, idx) => {
      acc[String(idx)] = String(val);
      return acc;
    }, {}),
  }));

  return chamarGateway('DatasetSP.remove', 'json', {
    entityName: entidade,
    standAlone: true,
    fields:     campos,
    records,
  });
}

/**
 * Executa qualquer serviço genérico via Gateway.
 */
export async function execute(serviceName, outputType = 'json', requestBody = {}) {
  return chamarGateway(serviceName, outputType, requestBody);
}

/**
 * Executa SQL direto via DbExplorerSP.executeQuery.
 * Permite consultar QUALQUER tabela do Sankhya (ex: TGFCAB, TGFITE, TGFPAR)
 * sem depender da camada de entidades.
 *
 * Paginação automática: a API Sankhya limita 5.000 linhas por chamada. Se o
 * SQL não contiver limite explícito (ROWNUM / TOP / FETCH), a função busca em
 * blocos de 5.000 e combina os resultados de forma transparente.
 * Detecta automaticamente se o banco é Oracle ou SQL Server.
 *
 * @param {string} sql - SQL (ex: "SELECT NUNOTA, DTNEG FROM TGFCAB WHERE CODEMP = 2")
 * @returns {Promise<{colunas: string[], registros: object[], total: number}>}
 */

// Cache do tipo de banco detectado no primeiro uso ('oracle' | 'sqlserver' | null)
let _tipoBanco = null;

function _sqlPaginado(sql, pagina, bloco, tipo) {
  const offset = pagina * bloco;
  if (tipo === 'oracle') {
    return offset === 0
      ? `SELECT * FROM (${sql}) q__ WHERE ROWNUM <= ${bloco}`
      : `SELECT * FROM (SELECT q__.*, ROWNUM AS rn__ FROM (${sql}) q__ WHERE ROWNUM <= ${offset + bloco}) WHERE rn__ > ${offset}`;
  }
  // SQL Server
  return offset === 0
    ? `SELECT TOP ${bloco} * FROM (${sql}) AS q__`
    : `SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn__ FROM (${sql}) AS q__) AS paged__ WHERE rn__ > ${offset} AND rn__ <= ${offset + bloco}`;
}

export async function executarSQL(sql, _retry = false) {
  // Se o SQL já contém limite explícito — não paginar
  const temLimiteExplicito = /\bROWNUM\b|\bTOP\b|\bFETCH\b/i.test(sql);
  if (temLimiteExplicito) {
    return _executarSQLBruto(sql, _retry);
  }

  const BLOCO     = 5000;
  const tempoInicio = Date.now();
  let todos       = [];
  let colunas     = [];
  let pagina      = 0;

  // Se ainda não sabemos o tipo de banco, detectar na primeira página
  if (!_tipoBanco) {
    // Tenta Oracle primeiro
    try {
      const sqlOracle = _sqlPaginado(sql, 0, BLOCO, 'oracle');
      const resultado = await _executarSQLBruto(sqlOracle, _retry);
      _tipoBanco = 'oracle';
      console.log(`[sql] Banco detectado: Oracle. Iniciando paginação automática...`);
      colunas = resultado.colunas.filter(c => c !== 'RN__');
      const linhas = resultado.registros.map(({ RN__: _rn, ...resto }) => resto);
      todos = linhas;
      console.log(`[sql] Página 1: ${linhas.length} linhas (total acumulado: ${todos.length})`);
      if (linhas.length < BLOCO) {
        const tempoTotal = Date.now() - tempoInicio;
        console.log(`[sql] Concluído (${tempoTotal}ms) — ${todos.length} linhas totais`);
        return { colunas, registros: todos, total: todos.length };
      }
      pagina = 1;
    } catch (errOracle) {
      // Se errou por sintaxe, assume SQL Server
      if (/sintaxe|syntax|rownum/i.test(errOracle.message)) {
        _tipoBanco = 'sqlserver';
        console.log(`[sql] Banco detectado: SQL Server. Iniciando paginação automática...`);
      } else {
        throw errOracle;
      }
    }
  } else {
    console.log(`[sql] Iniciando execução com paginação automática (${_tipoBanco})...`);
  }

  // Busca restante das páginas (ou todas as páginas se SQL Server)
  while (true) {
    const sqlPag = _sqlPaginado(sql, pagina, BLOCO, _tipoBanco);
    const resultado = await _executarSQLBruto(sqlPag, _retry);

    if (pagina === 0) {
      colunas = resultado.colunas.filter(c => c !== 'RN__');
    }

    const linhas = resultado.registros.map(({ RN__: _rn, ...resto }) => resto);
    todos = todos.concat(linhas);

    console.log(`[sql] Página ${pagina + 1}: ${linhas.length} linhas (total acumulado: ${todos.length})`);

    if (linhas.length < BLOCO) break;
    pagina++;
  }

  const tempoTotal = Date.now() - tempoInicio;
  console.log(`[sql] Concluído (${tempoTotal}ms) — ${todos.length} linhas totais, colunas: [${colunas}]`);
  return { colunas, registros: todos, total: todos.length };
}

/**
 * Executa o SQL exatamente como recebido, sem paginação.
 * @internal
 */
async function _executarSQLBruto(sql, _retry = false) {
  const tempoInicio = Date.now();

  const token = await obterToken();
  const url   = URL_GATEWAY;

  let resposta;
  try {
    resposta = await axios.post(url,
      { serviceName: 'DbExplorerSP.executeQuery', requestBody: { sql } },
      {
        params:  { serviceName: 'DbExplorerSP.executeQuery', outputType: 'json' },
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      }
    );
  } catch (err) {
    const detalhe = err.response?.data?.statusMessage || err.message;
    throw new Error(`[sql] HTTP error: ${detalhe}`);
  }

  const dados = resposta.data;
  const body  = dados?.responseBody;

  if (!body || (!body.rows && !body.fieldsMetadata)) {
    const msg = dados?.statusMessage || dados?.responseBody?.tsException?.message || 'Sem dados';
    // Token expirado/invalidado pelo Sankhya — renova e tenta 1x
    if (!_retry && /n[aã]o autorizado|unauthorized|token/i.test(msg)) {
      console.log(`[sql] Token rejeitado pelo Sankhya, renovando...`);
      invalidarToken();
      return _executarSQLBruto(sql, true);
    }
    throw new Error(`[sql] ${msg}`);
  }

  const colunas = (body.fieldsMetadata ?? []).map(f => f.name);
  const rows    = Array.isArray(body.rows) ? body.rows : [];

  const registros = rows.map(row =>
    Array.isArray(row)
      ? Object.fromEntries(colunas.map((col, i) => [col, row[i] ?? null]))
      : row
  );

  const tempoTotal = Date.now() - tempoInicio;
  console.log(`[sql] DbExplorerSP OK (${tempoTotal}ms) — ${rows.length} linhas`);
  return { colunas, registros, total: rows.length };
}

// ─────────────────────────────────────────────────────────────────────────────
// CAMADA 2 — REST v1 (endpoints RESTful modernos)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Chama qualquer endpoint da API REST v1 do Sankhya.
 *
 * @param {string} metodo   - HTTP: 'GET' | 'POST' | 'PUT' | 'DELETE'
 * @param {string} endpoint - Caminho após /v1/ (ex: "financeiros/receitas")
 * @param {object} [corpo]  - Corpo JSON para POST/PUT
 * @param {object} [params] - Query params para GET
 */
export async function rest(metodo, endpoint, corpo = null, params = {}) {
  const token = await obterToken();
  const url   = `${URL_REST}/${endpoint.replace(/^\/+/, '')}`;

  try {
    const { data } = await axios.request({
      method:  metodo.toUpperCase(),
      url,
      params,
      data:    corpo,
      headers: {
        Authorization:  `Bearer ${token}`,
        'Content-Type': 'application/json',
        Accept:         'application/json',
      },
    });
    return data;
  } catch (err) {
    const detalhe = err.response?.data
      ? JSON.stringify(err.response.data)
      : err.message;
    throw new Error(`Sankhya REST Error [${metodo.toUpperCase()} /v1/${endpoint}]: ${detalhe}`);
  }
}
