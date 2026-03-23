/**
 * Módulo de autenticação da API Sankhya
 *
 * OAuth 2.0 Client Credentials (único fluxo suportado)
 *   → obterToken()
 *   → Usa SANKHYA_CLIENT_ID + SANKHYA_CLIENT_SECRET + SANKHYA_APPKEY
 *   → Token expira em 300s; renova automaticamente a cada 290s
 */

import axios from 'axios';

// ─── URL ──────────────────────────────────────────────────────────────────────
const AUTH_URL = 'https://api.sankhya.com.br/authenticate'; // OAuth 2.0

// ─── Margem de renovação: renova 10s antes de expirar ─────────────────────────
const RENOVAR_ANTES_MS = 290 * 1000; // 300s - 10s = 290s

// ─── Estado interno ───────────────────────────────────────────────────────────
let tokenOAuth    = null;
let tokenOAuthExp = null; // timestamp (ms) de expiração

// ─────────────────────────────────────────────────────────────────────────────
// OAuth 2.0 Client Credentials
// ─────────────────────────────────────────────────────────────────────────────

async function autenticarOAuth() {
  const clientId     = process.env.SANKHYA_CLIENT_ID;
  const clientSecret = process.env.SANKHYA_CLIENT_SECRET;
  const appkey       = process.env.SANKHYA_APPKEY;

  if (!clientId || !clientSecret || !appkey) {
    throw new Error(
      'Credenciais OAuth não configuradas. Defina SANKHYA_CLIENT_ID, ' +
      'SANKHYA_CLIENT_SECRET e SANKHYA_APPKEY no arquivo .env'
    );
  }

  const corpo = new URLSearchParams({
    grant_type:    'client_credentials',
    client_id:     clientId,
    client_secret: clientSecret,
  });

  const resposta = await axios.post(AUTH_URL, corpo.toString(), {
    headers: {
      accept:         'application/json',
      'content-type': 'application/x-www-form-urlencoded',
      'X-Token':      appkey,
    },
  }).catch((err) => {
    const detalhe = err.response?.data ? JSON.stringify(err.response.data) : err.message;
    throw new Error(`Falha na autenticação OAuth (${err.response?.status ?? 'network'}): ${detalhe}`);
  });

  const { access_token } = resposta.data;
  if (!access_token) throw new Error('Resposta OAuth não contém access_token');

  tokenOAuth    = access_token;
  tokenOAuthExp = Date.now() + RENOVAR_ANTES_MS;
  return tokenOAuth;
}

/**
 * Retorna o token OAuth 2.0 válido (renova automaticamente se expirado).
 *
 * @returns {Promise<string>} Bearer token ativo
 */
export async function obterToken() {
  if (!tokenOAuth || Date.now() >= tokenOAuthExp) {
    await autenticarOAuth();
  }
  return tokenOAuth;
}
