import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { executarSQL } from './sankhya/sankhya-api.js';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json()); // Suporte para envio de JSON no POST
app.use(express.text()); // Suporte para envio de texto puro no POST

// 📍 Rota para servir a Interface Gráfica (HTML, CSS, JS) - aberta p/ carregar o frontend visual (apenas HTML, dados e queries bloqueados no gateway abaixo)
app.use(express.static('public'));

// 🔒 MIDDLEWARE DE SEGURANÇA (BLINDAGEM DA API)
app.use((req, res, next) => {
  const tokenConfigurado = process.env.API_TOKEN;
  
  if (!tokenConfigurado) {
     console.error('[Segurança] ALERTA: Variável API_TOKEN não foi encontrada no .env do servidor!');
     return res.status(500).json({ erro: 'Servidor trancado por falta de configuração. Variável API_TOKEN exigida no arquivo .env.' });
  }

  // Verifica se o Token foi enviado no Cabeçalho (x-api-key) ou diretamente na URL (?token=...)
  const tokenInformado = req.headers['x-api-key'] || req.query.token;

  if (tokenInformado !== tokenConfigurado) {
     return res.status(401).json({ erro: '🔒 Acesso Negado: Token de Segurança Inválido ou Ausente.' });
  }

  next(); // Passou na segurança? Pode consultar os endpoints /sql abaixo!
});

/**
 * 📍 Rota GET /sql
 * Exemplo de uso: http://localhost:3000/sql?query=SELECT TOP 10 NUNOTA, VLRNOTA FROM TGFCAB
 */
app.get('/sql', async (req, res) => {
  try {
    const query = req.query.query || req.query.q;
    
    if (!query) {
      return res.status(400).json({ erro: 'O parâmetro de pesquisa "query" ou "q" é obrigatório.' });
    }
    
    console.log(`[BancoSankhya] GET SQL Recebido: ${query}`);
    const resultado = await executarSQL(query);
    
    // Retornamos direto 'registros', para que o Power BI já enxergue como uma lista plana e converta em Tabela logo de cara
    res.json(resultado.registros);
  } catch (erro) {
    console.error(`[BancoSankhya] Erro ao executar GET SQL:`, erro.message);
    res.status(500).json({ erro: erro.message });
  }
});

/**
 * 📍 Rota POST /sql
 * Exemplo de uso em PowerBI via conector Web usando JSON ou texto puro na request.
 */
app.post('/sql', async (req, res) => {
  try {
    let query = req.body;
    
    // Se o cliente (Power BI) mandou via JSON {"query": "SELECT..."}
    if (typeof query === 'object') {
      query = query.query || query.sql || query.q;
    }
    
    if (!query || typeof query !== 'string') {
        return res.status(400).json({ erro: 'A query precisa ser enviada no corpo da requisição.' });
    }

    console.log(`[BancoSankhya] POST SQL Recebido: ${query}`);
    const resultado = await executarSQL(query);
    
    // Mesma lógica, retornando os registros planos como array para fácil ingestão do Power BI
    res.json(resultado.registros);
  } catch (erro) {
    console.error(`[BancoSankhya] Erro ao executar POST SQL:`, erro.message);
    res.status(500).json({ erro: erro.message });
  }
});

// Rota GET / para teste básico removida pois a pasta "public" agora responderá no "/" para abrir a interface gráfica.

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n======================================================`);
  console.log(`✅ Banco de Dados Sankhya (Proxy API) na porta ${PORT}`);
  console.log(`======================================================`);
  console.log(`\nComo conectar no \x1b[33mPower BI\x1b[0m:\n`);
  console.log(`  1. No Power BI, vá em "Obter Dados" -> "Web".`);
  console.log(`  2. Para consultas simples curtas, coloque na aba "Básico", exemplo:`);
  console.log(`     http://localhost:${PORT}/sql?query=SELECT CODEMP, NOMEFANTASIA FROM TSIEMP`);
  console.log(`\n  3. Para consultas avançadas (recomendado) e longas:`);
  console.log(`     a. Escolha a aba "Avançado".`);
  console.log(`     b. Partes da URL: http://localhost:${PORT}/sql`);
  console.log(`     c. Cabeçalho de solicitação HTTP: Content-Type = application/json`);
  console.log(`     d. Parâmetros da página solicitação HTTP (corpo/body): `);
  console.log(`        {"query": "SELECT TOP 100 * FROM TGFCAB"}`);
  console.log(`======================================================\n`);
});
