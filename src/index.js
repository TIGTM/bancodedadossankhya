import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { executarSQL } from './sankhya/sankhya-api.js';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.text());

// Ignorar favicon p/ evitar poluição no log e console
app.get('/favicon.ico', (req, res) => res.status(204).end());

app.use(express.static('public'));

app.use((req, res, next) => {
  const tokenConfigurado = process.env.API_TOKEN;
  
  if (!tokenConfigurado) {
     console.error('[Segurança] ALERTA: API_TOKEN não configurada no .env');
     return res.status(500).json({ erro: 'Servidor trancado por falta de configuração.' });
  }

  const tokenInformado = req.headers['x-api-key'] || req.query.token;

  if (tokenInformado !== tokenConfigurado) {
     return res.status(401).json({ erro: '🔒 Acesso Negado: Token Inválido.' });
  }

  next();
});

app.get('/sql', async (req, res) => {
  try {
    const query = req.query.query || req.query.q;
    if (!query) return res.status(400).json({ erro: 'Query é obrigatória.' });
    
    console.log(`[BancoSankhya] GET SQL: ${query}`);
    const resultado = await executarSQL(query);
    res.json(resultado.registros);
  } catch (erro) {
    console.error(`[BancoSankhya] Erro GET:`, erro.message);
    res.status(500).json({ erro: erro.message });
  }
});

app.post('/sql', async (req, res) => {
  try {
    let query = req.body;
    if (typeof query === 'object') {
      query = query.query || query.sql || query.q;
    }
    
    if (!query || typeof query !== 'string') {
        return res.status(400).json({ erro: 'Query é obrigatória.' });
    }

    console.log(`[BancoSankhya] POST SQL: ${query}`);
    const resultado = await executarSQL(query);
    res.json(resultado.registros);
  } catch (erro) {
    console.error(`[BancoSankhya] Erro POST:`, erro.message);
    res.status(500).json({ erro: erro.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n======================================================`);
  console.log(`✅ Banco de Dados Sankhya (Proxy API) na porta ${PORT}`);
  console.log(`======================================================`);
});
