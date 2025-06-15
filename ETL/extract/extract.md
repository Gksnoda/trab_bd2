# 📊 Módulo de Extração - Twitch Analytics

Este módulo é responsável por extrair dados da **API da Twitch** de forma assíncrona e eficiente, coletando informações sobre streams, usuários, vídeos, clips e games.

## 🎯 Visão Geral

O processo de extração segue uma **sequência lógica** onde cada etapa depende dos dados da anterior:

```
Streams → Users → Videos/Clips → Games
```

Todos os dados são salvos em formato **JSON** no diretório `../data/raw/`.

---

## 📁 Estrutura dos Arquivos

### 🔧 **`twitch_api.py`** (Módulo Base)
**Localização**: `ETL/twitch_api.py`

**Função**: Classe base para inicialização da API da Twitch
- Carrega credenciais do arquivo `.env`
- Configura headers de autenticação
- Define URL base da API (`https://api.twitch.tv/helix`)
- **NÃO contém endpoints específicos** - apenas inicialização

**Uso**:
```python
api = TwitchAPI()  # Inicializa e autentica
# Usar api.base_url e api.headers nos outros arquivos
```

---

### 🌊 **`streams.py`** - Extração de Streams
**Endpoint**: `/streams`

**O que faz**:
- Busca streams **ao vivo** da Twitch
- Coleta informações como: viewer count, game_id, user_id, título, linguagem
- Processa **10 páginas** de **100 streams** cada = até 1.000 streams

**Configurações**:
```python
MAX_PAGES = 10          # Páginas para buscar
STREAMS_PER_PAGE = 100  # Streams por página
ENDPOINT = "/streams"   # Endpoint da API
```

**Saída**: `streams.json`
- Lista de streams ao vivo
- Metadados de paginação e estatísticas

---

### 👥 **`users.py`** - Extração de Usuários
**Endpoint**: `/users`
**Depende de**: `streams.json`

**O que faz**:
- Lê `streams.json` e extrai **user_ids únicos**
- Busca dados completos dos usuários na API
- Processa **30 lotes simultâneos** de **100 usuários** cada

**Configurações**:
```python
USERS_PER_BATCH = 100   # Usuários por lote
CONCURRENT_BATCHES = 30 # Lotes simultâneos
ENDPOINT = "/users"     # Endpoint da API
```

**Saída**: `users.json`
- Dados completos dos usuários (nome, descrição, seguidores, etc.)
- Taxa de sucesso da coleta

---

### 🎬 **`videos.py`** - Extração de Vídeos
**Endpoint**: `/videos`
**Depende de**: `users.json`

**O que faz**:
- Lê `users.json` e extrai **user_ids**
- Para cada usuário, busca seus **vídeos arquivados**
- Processa **5 usuários simultâneos**, **5 páginas** de **30 vídeos** cada

**Configurações**:
```python
MAX_PAGES = 5           # Páginas por usuário
VIDEOS_PER_PAGE = 30    # Vídeos por página
VIDEO_TYPE = "archive"  # Tipo de vídeo
CONCURRENT_USERS = 5    # Usuários simultâneos
ENDPOINT = "/videos"    # Endpoint da API
```

**Saída**: `videos.json`
- Vídeos de todos os usuários
- Estatísticas de coleta por usuário

---

### 🎯 **`clips.py`** - Extração de Clips
**Endpoint**: `/clips`
**Depende de**: `users.json`

**O que faz**:
- Lê `users.json` e extrai **user_ids**
- Para cada usuário, busca seus **clips populares**
- Processa **10 usuários simultâneos**, **2 páginas** de **30 clips** cada

**Configurações**:
```python
MAX_PAGES = 2           # Páginas por usuário
CLIPS_PER_PAGE = 30     # Clips por página
CONCURRENT_USERS = 10   # Usuários simultâneos
ENDPOINT = "/clips"     # Endpoint da API
```

**Saída**: `clips.json`
- Clips de todos os usuários
- Mais rápido que vídeos (menos páginas, mais concorrência)

---

### 🎮 **`games.py`** - Extração de Games
**Endpoint**: `/games`
**Depende de**: `streams.json`, `videos.json`, `clips.json`

**O que faz**:
- Lê **TODOS** os arquivos JSON gerados anteriormente
- Extrai **game_ids únicos** de streams, vídeos e clips
- Busca dados completos dos games (nome, categoria, arte)
- Processa **30 lotes simultâneos** de **100 games** cada

**Configurações**:
```python
GAMES_PER_BATCH = 100   # Games por lote
CONCURRENT_BATCHES = 30 # Lotes simultâneos
ENDPOINT = "/games"     # Endpoint da API
```

**Saída**: `games.json`
- Dados completos de todos os games encontrados
- Consolida informações de múltiplas fontes

---

### 🚀 **`run_extract.py`** - Orquestrador
**O que faz**:
- Executa **TODOS** os scripts em sequência
- Monitora progresso e tempo de cada etapa
- Gera relatório final com estatísticas
- Continua mesmo se uma etapa falhar

**Ordem de Execução**:
1. `streams.py` → Base de dados
2. `users.py` → Usuários das streams
3. `videos.py` → Vídeos dos usuários
4. `clips.py` → Clips dos usuários
5. `games.py` → Games de todas as fontes

**Recursos**:
- Timeout de 30 minutos por script
- Captura de erros detalhada
- Relatório visual com tempos
- Suporte a Ctrl+C para interrupção

---

## ⚡ Tecnologias Utilizadas

### **Processamento Assíncrono**
- **`asyncio`**: Programação assíncrona
- **`aiohttp`**: Requisições HTTP assíncronas
- **`aiofiles`**: Operações de arquivo assíncronas
- **`tqdm`**: Barras de progresso visuais

### **Controle de Concorrência**
- **Semáforos**: Limitam requisições simultâneas
- **Batching**: Agrupa requisições para eficiência
- **Rate Limiting**: Respeita limites da API da Twitch

---

## 🚀 Como Usar

### **Execução Individual**
```bash
cd ETL/extract

# Executar um script específico
uv run streams.py
uv run users.py
uv run videos.py
uv run clips.py
uv run games.py
```

### **Execução Completa**
```bash
cd ETL/extract

# Executar todo o pipeline
uv run run_extract.py
```

### **Configuração**
Cada arquivo tem **constantes no topo** para fácil ajuste:
```python
# Exemplo: ajustar concorrência em videos.py
CONCURRENT_USERS = 10  # Era 5, agora 10 simultâneos
MAX_PAGES = 3          # Era 5, agora 3 páginas
```

---

## 📊 Dados Coletados

### **Volumes Típicos**
- **Streams**: ~1.000 streams ao vivo
- **Users**: ~800-900 usuários únicos
- **Videos**: ~5.000-15.000 vídeos (depende da atividade)
- **Clips**: ~2.000-8.000 clips
- **Games**: ~200-500 games únicos

### **Tempo de Execução**
- **streams.py**: ~45 segundos
- **users.py**: ~15 segundos
- **videos.py**: ~3-5 minutos (mais demorado)
- **clips.py**: ~1-2 minutos
- **games.py**: ~8 segundos
- **Total**: ~5-8 minutos para pipeline completo

---

## 🔧 Estrutura dos JSONs

Todos os arquivos seguem o padrão:
```json
{
  "data": [...],           // Dados principais
  "total_items": 1000,     // Total coletado
  "requested_items": 1000, // Total solicitado
  "success_rate": 95.0,    // Taxa de sucesso
  "config_used": {...}     // Configurações usadas
}
```

---

## 📝 Observações Importantes

### **Dependências**
- Ordem de execução **DEVE** ser respeitada
- `users.py` precisa de `streams.json`
- `videos.py` e `clips.py` precisam de `users.json`
- `games.py` precisa de todos os anteriores

### **Rate Limiting**
- Todos os scripts respeitam limites da API da Twitch
- Concorrência configurada para não sobrecarregar
- Timeouts configurados para evitar travamentos

### **Tratamento de Erros**
- Scripts continuam mesmo com falhas parciais
- Logs detalhados para debugging
- Dados salvos mesmo com coletas incompletas

---

## 🎯 Próximos Passos

Após a extração, os dados estão prontos para:
1. **Transformação**: Limpeza e estruturação
2. **Load**: Carregamento no banco de dados
3. **Análise**: Geração de insights e relatórios

---

**🎉 O módulo de extração é a base de todo o pipeline de analytics da Twitch!** 