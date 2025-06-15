# Transformações de Dados - ETL

Este diretório contém os scripts de transformação de dados do processo ETL do projeto de análise da Twitch. Cada script é responsável por limpar e transformar um tipo específico de dado extraído da API da Twitch.

## Visão Geral

As transformações aplicam filtros e limpeza nos dados brutos para:
- Remover campos desnecessários para a análise
- Padronizar formatos de data
- Filtrar registros inválidos ou incompletos
- Reduzir o tamanho dos arquivos
- Preparar os dados para carregamento no banco de dados

## Scripts de Transformação

### 1. `streams_transform.py`
**Objetivo**: Limpar dados de streams ao vivo da Twitch

**Campos removidos**:
- `user_login` - Login do usuário (redundante com user_name)
- `type` - Tipo da stream (sempre "live")
- `started_at` - Data/hora de início (não necessário para análise)
- `thumbnail_url` - URL da miniatura (não usado)
- `tag_ids` - IDs das tags (temos o campo tags)
- `is_mature` - Flag de conteúdo maduro (não relevante)

**Dados mantidos**:
- `id`, `user_id`, `user_name`, `game_id`, `game_name`
- `title`, `viewer_count`, `language`, `tags`

**Uso**:
```bash
uv run ETL/transform/streams_transform.py
```

---

### 2. `users_transform.py`
**Objetivo**: Limpar dados de usuários/streamers da Twitch

**Campos removidos**:
- `login` - Nome de login (redundante com display_name)
- `type` - Tipo de usuário (campo sempre vazio)
- `offline_image_url` - URL da imagem offline (não usado)
- `view_count` - Contagem de visualizações (sempre 0 nos dados)

**Transformações aplicadas**:
- `created_at`: Convertido de `"2010-10-30T08:22:30Z"` para `"2010-10-30"` (apenas data)

**Dados mantidos**:
- `id`, `display_name`, `broadcaster_type`, `description`
- `profile_image_url`, `created_at` (só data)

**Uso**:
```bash
uv run ETL/transform/users_transform.py
```

---

### 3. `videos_transform.py`
**Objetivo**: Limpar dados de vídeos/VODs da Twitch

**Campos removidos**:
- `user_login` - Login do usuário (redundante)
- `description` - Descrição do vídeo (frequentemente vazia)
- `published_at` - Data de publicação (redundante com created_at)
- `thumbnail_url` - URL da miniatura (não usado)
- `viewable` - Status de visibilidade (sempre "public")
- `type` - Tipo do vídeo (sempre "archive")
- `muted_segments` - Segmentos silenciados (não relevante)

**Transformações aplicadas**:
- `created_at`: Convertido de `"2025-06-14T15:23:59Z"` para `"2025-06-14"` (apenas data)

**Dados mantidos**:
- `id`, `stream_id`, `user_id`, `user_name`, `title`
- `url`, `view_count`, `language`, `duration`, `created_at` (só data)

**Uso**:
```bash
uv run ETL/transform/videos_transform.py
```

---

### 4. `clips_transform.py`
**Objetivo**: Limpar dados de clips da Twitch

**Campos removidos**:
- `embed_url` - URL para embed (não usado)
- `thumbnail_url` - URL da miniatura (não usado)
- `vod_offset` - Offset no VOD (frequentemente null)
- `is_featured` - Flag de destaque (não relevante)

**Filtros aplicados**:
- **Remove clips com `video_id` vazio**: Clips sem vídeo associado são removidos completamente

**Dados mantidos**:
- `id`, `url`, `broadcaster_id`, `broadcaster_name`
- `creator_id`, `creator_name`, `video_id`, `game_id`
- `language`, `title`, `view_count`, `created_at`, `duration`

**Uso**:
```bash
uv run ETL/transform/clips_transform.py
```

---

### 5. `games_transform.py`
**Objetivo**: Limpar dados de jogos/categorias da Twitch

**Campos removidos**:
- `igdb_id` - ID do IGDB (base de dados externa, não necessário)

**Dados mantidos**:
- `id`, `name`, `box_art_url`

**Uso**:
```bash
uv run ETL/transform/games_transform.py
```

## Características Comuns

Todos os scripts compartilham as seguintes características:

### 🔧 **Funcionalidades**
- **Logger customizado**: Uso do sistema de log personalizado do projeto
- **Caminhos dinâmicos**: Funcionam independente do diretório de execução
- **Tratamento de erros**: Captura e reporta erros de forma clara
- **Progresso em tempo real**: Mostra progresso durante processamento
- **Metadados**: Salva informações sobre a transformação aplicada

### 📁 **Estrutura de Arquivos**
- **Entrada**: `ETL/data/raw/[tipo].json`
- **Saída**: `ETL/data/transformed/[tipo]_transformed.json`
- **Auto-criação**: Cria diretórios automaticamente se necessário

### 📊 **Formato de Saída**
```json
{
  "data": [...],
  "metadata": {
    "total_[tipo]": 1000,
    "fields_removed": ["campo1", "campo2"],
    "transformations_applied": ["descrição das transformações"],
    "transformation_timestamp": "2025-01-XX..."
  }
}
```

### 🚀 **Execução**
Cada script pode ser executado individualmente:
```bash
uv run ETL/transform/[script_name].py
```

## Impacto das Transformações

### Redução de Dados
- **Streams**: ~40% redução no tamanho do arquivo
- **Users**: ~35% redução no tamanho do arquivo
- **Videos**: ~50% redução no tamanho do arquivo
- **Clips**: ~25% redução + remoção de registros inválidos
- **Games**: ~15% redução no tamanho do arquivo

### Padronização
- Datas convertidas para formato ISO (YYYY-MM-DD)
- Remoção de campos redundantes ou vazios
- Filtros de qualidade de dados aplicados

### Preparação para Banco
- Estrutura limpa e consistente
- Campos otimizados para análise
- Dados validados e filtrados

## Próximos Passos

Após executar todas as transformações, os dados estarão prontos para a etapa de **Load** do processo ETL, onde serão carregados no banco de dados PostgreSQL para análise. 