# PLANO DO PROJETO - BANCO DE DADOS II
## Relatórios Ad Hoc com API do Twitch

### ✅ Etapa 1: API de Dados (CONCLUÍDA)
- [x] Escolher API do Twitch
- [x] Configurar credenciais no .env
- [x] Definir público-alvo e motivação

---

### 📋 Etapa 2: Engenharia Reversa (EM ANDAMENTO)

#### 2.1 Análise dos Dados da API Twitch
- [x] **Estudar endpoints disponíveis (CONFIRMADOS NA DOCUMENTAÇÃO):**
  - **Users**: `GET /users` - informações de streamers/usuários
  - **Games**: `GET /games` e `GET /games/top` - dados de jogos/categorias  
  - **Streams**: `GET /streams` - transmissões ao vivo atuais
  - **Videos**: `GET /videos` - vídeos salvos (VODs, highlights, uploads)
  - **Clips**: `GET /clips` e `POST /clips` - clipes criados pelos usuários
  - **Channel Info**: `GET /channels` - informações detalhadas dos canais

#### 2.2 Mapeamento para Modelo Relacional
- [x] **Identificar entidades principais (baseadas nos endpoints da API):**
  - **User** - streamers/criadores de conteúdo
    - Campos: id, login, display_name, description, profile_image_url, view_count, created_at, broadcaster_type
  - **Game** - jogos/categorias
    - Campos: id, name, box_art_url 
  - **Stream** - transmissões ao vivo
    - Campos: id, user_id, game_id, title, viewer_count, started_at, language, thumbnail_url
  - **Video** - vídeos salvos (VODs/highlights)
    - Campos: id, user_id, title, description, created_at, published_at, url, thumbnail_url, view_count, duration, language, type
  - **Clip** - clipes criados por usuários
    - Campos: id, broadcaster_id, creator_id, video_id, game_id, title, view_count, created_at, thumbnail_url, duration
    
- [x] **Relacionamentos identificados:**
  - User (1) ---- hosts ----> (0,n) Stream
  - User (1,1) ---- creates -----> (0,n) Clip  
  - User (1) ---- uploads -----> (0,n) Video
  - Game (1,1) ---- plays -----> (0,n) Stream
  - Game (1,1) ---- appears -----> (0,n) Clip
  - Video (1,1) ---- clipped -----> (0,n) Clip

#### 2.3 Criar Documentação
- [x] **Modelo Entidade-Relacionamento (MER)**
- [x] **Modelo Relacional**
- [x] **Dicionário de Dados inicial**

#### 2.4 Definir Público-Alvo Específico
- [x] **Exemplos de usuários finais:**
  - Analistas de marketing digital
  - Criadores de conteúdo
  - Empresas de gaming
  - Pesquisadores de mídia digital

---

### 🔧 Etapa 3: Carga no Banco (ETL)

#### 3.1 Configuração do Ambiente
- [ ] **Definir SGBD:** PostgreSQL, MySQL ou SQLite
- [ ] **Instalar dependências:**
  ```bash
  uv add requests
  uv add sqlalchemy  # ORM
  uv add pandas      # Manipulação de dados
  uv add python-dotenv
  ```

#### 3.2 Implementar ETL
- [ ] **Extração (Extract):**
  - Conectar com API Twitch
  - Buscar dados de múltiplos endpoints
  - Implementar paginação
  - Usar custom logger para monitoramento

- [ ] **Transformação (Transform):**
  - Limpeza de dados nulos
  - Padronização de formatos
  - Validação de tipos
  - Tratamento de duplicatas

- [ ] **Carga (Load):**
  - Criar script de criação do banco
  - Popular tabelas respeitando relacionamentos
  - Implementar verificações de integridade

#### 3.3 Aspectos de Segurança e Performance
- [ ] **Criar usuários e perfis:**
  - Usuário administrador
  - Usuário de consulta (read-only)
  - Usuário da aplicação

- [ ] **Implementar índices:**
  - Índices em chaves primárias
  - Índices em campos de busca frequente
  - Índices compostos para relatórios

- [ ] **Documentar quantidades carregadas**

---

### ⚡ Etapa 4: Performance do Banco (JMeter)

#### 4.1 Preparação
- [ ] **Instalar JMeter**
- [ ] **Configurar conexão JDBC com o banco**
- [ ] **Escolher consulta custosa para teste**

#### 4.2 Testes de Latência
- [ ] **Teste 1 - Usuários fixos, requisições crescentes:**
  - Fixar threads (usuários)
  - Aumentar loop count até erro
  - Gerar gráfico: Latência x Número de Requisições

- [ ] **Teste 2 - Requisições fixas, usuários crescentes:**
  - Fixar loop count (requisições)
  - Aumentar threads até erro
  - Gerar gráfico: Latência x Número de Usuários

#### 4.3 Análise de Resultados
- [ ] **Documentar arquitetura da máquina de teste**
- [ ] **Interpretar limites do sistema**
- [ ] **Gerar relatório com discussão dos resultados**

---

### 🎨 Etapa 5: Prototipação e Modelagem da Aplicação

#### 5.1 Definição da Arquitetura
- [ ] **Escolher stack tecnológico:**
  - Backend: Python (Flask/FastAPI/Django)
  - Frontend: React, Vue.js ou HTML simples
  - ORM: SQLAlchemy

- [ ] **Modelagem MVC:**
  - Model: Entidades do banco
  - View: Interface do usuário
  - Controller: Lógica de negócio

#### 5.2 Prototipação do Relatório Ad Hoc
- [ ] **Interface para seleção:**
  - Escolha de tabelas
  - Seleção de campos
  - Filtros simples e compostos
  - Operadores lógicos (AND/OR)
  - Funções de agregação (COUNT, SUM, AVG)
  - Ordenação
  - Exportação (CSV, JSON)

- [ ] **Casos de uso específicos para Twitch:**
  - "Top streamers por categoria de jogo"
  - "Análise de engagement por período"
  - "Comparativo de performance entre jogos"

---

### 💻 Etapa 6: Desenvolvimento da Aplicação

#### 6.1 Backend (Consultas Dinâmicas)
- [ ] **Implementar ORM models**
- [ ] **Criar sistema de consultas dinâmicas:**
  - Query builder baseado em parâmetros
  - Validação de segurança (SQL injection)
  - Otimização de consultas

- [ ] **APIs REST:**
  - GET /tables - listar tabelas disponíveis
  - GET /fields/{table} - campos de uma tabela
  - POST /query - executar consulta dinâmica
  - GET /export/{format} - exportar resultados

#### 6.2 Frontend
- [ ] **Interface de construção de relatórios:**
  - Seletor de tabelas com preview de relacionamentos
  - Multi-select para campos
  - Interface para filtros complexos
  - Preview de consulta SQL (opcional)
  - Visualização de resultados

#### 6.3 Funcionalidades Extras (Pontos Bonus)
- [ ] **Gráficos dinâmicos** (+0.5pt)
- [ ] **Deploy distribuído** - App e BD em máquinas diferentes (+0.5pt)

---

### 📊 Funcionalidades Específicas para API Twitch

#### Relatórios Sugeridos (baseados nos dados reais disponíveis):
1. **Análise de Streamers:**
   - Top streamers por view_count total
   - Streamers mais ativos por número de streams
   - Média de viewers por streamer e categoria
   - Diversidade de jogos por streamer (broadcaster_type analysis)

2. **Análise de Jogos:**
   - Top games por número de streams ativas
   - Jogos com maior média de viewers
   - Evolução temporal da popularidade dos jogos
   - Jogos com mais clips criados

3. **Análise de Conteúdo:**
   - Performance de clips por criador (view_count vs duration)
   - Clips mais populares por jogo
   - Análise de duração ideal de clips por categoria
   - VODs vs Highlights: análise de engajamento
   - Streams por idioma e sua popularidade

4. **Relatórios de Engagement:**
   - Relação entre duração de stream e viewers
   - Melhor horário para streaming por jogo
   - Análise de thumbnails mais eficazes
   - Correlação entre clips e crescimento do canal

---

### 🗓️ Cronograma Sugerido

#### Semana 1-2: Etapas 1-2
- Finalizar modelagem do banco
- Criar documentação completa

#### Semana 3-4: Etapa 3
- Implementar ETL
- Popular banco de dados
- Configurar segurança e índices

#### Semana 5: Etapa 4
- Executar testes de performance
- Gerar relatórios de análise

#### Semana 6-7: Etapas 5-6
- Desenvolver aplicação
- Implementar relatórios ad hoc
- Testes finais

#### Semana 8: Apresentação
- Preparar slides
- Demonstração ao vivo

---

### 📝 Entregas por Etapa

#### Até 15/06 (Etapas 1-4):
- [ ] Relatório compilado com todas as etapas
- [ ] MER e Modelo Relacional
- [ ] Dicionário de dados
- [ ] Scripts de ETL
- [ ] Prints com count das tabelas
- [ ] Relatório de performance (JMeter)
- [ ] Link do GitHub

#### 01-03/07 (Etapas 5-6):
- [ ] Aplicação funcionando
- [ ] Apresentação oral
- [ ] Demonstração ao vivo dos relatórios ad hoc
- [ ] Código final no GitHub

---

### ⚠️ Pontos de Atenção

1. **Usar sempre UV para gerenciamento:**
   ```bash
   uv add <package>
   uv run <file.py>
   ```

2. **Usar custom logger em todo o projeto**

3. **Consultas DEVEM ser dinâmicas no backend**

4. **Obrigatório usar ORM**

5. **Dados devem vir exclusivamente da API**

6. **Participar de todas as consultorias (-1pt se não participar)**

7. **JMeter é crítico - começar cedo**

---

### 🎯 Próximos Passos Imediatos

1. **Estudar endpoints da API Twitch**
2. **Criar modelo conceitual das entidades**
3. **Definir relacionamentos entre tabelas**
4. **Escolher SGBD e configurar ambiente**
5. **Começar implementação do ETL** 