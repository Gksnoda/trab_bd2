# ETL - Twitch Analytics

Estrutura organizada para o processo de ETL (Extract, Transform, Load) dos dados da API Twitch.

## 📁 Estrutura de Diretórios

```
etl/
├── config/          # Configurações e conexões
├── extract/         # Scripts de extração da API Twitch
├── transform/       # Scripts de transformação dos dados
├── load/           # Scripts de carga no banco PostgreSQL
├── utils/          # Utilitários e funções auxiliares
├── testes/         # Scripts de teste de conexões
└── README.md       # Esta documentação
```

## 🎯 Finalidade de Cada Pasta

### **config/**
- Configurações de conexão com API Twitch
- Configurações de conexão com PostgreSQL
- Constantes e parâmetros do ETL

### **extract/**
- Scripts para buscar dados dos endpoints da API:
  - `extract_users.py` - Dados de streamers/usuários
  - `extract_games.py` - Dados de jogos/categorias
  - `extract_streams.py` - Dados de streams ao vivo
  - `extract_videos.py` - Dados de vídeos salvos
  - `extract_clips.py` - Dados de clips

### **transform/**
- Scripts para limpeza e padronização:
  - Remoção de dados nulos/inválidos
  - Padronização de formatos de data/hora
  - Validação de tipos de dados
  - Tratamento de duplicatas

### **load/**
- Scripts para criação de tabelas e carga:
  - Criação das tabelas do MER
  - Inserção de dados respeitando relacionamentos
  - Verificações de integridade

### **utils/**
- Funções auxiliares reutilizáveis
- Helpers para API e banco de dados
- Utilitários de logging e validação

### **testes/**
- Scripts de teste de conectividade
- Validação de credenciais
- Testes de funcionalidade