:wrench: ETAPA 2: Configuração (2º passo)
O que vamos fazer:
Criar classe para conexão com a API Twitch
Criar classe para conexão com o banco PostgreSQL
Criar configurações centralizadas
Testar se todas as conexões estão funcionando
:inbox_tray: ETAPA 3: Extract (Extração) (3º passo)
O que vamos fazer:
Criar scripts para buscar cada tipo de dados:
extract_users.py - Buscar streamers/usuários
extract_games.py - Buscar jogos/categorias
extract_streams.py - Buscar streams ao vivo
extract_videos.py - Buscar vídeos salvos
extract_clips.py - Buscar clips
Implementar paginação (API Twitch limita resultados)
Salvar dados brutos em arquivos temporários
:arrows_counterclockwise: ETAPA 4: Transform (Transformação) (4º passo)
O que vamos fazer:
Limpar dados nulos/inválidos
Padronizar formatos de data/hora
Validar tipos de dados
Remover duplicatas
Preparar dados para inserção no banco
:floppy_disk: ETAPA 5: Load (Carga) (5º passo)
O que vamos fazer:
Criar tabelas no PostgreSQL (baseadas no MER)
Inserir dados na ordem correta:
users (sem dependências)
games (sem dependências)
streams (depende de users e games)
videos (depende de users)
clips (depende de users, games e videos)
:dart: ETAPA 6: Orquestração (6º passo)
O que vamos fazer:
Criar script principal que executa tudo em sequência
Adicionar logs detalhados
Implementar tratamento de erros
Criar relatório final de carga