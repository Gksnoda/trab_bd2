from django.shortcuts import render
from django.db import connection
from django.core.paginator import Paginator
from django.apps import apps
from datetime import date, timedelta
from .forms import ReportForm, traducoes_modelos
from django.db.models import Sum, Max, Count
from reports.models import User, Stream, Game, Video, Clip
import csv
import json
import pandas as pd
from django.http import HttpResponse
from django.db import connection
import io
from openpyxl import Workbook

def get_tabelas_e_campos(selected_tables=None):
    from django.apps import apps

    campos = []
    column_labels = {}
    tabelas = []

    campos_permitidos = {
        'users': ['display_name', 'broadcaster_type', 'id', 'description', 'created_at'],
        'streams': ['viewer_count', 'language', 'started_at', 'title', 'tag_ids'],
        'games': ['name'],
        'videos': ['title', 'url', 'view_count', 'duration', 'created_at', 'language'],
        'clips': ['title', 'url', 'view_count', 'duration', 'created_at'],
    }

    traducoes_campos = {
        'users': {
            'display_name': 'Nome do Streamer',
            'broadcaster_type': 'Tipo de Transmissor',
            'id': 'ID do Usuário',
            'description': 'Descrição',
            'created_at': 'Data de Criação',
        },
        'streams': {
            'viewer_count': 'Visualizações',
            'language': 'Idioma',
            'started_at': 'Início da Transmissão',
            'title': 'Título da Live',
            'tag_ids': 'Tags',
        },
        'games': {
            'name': 'Nome do Jogo',
        },
        'videos': {
            'title': 'Título do Vídeo',
            'url': 'Link do Vídeo',
            'view_count': 'Visualizações',
            'duration': 'Duração',
            'created_at': 'Data de Criação',
            'language': 'Idioma',
        },
        'clips': {
            'title': 'Título do Clipe',
            'url': 'Link do Clipe',
            'view_count': 'Visualizações',
            'duration': 'Duração',
            'created_at': 'Data de Criação',
        }
    }

    models = apps.get_app_config('reports').get_models()
    for model in models:
        nome_tabela = model._meta.db_table
        nome_modelo = model._meta.object_name
        nome_traduzido = traducoes_modelos.get(nome_tabela, nome_modelo)

        tabelas.append(nome_tabela)

        if selected_tables is None or nome_tabela in selected_tables:
            for field in model._meta.fields:
                nome_campo = field.get_attname_column()[1]
                if nome_campo not in campos_permitidos.get(nome_tabela, []):
                    continue
                chave = f"{nome_tabela}.{nome_campo}"
                traducao = traducoes_campos.get(nome_tabela, {}).get(nome_campo, nome_campo.title())
                label = f"{nome_traduzido}: {traducao}"
                campos.append({"value": chave, "label": label})
                column_labels[chave.replace(".", "_")] = label

    return tabelas, campos, traducoes_modelos, column_labels


traducoes_modelos = {
    'users': 'Streamers',
    'streams': 'Transmissões',
    'games': 'Jogos',
    'videos': 'Vídeos',
    'clips': 'Clipes'
}
def montar_query(dados):
    tabelas = dados.get("tables", [])
    if isinstance(tabelas, str):
        tabelas = [tabelas]

    fields_raw = dados.get("fields", [])
    colunas = []
    if isinstance(fields_raw, list):
        colunas = [col.strip() for col in fields_raw if col and col.strip()]
    elif isinstance(fields_raw, str):
        colunas = [fields_raw.strip()]

    group_by = dados.get("group_by")
    order_field = dados.get("order_field")

    campos_extras = [
        dados.get("filter_field"),
        order_field,
        group_by,
    ]

    busca_global = dados.get("busca_global", "")

    # 🔍 Inclui tabelas referenciadas em colunas, filtros ou busca
    todos_campos = campos_extras + colunas
    if isinstance(busca_global, str) and busca_global:
        for tabela in ["streams", "users", "games", "clips", "videos"]:
            for campo in ["name", "title", "language", "display_name"]:
                if f"{tabela}.{campo}" not in todos_campos:
                    todos_campos.append(f"{tabela}.{campo}")

    for campo in todos_campos:
        if not campo:
            continue
        for tabela in ["streams", "users", "games", "clips", "videos"]:
            if f"{tabela}." in campo and tabela not in tabelas:
                tabelas.append(tabela)

    where_clauses = []
    valores = []

    # Busca global
    if busca_global:
        filtros = []
        if 'users' in tabelas:
            filtros.append("users.display_name ILIKE %s")
            valores.append(f"%{busca_global}%")
        if 'games' in tabelas:
            filtros.append("games.name ILIKE %s")
            valores.append(f"%{busca_global}%")
        if 'streams' in tabelas:
            filtros.append("streams.language ILIKE %s")
            valores.append(f"%{busca_global}%")
        if 'videos' in tabelas:
            filtros.append("videos.title ILIKE %s")
            valores.append(f"%{busca_global}%")
        if 'clips' in tabelas:
            filtros.append("clips.title ILIKE %s")
            valores.append(f"%{busca_global}%")
        if filtros:
            where_clauses.append("(" + " OR ".join(filtros) + ")")

    # Filtro simples
    campo = dados.get("filter_field")
    operador = dados.get("filter_operator")
    valor = dados.get("filter_value")
    if campo and operador and valor:
        where_clauses.append(f"{campo} {operador} %s")
        valores.append(valor)

    # Filtro por data
    data_inicio = dados.get("data_inicio")
    data_fim = dados.get("data_fim")
    if data_inicio and data_fim:
        for tb in ["streams", "videos", "clips"]:
            if tb in tabelas:
                campo_data = "started_at" if tb == "streams" else "created_at"
                where_clauses.append(f"{tb}.{campo_data} BETWEEN %s AND %s")
                valores.extend([data_inicio, data_fim])
                break

    # Determina tabela base
    tabela_base = None
    for col in colunas:
        for tabela in ["streams", "clips", "videos", "users", "games"]:
            if f"{tabela}." in col:
                tabela_base = tabela
                break
        if tabela_base:
            break
    if not tabela_base:
        tabela_base = tabelas[0] if tabelas else "streams"
    if tabela_base not in tabelas:
        tabelas.append(tabela_base)

    tabelas_set = {t for t in set(tabelas) if t != tabela_base}
    joins = []

    def join(tipo, linha):
        comando = f"{tipo} {linha}"
        if comando not in joins:
            joins.append(comando)

    join_type = "LEFT JOIN"
    if dados.get("filter") in ["top_streamers", "jogos_populares"]:
        join_type = "INNER JOIN"

    if tabela_base == "users":
        if "streams" in tabelas_set:
            join(join_type, "streams ON users.id = streams.user_id")
        if "clips" in tabelas_set:
            join("LEFT JOIN", "clips ON users.id = clips.user_id")
    elif tabela_base == "streams":
        if "users" in tabelas_set:
            join(join_type, "users ON users.id = streams.user_id")
        if "games" in tabelas_set:
            join(join_type, "games ON games.id = streams.game_id")
        if "videos" in tabelas_set:
            join("LEFT JOIN", "videos ON videos.id = streams.id")
        if "clips" in tabelas_set:
            join("LEFT JOIN", "clips ON clips.id = streams.id")
    elif tabela_base == "games":
        if "streams" in tabelas_set:
            join(join_type, "streams ON games.id = streams.game_id")
    elif tabela_base == "videos":
        if "streams" in tabelas_set:
            join("LEFT JOIN", "streams ON videos.id = streams.id")
    elif tabela_base == "clips":
        if "users" in tabelas_set:
            join("LEFT JOIN", "users ON users.id = clips.user_id")
        if "videos" in tabelas_set:
            join("LEFT JOIN", "videos ON clips.id = videos.id")

    from_clause = f"{tabela_base} {' '.join(joins)}"
    select_clause = ", ".join([
        col if " AS " in col.upper()
        else f"{col} AS \"{col.replace('.', '_')}\""
        for col in colunas
    ])
    for col in colunas:
        if " AS " not in col.upper() and "(" not in col:
            where_clauses.append(f"{col} IS NOT NULL")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    group_clause = f"GROUP BY {group_by}" if group_by else ""
    order_clause = f"ORDER BY {order_field} {dados.get('order_type', 'ASC')}" if order_field else ""

    query = f"SELECT {select_clause} FROM {from_clause} {where_sql} {group_clause} {order_clause}"
    print("QUERY FINAL:", query)
    return query, valores



def builder(request):
    get_data = request.GET.copy()
    filtro_rapido = get_data.get("filter")
    busca_global = get_data.get("busca_global", "").strip()

    if filtro_rapido == "top_streamers":
        get_data.setlist("tables", ["users", "streams"])
        get_data.setlist("fields", ["users.display_name", "streams.viewer_count"])
        get_data["order_field"] = "streams.viewer_count"
        get_data["order_type"] = "DESC"

    elif filtro_rapido == "jogos_populares":
        get_data.setlist("tables", ["streams", "games"])
        get_data.setlist("fields", [
            "games.name",
            "SUM(streams.viewer_count) AS streams_viewer_count"
        ])
        get_data["group_by"] = "games.name"
        get_data["order_field"] = "streams_viewer_count"
        get_data["order_type"] = "DESC"

    elif filtro_rapido == "brpt":
        get_data.setlist("tables", ["streams", "users"])
        get_data.setlist("fields", ["users.display_name", "streams.language", "streams.viewer_count"])
        get_data["filter_field"] = "streams.language"
        get_data["filter_operator"] = "="
        get_data["filter_value"] = "pt"
        get_data["order_field"] = "streams.viewer_count"
        get_data["order_type"] = "DESC"

    elif filtro_rapido == "hoje":
        hoje = date.today().isoformat()
        get_data.setlist("tables", ["streams", "users"])
        get_data.setlist("fields", ["users.display_name", "streams.started_at", "streams.viewer_count"])
        get_data["data_inicio"] = hoje
        get_data["data_fim"] = hoje

    elif filtro_rapido == "semana":
        hoje = date.today()
        inicio = hoje - timedelta(days=7)
        get_data.setlist("tables", ["streams", "users"])
        get_data.setlist("fields", ["users.display_name", "streams.started_at", "streams.viewer_count"])
        get_data["data_inicio"] = inicio.isoformat()
        get_data["data_fim"] = hoje.isoformat()

    elif busca_global:
        if not get_data.getlist("tables"):
            get_data.setlist("tables", ["users", "streams", "games"])
        if not get_data.getlist("fields"):
            get_data.setlist("fields", [
                "users.display_name",
                "streams.viewer_count",
                "streams.language",
                "games.name"
            ])

    forcar_gerar_relatorio = bool(filtro_rapido or busca_global)
    selected_tables = get_data.getlist("tables") if get_data else []
    tabelas, campos, traducoes_modelos, column_labels = get_tabelas_e_campos(selected_tables)

    form = ReportForm(get_data or None, initial={
        "tables": selected_tables,
        "fields": get_data.getlist("fields"),
    })

    models = apps.get_app_config('reports').get_models()
    form.fields["tables"].choices = [
        (model._meta.db_table, traducoes_modelos.get(model._meta.db_table, model._meta.object_name))
        for model in models
    ]
    if selected_tables:
        form.fields["fields"].choices = [(f["value"], f["label"]) for f in campos]
        form.fields["filter_field"].choices = [(f["value"], f["label"]) for f in campos]
        form.fields["order_field"].choices = [(f["value"], f["label"]) for f in campos]
    else:
        form.fields["fields"].choices = []
        form.fields["filter_field"].choices = []
        form.fields["order_field"].choices = []

    results = []
    preview_query = ""
    if form.is_valid() or forcar_gerar_relatorio:
        data = form.cleaned_data if form.is_valid() else get_data
        if filtro_rapido == "jogos_populares":
            data["fields"] = [
                "games.name",
                "SUM(streams.viewer_count) AS streams_viewer_count"
            ]
        if filtro_rapido:
            data["filter"] = filtro_rapido
        query, valores = montar_query(data)
        preview_query = query

        if query:
            with connection.cursor() as cursor:
                cursor.execute(query, valores)
                colnames = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                results = [dict(zip(colnames, row)) for row in rows]

    paginator = Paginator(results, 10)
    page_number = request.GET.get("page")
    results_paginated = paginator.get_page(page_number)
    column_labels["games_name"] = "Jogo"
    column_labels["streams_viewer_count"] = "Visualizações"

    # Métricas
    total_streamers = 0
    total_views = 0
    jogo_mais_popular = "N/A"
    idioma_mais_falado = "N/A"
    ultima_atualizacao = "N/A"
    data_ultima_stream = None

    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM users")
        total_streamers = cursor.fetchone()[0]

        cursor.execute("SELECT SUM(viewer_count) FROM streams")
        total_views = cursor.fetchone()[0] or 0

        cursor.execute("""
            SELECT g.name, SUM(s.viewer_count) AS total 
            FROM games g 
            JOIN streams s ON g.id = s.game_id 
            GROUP BY g.name 
            ORDER BY total DESC 
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row:
            jogo_mais_popular = row[0]

        cursor.execute("""
            SELECT language, COUNT(*) 
            FROM streams 
            GROUP BY language 
            ORDER BY COUNT(*) DESC 
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row:
            idioma_mais_falado = row[0]

        cursor.execute("SELECT MAX(started_at) FROM streams")
        row = cursor.fetchone()
        if row and row[0]:
            ultima_atualizacao = row[0].strftime("%d/%m/%Y %H:%M")

        if 'streams' in selected_tables:
            data_ultima_stream = Stream.objects.aggregate(Max('started_at'))['started_at__max']

    return render(request, "reports/builder.html", {
        "form": form,
        "results": results_paginated,
        "preview_query": preview_query,
        "selected_tables": selected_tables,
        "column_labels": column_labels,
        "total_streamers": total_streamers,
        "total_views": total_views,
        "jogo_mais_popular": jogo_mais_popular,
        "idioma_mais_falado": idioma_mais_falado,
        "ultima_atualizacao": ultima_atualizacao,
        "data_ultima_stream": data_ultima_stream,
    })
    
def fetch_data_for_export():
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT u.display_name, s.viewer_count, s.language, g.name AS game
            FROM users u
            LEFT JOIN streams s ON u.id = s.user_id
            LEFT JOIN games g ON g.id = s.game_id
            LIMIT 100
        """)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)


def export_excel(request):
    df = fetch_data_for_export()
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename="relatorio.xlsx"'
    df.to_excel(response, index=False)
    return response


def export_csv(request):
    df = fetch_data_for_export()
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="relatorio.csv"'
    df.to_csv(path_or_buf=response, index=False)
    return response


def export_json(request):
    df = fetch_data_for_export()
    response = HttpResponse(content_type='application/json')
    response['Content-Disposition'] = 'attachment; filename="relatorio.json"'
    response.write(df.to_json(orient='records', force_ascii=False))
    return response

def export_data(request, format):
    get_data = request.GET.copy()
    _, _, _, column_labels = get_tabelas_e_campos(get_data.getlist("tables"))
    query, valores = montar_query(get_data)

    with connection.cursor() as cursor:
        cursor.execute(query, valores)
        colnames = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        data = [dict(zip(colnames, row)) for row in rows]

    if format == "json":
        return HttpResponse(
            json.dumps(data, ensure_ascii=False, indent=2),
            content_type="application/json",
            headers={"Content-Disposition": 'attachment; filename="relatorio.json"'},
        )

    elif format == "csv":
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="relatorio.csv"'
        writer = csv.writer(response)
        writer.writerow([column_labels.get(col, col) for col in colnames])
        for row in data:
            writer.writerow([row[col] for col in colnames])
        return response

    elif format == "excel":
        wb = Workbook()
        ws = wb.active
        ws.append([column_labels.get(col, col) for col in colnames])
        for row in data:
            ws.append([row[col] for col in colnames])
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        response = HttpResponse(
            output.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="relatorio.xlsx"'},
        )
        return response

    return HttpResponse("Formato inválido", status=400)