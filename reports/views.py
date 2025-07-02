from django.shortcuts import render
from django.core.paginator import Paginator
from .forms import ReportForm, traducoes_modelos
from django.db.models import Count, Sum, Max, Q, Min, Count, Avg
from reports.models import User, Stream, Game, Video, Clip
import csv
import json
import pandas as pd
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from io import BytesIO
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse, HttpResponse
from django.db.models import Q
from django import forms

def get_tabelas_e_campos(selected_tables=None):
    from django.apps import apps

    campos = []
    column_labels = {}
    tabelas = []

    campos_permitidos = {
        'users': ['display_name', 'broadcaster_type', 'id', 'description', 'created_at'],
        'streams': ['viewer_count', 'language', 'started_at', 'title', 'tags'],
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
            'tags': 'Tags',
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
                chave = f"{nome_tabela}__{nome_campo}"
                traducao = traducoes_campos.get(nome_tabela, {}).get(nome_campo, nome_campo.title())
                label = f"{nome_traduzido}: {traducao}"
                campos.append({"value": chave, "label": label})
                column_labels[chave] = label

    return tabelas, campos, traducoes_modelos, column_labels

traducoes_modelos = {
    'users': 'Streamers',
    'streams': 'Transmissões',
    'games': 'Jogos',
    'videos': 'Vídeos',
    'clips': 'Clipes'
}



def get_lookup(operator):
    return {
        '=': '',
        '!=': '',
        '<': '__lt',
        '<=': '__lte',
        '>': '__gt',
        '>=': '__gte',
        'LIKE': '__icontains'
    }.get(operator, '')

def montar_queryset(dados):
    print("CHAVE FILTER NO INÍCIO:", dados.get("filter"))
    print("DADOS RECEBIDOS:", dados)

    # Função auxiliar para montar os nomes dos campos do queryset
    def mapear_campo(f):
        if f.startswith("users__"):
            return f.replace("users__", "user__")
        if f.startswith("streams__"):
            return f.replace("streams__", "")
        if f.startswith("games__"):
            return f.replace("games__", "game__")
        if f.startswith("clips__"):
            return f.replace("clips__", "")
        if f.startswith("videos__"):
            return f.replace("videos__", "video__")
        return f

    def get_lookup(op):
        return {
            "=": "",
            "!=": "",
            "<": "__lt",
            "<=": "__lte",
            ">": "__gt",
            ">=": "__gte",
            "LIKE": "__icontains"
        }.get(op, "")

    tabelas = dados.get("tables", [])
    campos = dados.get("fields", [])
    order_field = dados.get("order_field")
    order_type = dados.get("order_type", "ASC").upper()

    # Garante que tabelas e campos são listas
    if isinstance(tabelas, str):
        tabelas = [tabelas]
    if isinstance(campos, str):
        campos = [campos]

    filtro_rapido = dados.get("filter")

    # ======================
    # FILTROS RÁPIDOS
    # ======================
    if filtro_rapido == "top_streamers":
        qs = Stream.objects.select_related('user').values(
            'user__id', 'user__display_name', 'user__broadcaster_type'
        ).annotate(total_views=Sum('viewer_count')).order_by('-total_views')
        return [
            {
                'users__id': row['user__id'],
                'users__display_name': row['user__display_name'],
                'users__broadcaster_type': row['user__broadcaster_type'],
                'streams__viewer_count': row['total_views'],
            }
            for row in qs
        ]

    if filtro_rapido == "jogos_populares":
        qs = Stream.objects.select_related('game').values(
            'game__id', 'game__name'
        ).annotate(total_views=Sum('viewer_count')).order_by('-total_views')
        return [
            {
                'games__id': row['game__id'],
                'games__name': row['game__name'],
                'streams__viewer_count': row['total_views'],
            }
            for row in qs
        ]

    if filtro_rapido == "brpt":
        idiomas = ["pt", "pt-br", "br"]
        qs = (
            Stream.objects
            .select_related('user')
            .filter(language__in=idiomas)
            .values('user__id', 'user__display_name', 'user__broadcaster_type', 'language')
            .annotate(total_views=Sum('viewer_count'))
            .order_by('-total_views')
        )
        return [
            {
                'users__id': row['user__id'],
                'users__display_name': row['user__display_name'],
                'users__broadcaster_type': row['user__broadcaster_type'],
                'streams__language': row['language'],
                'streams__viewer_count': row['total_views'],
            }
            for row in qs
        ]

    # Agrupa os campos por tabela
    campos_por_tabela = {}
    for campo in campos:
        if "__" in campo:
            tabela, atributo = campo.split("__", 1)
        else:
            tabela, atributo = tabelas[0], campo
        campos_por_tabela.setdefault(tabela, []).append(atributo)

    # ======================
    # FILTROS AVANÇADOS
    # ======================
    filter_field1 = dados.get("filter_field1")
    filter_operator1 = dados.get("filter_operator1")
    filter_value1 = dados.get("filter_value1")
    filter_field2 = dados.get("filter_field2")
    filter_operator2 = dados.get("filter_operator2")
    filter_value2 = dados.get("filter_value2")
    logical_operator = dados.get("logical_operator")  # 'AND' ou 'OR'

    filtros1 = Q()
    filtros2 = Q()
    if filter_field1 and filter_operator1 and filter_value1:
        field1 = mapear_campo(filter_field1)
        lookup1 = get_lookup(filter_operator1)
        filtro_key1 = field1 + lookup1
        if filter_operator1 == '!=':
            filtros1 = ~Q(**{filtro_key1: filter_value1})
        else:
            filtros1 = Q(**{filtro_key1: filter_value1})

    if filter_field2 and filter_operator2 and filter_value2:
        field2 = mapear_campo(filter_field2)
        lookup2 = get_lookup(filter_operator2)
        filtro_key2 = field2 + lookup2
        if filter_operator2 == '!=':
            filtros2 = ~Q(**{filtro_key2: filter_value2})
        else:
            filtros2 = Q(**{filtro_key2: filter_value2})

    filtros = Q()
    if filter_field1 and filter_field2 and logical_operator in ["AND", "OR"]:
        if logical_operator == "AND":
            filtros = filtros1 & filtros2
        else:
            filtros = filtros1 | filtros2
    elif filter_field1:
        filtros = filtros1
    elif filter_field2:
        filtros = filtros2

    # ======================
    # BUSCA GLOBAL
    # ======================
    busca_global = dados.get("busca_global", "").strip()
    filtro_busca_global = Q()
    if busca_global:
        filtro_busca_global |= Q(user__display_name__icontains=busca_global)
        filtro_busca_global |= Q(game__name__icontains=busca_global)
        filtro_busca_global |= Q(language__icontains=busca_global)
        filtro_busca_global |= Q(title__icontains=busca_global)

    # ======================
    # JOIN Streams (users, games, etc)
    # ======================
    if "streams" in campos_por_tabela:
        qs = Stream.objects.select_related('user', 'game')

        # Junta filtros avançados e busca global
        filtro_total = Q()
        if filtros:
            filtro_total &= filtros
        if filtro_busca_global:
            filtro_total &= filtro_busca_global
        if filtro_total:
            qs = qs.filter(filtro_total)

        # Seleciona os campos para values()
        values_fields = []
        campo_to_rowkey = {}
        for c in campos:
            if c.startswith("users__"):
                row_key = f"user__{c.split('__', 1)[1]}"
            elif c.startswith("games__"):
                row_key = f"game__{c.split('__', 1)[1]}"
            elif c.startswith("streams__"):
                row_key = c.split('__', 1)[1]
            else:
                row_key = c
            values_fields.append(row_key)
            campo_to_rowkey[c] = row_key
        qs = qs.values(*values_fields)

        # AGREGAÇÃO (COUNT, SUM, etc)
        aggregation_function = dados.get("aggregation_function")
        aggregation_field = dados.get("aggregation_field")
        if aggregation_function and aggregation_field:
            campo_agg = aggregation_field.split('__', 1)[-1] if '__' in aggregation_field else aggregation_field
            func_map = {
                "COUNT": Count,
                "SUM": Sum,
                "AVG": Avg,
                "MAX": Max,
                "MIN": Min,
            }
            func = func_map.get(aggregation_function.upper())
            if func:
                qs_agg = qs.aggregate(resultado=func(campo_agg))
                return [{
                    "Agregação": f"{aggregation_function.upper()}({campo_agg})",
                    "Resultado": qs_agg["resultado"]
                }]

        # ORDENAÇÃO
        if order_field:
            if order_field.startswith("users__"):
                qs = qs.order_by(
                    f"user__{order_field.split('__', 1)[1]}"
                    if order_type == "ASC" else
                    f"-user__{order_field.split('__', 1)[1]}"
                )
            elif order_field.startswith("games__"):
                qs = qs.order_by(
                    f"game__{order_field.split('__', 1)[1]}"
                    if order_type == "ASC" else
                    f"-game__{order_field.split('__', 1)[1]}"
                )
            elif order_field.startswith("streams__"):
                qs = qs.order_by(
                    f"{order_field.split('__', 1)[1]}"
                    if order_type == "ASC" else
                    f"-{order_field.split('__', 1)[1]}"
                )
            else:
                qs = qs.order_by(order_field if order_type == "ASC" else f"-{order_field}")

        resultado_temp = [
            {campo: row.get(campo_to_rowkey[campo], "") for campo in campos}
            for row in qs
        ]
        print("LINHAS DO JOIN (debug):", resultado_temp[:5])
        return resultado_temp

    # ======================
    # Relatórios de outras tabelas (exemplo para Clips)
    # ======================
    if "clips" in campos_por_tabela:
        qs = Clip.objects.select_related('user', 'video', 'game')
        filtro_total = Q()
        if filtros:
            filtro_total &= filtros
        if filtro_busca_global:
            filtro_total &= filtro_busca_global
        if filtro_total:
            qs = qs.filter(filtro_total)
        values_fields = []
        for c in campos:
            if c.startswith("users__"):
                values_fields.append(f"user__{c.split('__', 1)[1]}")
            elif c.startswith("games__"):
                values_fields.append(f"game__{c.split('__', 1)[1]}")
            elif c.startswith("videos__"):
                values_fields.append(f"video__{c.split('__', 1)[1]}")
            elif c.startswith("clips__"):
                values_fields.append(c.split('__', 1)[1])
            else:
                values_fields.append(c)
        qs = qs.values(*values_fields)
        return [
            {f: row.get(mapear_campo(f), "") for f in campos}
            for row in qs
        ]

    # ======================
    # Fallback: só uma tabela (users, games, etc)
    # ======================
    if len(campos_por_tabela) == 1:
        tabela = list(campos_por_tabela.keys())[0]
        model = {
            "users": User,
            "streams": Stream,
            "games": Game,
            "videos": Video,
            "clips": Clip
        }[tabela]
        qs = model.objects.all()
        filtro_total = Q()
        if filtros:
            filtro_total &= filtros
        if filtro_busca_global:
            filtro_total &= filtro_busca_global
        if filtro_total:
            qs = qs.filter(filtro_total)
        qs = qs.values(*[f.split("__", 1)[1] for f in campos])
        if order_field and order_field.startswith(f"{tabela}__"):
            field = order_field.split("__", 1)[1]
            qs = qs.order_by(field if order_type == "ASC" else f"-{field}")
        return [{f: row.get(f.split("__", 1)[1], "") for f in campos} for row in qs]

    # Se nada se aplica, retorna lista vazia
    return []



def builder(request):
    get_data = request.GET.copy()
    print("GET_DATA RECEBIDO:", get_data)
    filtro_rapido = get_data.get("filter")
    busca_global = get_data.get("busca_global", "").strip()

    # Configurações rápidas
    if filtro_rapido == "top_streamers":
        get_data.setlist("tables", ["streams", "users"])
        
        if not get_data.getlist("fields"):
            get_data.setlist("fields", [
                "users__id",
                "users__display_name",
                "users__broadcaster_type",
                "streams__viewer_count"
            ])
        get_data["order_field"] = "streams__viewer_count"
        get_data["order_type"] = "DESC"

    elif filtro_rapido == "jogos_populares":
        get_data.setlist("tables", ["streams", "games"])
        get_data.setlist("fields", [
            "games__id",
            "games__name",
            "streams__viewer_count"
        ])
        get_data["order_field"] = "streams__viewer_count"
        get_data["order_type"] = "DESC"

    elif filtro_rapido == "brpt":
        get_data.setlist("tables", ["streams", "users"])
        get_data.setlist("fields", [
            "users__id",
            "users__display_name",
            "users__broadcaster_type",
            "streams__language",
            "streams__viewer_count"
        ])
        get_data["order_field"] = "streams__viewer_count"
        get_data["order_type"] = "DESC"
        get_data["filter"] = "brpt"

    if busca_global:
        if not get_data.getlist("tables"):
            get_data.setlist("tables", ["streams", "users", "games"])
        if not get_data.getlist("fields"):
            get_data.setlist("fields", [
                "users__display_name",
                "streams__viewer_count",
                "streams__language",
                "games__name"
            ])

    print("ANTES DO FORM, GET_DATA:", get_data)

    selected_tables = get_data.getlist("tables") if get_data else []
    tabelas, campos, traducoes_modelos, column_labels = get_tabelas_e_campos(selected_tables)

    # Gera os choices para os campos de filtro dinâmico
    campos_choices = [(f["value"].replace(".", "__"), f["label"]) for f in campos] if campos else []

    # Inicializa o form, agora aceitando os choices dinâmicos
    form = ReportForm(
        get_data or None,
        initial={
            "tables": selected_tables,
            "fields": get_data.getlist("fields"),
        },
        campos_choices=campos_choices,  # <-- importante
    )

    # Atualiza os choices dos fields manualmente também (garante campos nos selects)
    if selected_tables:
        form.fields["fields"].choices = campos_choices
        form.fields["order_field"].choices = campos_choices
        # Filtros avançados: campos dinâmicos
        form.fields["filter_field1"].choices = campos_choices
        form.fields["filter_field2"].choices = campos_choices
    else:
        form.fields["fields"].choices = []
        form.fields["order_field"].choices = []
        form.fields["filter_field1"].choices = []
        form.fields["filter_field2"].choices = []

    results = []
    preview_query = ""
    queryset = None

    # Coleta dados do form
    if form.is_valid() and not filtro_rapido:
        data = form.cleaned_data
    else:
        data = get_data

    queryset = montar_queryset(data)
    preview_query = '[Query baseada no ORM e nos joins automáticos]'

    if isinstance(queryset, list):
        results = queryset
    else:
        results = list(queryset)
    print("RESULTS PARA TEMPLATE (final):", results[:3])

    paginator = Paginator(results, 10)
    page_number = request.GET.get("page")
    results_paginated = paginator.get_page(page_number)

    # Métricas rápidas
    total_streamers = User.objects.count()
    total_views = Stream.objects.aggregate(total=Sum("viewer_count"))["total"] or 0
    popular_games = Stream.objects.values('game_id').annotate(total=Sum('viewer_count')).order_by('-total')
    if popular_games:
        game_id = popular_games[0]['game_id']
        game_name = Game.objects.filter(id=game_id).first().name if game_id else 'N/A'
    else:
        game_name = 'N/A'
    idioma_mais_falado = (
        Stream.objects.values("language")
        .annotate(total=Count("language"))
        .order_by("-total")
        .first()
    )
    ultima_atualizacao = Stream.objects.aggregate(ultima=Max("started_at"))["ultima"]
    if ultima_atualizacao:
        ultima_atualizacao_str = ultima_atualizacao.strftime("%d/%m/%Y %H:%M")
    else:
        ultima_atualizacao_str = "--"

    print("RESULTS PARA TEMPLATE (final):", results[:3])
    print("RESULTS NO RENDER:", results[0].keys() if results else [])
    return render(request, "reports/builder.html", {
        "form": form,
        "results": results_paginated,
        "preview_query": preview_query,
        "selected_tables": selected_tables,
        "column_labels": column_labels,
        "total_streamers": total_streamers,
        "total_views": total_views,
        "jogo_mais_popular": game_name,
        "idioma_mais_falado": idioma_mais_falado["language"] if idioma_mais_falado else "N/A",
        "ultima_atualizacao": ultima_atualizacao_str,
    })

def export_data(request, format):
    get_data = request.GET.copy()
    print("GET_DATA RECEBIDO NO EXPORT:", get_data)

    # Sempre garanta que múltiplos campos vão como lista
    data_dict = {}
    for k in get_data.keys():
        v = get_data.getlist(k)
        data_dict[k] = v if len(v) > 1 else v[0]

    print("DADOS DICT EXPORTAÇÃO:", data_dict)

    # Pegue os fields e tables do dict normalizado
    fieldnames = data_dict.get("fields", [])
    if isinstance(fieldnames, str):
        fieldnames = [fieldnames]
    tables = data_dict.get("tables", [])
    if isinstance(tables, str):
        tables = [tables]
    print("CAMPOS ENVIADOS PARA EXPORTAÇÃO:", fieldnames)
    print("TABELAS ENVIADAS PARA EXPORTAÇÃO:", tables)

    if not fieldnames or not tables:
        return HttpResponse("Nenhum campo ou tabela selecionado.", status=400)

    queryset = montar_queryset(data_dict)
    data = list(queryset)
    print("DATA PARA EXPORTAR:", data[:3])

    if not data:
        data = [{}]

    export_data = []
    for row in data:
        # Usa os nomes exatos que o usuário selecionou
        linha = {campo: row.get(campo, "") for campo in fieldnames}
        export_data.append(linha)

    if format == "csv":
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = "attachment; filename=relatorio.csv"
        writer = csv.DictWriter(response, fieldnames=fieldnames)
        writer.writeheader()
        for row in export_data:
            writer.writerow(row)
        return response

    elif format == "json":
        response = HttpResponse(content_type="application/json")
        response["Content-Disposition"] = "attachment; filename=relatorio.json"
        response.write(json.dumps(export_data, ensure_ascii=False, indent=2))
        return response

    elif format == "excel":
        output = io.BytesIO()
        df = pd.DataFrame(export_data, columns=fieldnames)
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name="Relatório")
        response = HttpResponse(
            output.getvalue(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = "attachment; filename=relatorio.xlsx"
        return response

    else:
        return HttpResponse("Formato não suportado.", status=400)


@csrf_exempt
def grafico_dinamico_relatorio(request):
    if request.method == "POST":
        dados = json.loads(request.body.decode('utf-8'))
        colunas = dados["columns"]
        linhas = dados["rows"]

        x = [row[colunas[0]] for row in linhas]
        y = []
        for c in colunas:
            if c != colunas[0]:
                try:
                    y = [float(row[c]) if row[c] else 0 for row in linhas]
                    break
                except Exception:
                    continue

        fig, ax = plt.subplots(figsize=(9, 4))
        ax.bar(x, y)
        ax.set_xlabel(colunas[0])
        ax.set_ylabel(colunas[1] if len(colunas) > 1 else "Valor")
        ax.set_title("Gráfico Dinâmico do Relatório")
        plt.tight_layout()

        buf = BytesIO()
        plt.savefig(buf, format="png")
        plt.close(fig)
        buf.seek(0)
        return HttpResponse(buf, content_type='image/png')

    return JsonResponse({"erro": "Só POST"}, status=405)
