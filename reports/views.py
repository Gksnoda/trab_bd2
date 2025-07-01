from django.shortcuts import render
from django.core.paginator import Paginator
from .forms import ReportForm, traducoes_modelos
from django.db.models import Count, Sum, Max
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

def montar_queryset(dados):
    from django.db.models import Q, Sum, Count
    from reports.models import User, Stream, Game, Video, Clip

    modelos = {
        "users": User,
        "streams": Stream,
        "games": Game,
        "videos": Video,
        "clips": Clip,
    }

    CAMPOS_VALIDOS = {
        "users": [f.name for f in User._meta.get_fields() if hasattr(f, "attname")],
        "streams": [f.name for f in Stream._meta.get_fields() if hasattr(f, "attname")],
        "games": [f.name for f in Game._meta.get_fields() if hasattr(f, "attname")],
        "videos": [f.name for f in Video._meta.get_fields() if hasattr(f, "attname")],
        "clips": [f.name for f in Clip._meta.get_fields() if hasattr(f, "attname")],
    }

    tabelas = dados.get("tables", [])
    campos = dados.get("fields", [])
    busca_global = dados.get("busca_global", "").strip()
    filter_field = dados.get("filter_field")
    min_value = dados.get("min_value")
    max_value = dados.get("max_value")
    include_nulls = dados.get("include_nulls")
    filter_operator = dados.get("filter_operator")
    filter_value = dados.get("filter_value")

    if isinstance(tabelas, str):
        tabelas = [tabelas]
    if isinstance(campos, str):
        campos = [campos]

    # Monta dict campos por tabela
    campos_por_tabela = {}
    for campo in campos:
        if "__" in campo:
            tabela, atributo = campo.split("__", 1)
        else:
            tabela, atributo = tabelas[0], campo
        campos_por_tabela.setdefault(tabela, []).append(atributo)

    # BUSCA GLOBAL
    if busca_global:
        user_ids = list(User.objects.filter(display_name__icontains=busca_global).values_list('id', flat=True))
        # ADICIONE ISSO AQUI:
        game_ids = list(Game.objects.filter(name__icontains=busca_global).values_list('id', flat=True))

        streams_lang = Stream.objects.filter(language__icontains=busca_global)
        user_ids_lang = list(streams_lang.values_list('user_id', flat=True))
        user_ids = list(set(user_ids + user_ids_lang))
        final = []

        # ALTERE ESTA LINHA:
        if user_ids or game_ids:
            queryset = Stream.objects.all()
            # Adicione os filtros de usuário OU de jogo
            if user_ids:
                queryset = queryset.filter(user_id__in=user_ids)
            if game_ids:
                queryset = queryset.filter(game_id__in=game_ids)

            queryset = queryset.values('user_id', 'title', 'viewer_count', 'language', 'started_at', 'game_id')
            users = User.objects.in_bulk(list(set([row['user_id'] for row in queryset])))
            games = Game.objects.in_bulk(list(set([row['game_id'] for row in queryset if row.get('game_id')])))

            for row in queryset:
                res = {
                    'users__display_name': users.get(row['user_id']).display_name if users.get(row['user_id']) else '',
                    'users__broadcaster_type': users.get(row['user_id']).broadcaster_type if users.get(row['user_id']) else '',
                    'games__name': games.get(row.get('game_id')).name if games.get(row.get('game_id')) else '',
                    'streams__title': row['title'],
                    'streams__viewer_count': row['viewer_count'],
                    'streams__language': row['language'],
                    'streams__started_at': row['started_at'],
                }
                final_row = {campo: res.get(campo, "") for campo in campos}
                final.append(final_row)
            return final
        return []

    # FILTROS RÁPIDOS
    if dados.get("filter") == "top_streamers":
        return Stream.objects.values('user_id').annotate(viewer_count=Sum('viewer_count'))

    if dados.get("filter") == "jogos_populares":
        queryset = (
            Stream.objects
            .values('game_id')
            .annotate(total_views=Sum('viewer_count'))
            .order_by('-total_views')
        )
        game_ids = [row['game_id'] for row in queryset]
        games = Game.objects.in_bulk(game_ids)
        results = []
        for row in queryset:
            ordered = {
                'game_id': row['game_id'],
                'game_name': games.get(row['game_id']).name if games.get(row['game_id']) else 'Desconhecido',
                'total_views': row['total_views'],
            }
            final_row = {campo: ordered.get(campo, "") for campo in campos}
            results.append(final_row)
        return results

    if dados.get("filter") == "brpt":
        streams = Stream.objects.filter(language__in=["pt", "pt-br", "br"]).values("user_id", "viewer_count", "language")
        user_ids = [s["user_id"] for s in streams]
        users = User.objects.filter(id__in=user_ids).values("id", "display_name", "broadcaster_type")
        users_dict = {u["id"]: u for u in users}
        results = []
        print("users_dict:", users_dict)
        print("streams:", list(streams))
        for stream in streams:
            # Observe os nomes dos campos, IGUAL ao HTML
            row = {
                "display_name": users_dict.get(stream["user_id"], {}).get("display_name", ""),
                "language": stream.get("language", ""),
                "broadcaster_type": users_dict.get(stream["user_id"], {}).get("broadcaster_type", ""),
                "total_views": stream.get("viewer_count", 0),
            }
            results.append(row)
        return results



    # STREAMS COMO BASE
    if "streams" in campos_por_tabela:
        campos_streams = list(set(campos_por_tabela.get("streams", []) + ["user_id", "game_id"]))
        streams = Stream.objects.all()

        # Filtro avançado
        filtros = {}
        if filter_field and filter_field.startswith("streams__"):
            field_name = filter_field.split("__", 1)[-1]
            if min_value:
                filtros[f"{field_name}__gte"] = min_value
            if max_value:
                filtros[f"{field_name}__lte"] = max_value
            if filtros:
                streams = streams.filter(**filtros)
            if include_nulls == "1":
                streams = streams.filter(Q(**filtros) | Q(**{f"{field_name}__isnull": True}))
            elif include_nulls == "0":
                streams = streams.exclude(**{f"{field_name}__isnull": True})

        # Filtro operador
        if filter_field and filter_operator and filter_value and filter_field.startswith("streams__"):
            field_name = filter_field.split("__", 1)[-1]
            operadores = {
                "=": "",
                "!=": "",
                "<": "__lt",
                "<=": "__lte",
                ">": "__gt",
                ">=": "__gte",
                "LIKE": "__icontains"
            }
            lookup = operadores.get(filter_operator, "")
            filtro_key = field_name + lookup
            filtro = {}
            filtro[filtro_key] = filter_value

            if filter_operator == "!=":
                streams = streams.exclude(**{field_name: filter_value})
            elif filter_operator == "=":
                streams = streams.filter(**{field_name: filter_value})
            else:
                streams = streams.filter(**filtro)

        streams = list(streams.values(*campos_streams))
        user_ids = [s["user_id"] for s in streams if s.get("user_id")]
        game_ids = [s["game_id"] for s in streams if s.get("game_id")]

        campos_users = list(set(campos_por_tabela.get("users", []) + ["id"])) if "users" in campos_por_tabela else []
        campos_games = list(set(campos_por_tabela.get("games", []) + ["id"])) if "games" in campos_por_tabela else []
        campos_videos = list(set(campos_por_tabela.get("videos", []) + ["user_id", "game_id"])) if "videos" in campos_por_tabela else []
        campos_clips = list(set(campos_por_tabela.get("clips", []) + ["user_id", "game_id"])) if "clips" in campos_por_tabela else []

        users = {u["id"]: u for u in User.objects.filter(id__in=user_ids).values(*campos_users)} if campos_users else {}
        games = {g["id"]: g for g in Game.objects.filter(id__in=game_ids).values(*campos_games)} if campos_games else {}
        campos_videos_validos = [c for c in campos_videos if c in CAMPOS_VALIDOS["videos"]]
        videos = {v["user_id"]: v for v in Video.objects.filter(user_id__in=user_ids).values(*campos_videos_validos)} if campos_videos_validos else {}
        clips = {c["user_id"]: c for c in Clip.objects.filter(user_id__in=user_ids).values(*campos_clips)} if campos_clips else {}

        results = []
        for stream in streams:
            row = {}
            # STREAMS
            for f in campos_streams:
                row["streams__" + f] = stream.get(f, "")
            # USERS
            if campos_users:
                user = users.get(stream.get("user_id")) if stream.get("user_id") else None
                for f in campos_users:
                    if f == "id":
                        continue
                    row["users__" + f] = user.get(f, "") if user else ""
            # GAMES
            if campos_games:
                game = games.get(stream.get("game_id")) if stream.get("game_id") else None
                for f in campos_games:
                    if f == "id":
                        continue
                    row["games__" + f] = game.get(f, "") if game else ""
            # VIDEOS
            if campos_videos:
                video = videos.get(stream.get("user_id")) if stream.get("user_id") else None
                if video:
                    for f in campos_videos:
                        row["videos__" + f] = video.get(f, "")
            # CLIPS
            if campos_clips:
                clip = clips.get(stream.get("user_id")) if stream.get("user_id") else None
                if clip:
                    for f in campos_clips:
                        row["clips__" + f] = clip.get(f, "")
            # Garante todos os campos exportados
            final_row = {campo: row.get(campo, "") for campo in campos}
            print("DEBUG FINAL ROW:", final_row)
            results.append(final_row)
        order_field = dados.get("order_field")
        order_type = dados.get("order_type", "ASC").upper()
        if order_field:
            results.sort(
                key=lambda x: (x.get(order_field) is None, x.get(order_field)),
                reverse=(order_type == "DESC")
            )
        return results

    # CLIPS COMO BASE
    elif "clips" in campos_por_tabela:
        campos_clips = list(set(campos_por_tabela.get("clips", []) + ["user_id", "game_id", "video_id"]))
        clips = Clip.objects.all()
        filtros = {}
        if filter_field and filter_field.startswith("clips__"):
            field_name = filter_field.split("__", 1)[-1]
            if min_value:
                filtros[f"{field_name}__gte"] = min_value
            if max_value:
                filtros[f"{field_name}__lte"] = max_value
            if filtros:
                clips = clips.filter(**filtros)
            if include_nulls == "1":
                clips = clips.filter(Q(**filtros) | Q(**{f"{field_name}__isnull": True}))
            elif include_nulls == "0":
                clips = clips.exclude(**{f"{field_name}__isnull": True})

        if filter_field and filter_operator and filter_value and filter_field.startswith("clips__"):
            field_name = filter_field.split("__", 1)[-1]
            operadores = {
                "=": "",
                "!=": "",
                "<": "__lt",
                "<=": "__lte",
                ">": "__gt",
                ">=": "__gte",
                "LIKE": "__icontains"
            }
            lookup = operadores.get(filter_operator, "")
            filtro_key = field_name + lookup
            filtro = {}
            filtro[filtro_key] = filter_value
            if filter_operator == "!=":
                clips = clips.exclude(**{field_name: filter_value})
            elif filter_operator == "=":
                clips = clips.filter(**{field_name: filter_value})
            else:
                clips = clips.filter(**filtro)
        clips = list(clips.values(*campos_clips))
        user_ids = [c["user_id"] for c in clips if c.get("user_id")]
        game_ids = [c["game_id"] for c in clips if c.get("game_id")]
        video_ids = [c["video_id"] for c in clips if c.get("video_id")]
        campos_users = list(set(campos_por_tabela.get("users", []) + ["id"])) if "users" in campos_por_tabela else []
        campos_games = list(set(campos_por_tabela.get("games", []) + ["id"])) if "games" in campos_por_tabela else []
        campos_videos = list(set(campos_por_tabela.get("videos", []) + ["id"])) if "videos" in campos_por_tabela else []
        users = {u["id"]: u for u in User.objects.filter(id__in=user_ids).values(*campos_users)} if campos_users else {}
        games = {g["id"]: g for g in Game.objects.filter(id__in=game_ids).values(*campos_games)} if campos_games else {}
        videos = {v["id"]: v for v in Video.objects.filter(id__in=video_ids).values(*campos_videos)} if campos_videos else {}
        results = []
        for clip in clips:
            row = {}
            for f in campos_clips:
                row["clips__" + f] = clip.get(f, "")
            if campos_users:
                user = users.get(clip.get("user_id"))
                for f in campos_users:
                    if f == "id": continue
                    row["users__" + f] = user.get(f, "") if user else ""
            if campos_games:
                game = games.get(clip.get("game_id"))
                for f in campos_games:
                    if f == "id": continue
                    row["games__" + f] = game.get(f, "") if game else ""
            if campos_videos:
                video = videos.get(clip.get("video_id"))
                if video:
                    for f in campos_videos:
                        if f == "id": continue
                        row["videos__" + f] = video.get(f, "")
            final_row = {campo: row.get(campo, "") for campo in campos}
            print("DEBUG FINAL ROW:", final_row)
            results.append(final_row)
        order_field = dados.get("order_field")
        order_type = dados.get("order_type", "ASC").upper()
        if order_field:
            results.sort(
                key=lambda x: (x.get(order_field) is None, x.get(order_field)),
                reverse=(order_type == "DESC")
            )
        return results

    # VIDEOS COMO BASE
    elif "videos" in campos_por_tabela:
        # Só pegue campos válidos para Video
        CAMPOS_VALIDOS_VIDEOS = [f.name for f in Video._meta.get_fields() if hasattr(f, "attname")]
        # Adiciona user_id pois existe em Video, mas NÃO game_id!
        campos_videos = list(set(campos_por_tabela.get("videos", []) + ["user_id"]))
        campos_videos = [c for c in campos_videos if c in CAMPOS_VALIDOS_VIDEOS]

        videos = Video.objects.all()
        filtros = {}
        if filter_field and filter_field.startswith("videos__"):
            field_name = filter_field.split("__", 1)[-1]
            if min_value:
                filtros[f"{field_name}__gte"] = min_value
            if max_value:
                filtros[f"{field_name}__lte"] = max_value
            if filtros:
                videos = videos.filter(**filtros)
            if include_nulls == "1":
                videos = videos.filter(Q(**filtros) | Q(**{f"{field_name}__isnull": True}))
            elif include_nulls == "0":
                videos = videos.exclude(**{f"{field_name}__isnull": True})

        if filter_field and filter_operator and filter_value and filter_field.startswith("videos__"):
            field_name = filter_field.split("__", 1)[-1]
            operadores = {
                "=": "",
                "!=": "",
                "<": "__lt",
                "<=": "__lte",
                ">": "__gt",
                ">=": "__gte",
                "LIKE": "__icontains"
            }
            lookup = operadores.get(filter_operator, "")
            filtro_key = field_name + lookup
            filtro = {}
            filtro[filtro_key] = filter_value
            if filter_operator == "!=":
                videos = videos.exclude(**{field_name: filter_value})
            elif filter_operator == "=":
                videos = videos.filter(**{field_name: filter_value})
            else:
                videos = videos.filter(**filtro)

        videos = list(videos.values(*campos_videos))
        user_ids = [v["user_id"] for v in videos if v.get("user_id")]

        # Relaciona User apenas, pois Video NÃO tem game_id
        campos_users = list(set(campos_por_tabela.get("users", []) + ["id"])) if "users" in campos_por_tabela else []
        users = {u["id"]: u for u in User.objects.filter(id__in=user_ids).values(*campos_users)} if campos_users else {}

        results = []
        for video in videos:
            row = {}
            for f in campos_videos:
                row["videos__" + f] = video.get(f, "")
            if campos_users:
                user = users.get(video.get("user_id"))
                for f in campos_users:
                    if f == "id": continue
                    row["users__" + f] = user.get(f, "") if user else ""
            final_row = {campo: row.get(campo, "") for campo in campos}
            results.append(final_row)
        order_field = dados.get("order_field")
        order_type = dados.get("order_type", "ASC").upper()
        if order_field:
            results.sort(
                key=lambda x: (x.get(order_field) is None, x.get(order_field)),
                reverse=(order_type == "DESC")
            )
        return results

    # UMA TABELA APENAS
    if len(campos_por_tabela) == 1:
        tabela = list(campos_por_tabela.keys())[0]
        atributos = campos_por_tabela[tabela]
        model = modelos[tabela]
        queryset = model.objects.all()
        filtros = {}
        if filter_field and filter_field.startswith(tabela + "__"):
            field_name = filter_field.split("__", 1)[-1]
            if min_value:
                filtros[f"{field_name}__gte"] = min_value
            if max_value:
                filtros[f"{field_name}__lte"] = max_value
            if filtros:
                queryset = queryset.filter(**filtros)
            if include_nulls == "1":
                queryset = queryset.filter(Q(**filtros) | Q(**{f"{field_name}__isnull": True}))
            elif include_nulls == "0":
                queryset = queryset.exclude(**{f"{field_name}__isnull": True})
        if filter_field and filter_operator and filter_value and filter_field.startswith(tabela + "__"):
            field_name = filter_field.split("__", 1)[-1]
            operadores = {
                "=": "",
                "!=": "",
                "<": "__lt",
                "<=": "__lte",
                ">": "__gt",
                ">=": "__gte",
                "LIKE": "__icontains"
            }
            lookup = operadores.get(filter_operator, "")
            filtro_key = field_name + lookup
            filtro = {}
            filtro[filtro_key] = filter_value
            if filter_operator == "!=":
                queryset = queryset.exclude(**{field_name: filter_value})
            elif filter_operator == "=":
                queryset = queryset.filter(**{field_name: filter_value})
            else:
                queryset = queryset.filter(**filtro)
        campos_sem_prefixo = [f.split("__", 1)[1] if "__" in f else f for f in campos]
        queryset = queryset.values(*campos_sem_prefixo)
        results = []
        for obj in queryset:
            row = {}
            for f in campos:
                campo_real = f.split("__", 1)[1] if "__" in f else f
                row[f] = obj.get(campo_real, "")
            final_row = {campo: row.get(campo, "") for campo in campos}
            results.append(final_row)
        order_field = dados.get("order_field")
        order_type = dados.get("order_type", "ASC").upper()
        if order_field:
            results.sort(
                key=lambda x: (x.get(order_field) is None, x.get(order_field)),
                reverse=(order_type == "DESC")
            )
        return results

    # ZIP FALLBACK: multi-tabela sem join
    if len(campos_por_tabela) > 1:
        data_tabelas = {}
        for tabela, atributos in campos_por_tabela.items():
            model = modelos[tabela]
            qs = model.objects.all()
            filtros = {}
            if filter_field and filter_field.startswith(tabela + "__"):
                field_name = filter_field.split("__", 1)[-1]
                if min_value:
                    filtros[f"{field_name}__gte"] = min_value
                if max_value:
                    filtros[f"{field_name}__lte"] = max_value
                if filtros:
                    qs = qs.filter(**filtros)
                if include_nulls == "1":
                    qs = qs.filter(Q(**filtros) | Q(**{f"{field_name}__isnull": True}))
                elif include_nulls == "0":
                    qs = qs.exclude(**{f"{field_name}__isnull": True})
            if filter_field and filter_operator and filter_value and filter_field.startswith(tabela + "__"):
                field_name = filter_field.split("__", 1)[-1]
                operadores = {
                    "=": "",
                    "!=": "",
                    "<": "__lt",
                    "<=": "__lte",
                    ">": "__gt",
                    ">=": "__gte",
                    "LIKE": "__icontains"
                }
                lookup = operadores.get(filter_operator, "")
                filtro_key = field_name + lookup
                filtro = {}
                filtro[filtro_key] = filter_value
                if filter_operator == "!=":
                    qs = qs.exclude(**{field_name: filter_value})
                elif filter_operator == "=":
                    qs = qs.filter(**{field_name: filter_value})
                else:
                    qs = qs.filter(**filtro)
            qs = qs.values(*atributos)
            data_tabelas[tabela] = list(qs)
        max_len = max(len(lst) for lst in data_tabelas.values())
        results = []
        for i in range(max_len):
            row = {}
            for tabela, lista in data_tabelas.items():
                if i < len(lista):
                    for k, v in lista[i].items():
                        row[f"{tabela}__{k}"] = v
                else:
                    for k in campos_por_tabela[tabela]:
                        row[f"{tabela}__{k}"] = ""
            final_row = {campo: row.get(campo, "") for campo in campos}
            results.append(final_row)
        order_field = dados.get("order_field")
        order_type = dados.get("order_type", "ASC").upper()
        if order_field:
            results.sort(
                key=lambda x: (x.get(order_field) is None, x.get(order_field)),
                reverse=(order_type == "DESC")
            )
        return results

    return []


def builder(request):
    get_data = request.GET.copy()
    filtro_rapido = get_data.get("filter")
    busca_global = get_data.get("busca_global", "").strip()


    # Filtros rápidos para popular campos/tabelas
    if filtro_rapido == "top_streamers":
        get_data.setlist("tables", ["streams"])
        get_data.setlist("fields", [
            "user_id",
            "language",
            "SUM(viewer_count) AS total_views"
        ])
        get_data["group_by"] = "user_id,language"
        get_data["order_field"] = "total_views"
        get_data["order_type"] = "DESC"

    elif filtro_rapido == "jogos_populares":
        get_data.setlist("tables", ["streams", "users"])
        get_data.setlist("fields", [
            "user__id",
            "SUM(viewer_count) AS total_views"
        ])
        get_data["group_by"] = "user__id"
        get_data["order_field"] = "total_views"
        get_data["order_type"] = "DESC"
    
    if filtro_rapido == "brpt":
        get_data.setlist("tables", ["streams"])
        get_data.setlist("fields", [
            "user_id",
            "language",
            "broadcaster_type",
            "viewer_count"
        ])
        get_data["filter_field"] = "language"
        get_data["filter_operator"] = "="
        get_data["filter_value"] = "pt"
        get_data["order_field"] = "viewer_count"
        get_data["order_type"] = "DESC"

    elif busca_global:
        if not get_data.getlist("tables"):
            get_data.setlist("tables", ["streams", "users", "games"])
        if not get_data.getlist("fields"):
            get_data.setlist("fields", [
                "users__display_name",
                "streams__viewer_count",
                "streams__language",
                "games__name"
            ])

    selected_tables = get_data.getlist("tables") if get_data else []
    tabelas, campos, traducoes_modelos, column_labels = get_tabelas_e_campos(selected_tables)
    form = ReportForm(get_data or None, initial={
        "tables": selected_tables,
        "fields": get_data.getlist("fields"),
    })

    if selected_tables:
        form.fields["fields"].choices = [(f["value"].replace(".", "__"), f["label"]) for f in campos]
        form.fields["filter_field"].choices = [(f["value"].replace(".", "__"), f["label"]) for f in campos]
        form.fields["order_field"].choices = [(f["value"].replace(".", "__"), f["label"]) for f in campos]
    else:
        form.fields["fields"].choices = []
        form.fields["filter_field"].choices = []
        form.fields["order_field"].choices = []

    results = []
    preview_query = ""
    queryset = None

    if form.is_valid() or filtro_rapido or busca_global:
        data = form.cleaned_data if form.is_valid() else get_data
        queryset = montar_queryset(data)
        if hasattr(queryset, 'query'):
            preview_query = str(queryset.query)
        else:
            preview_query = '[Resultado fora do QuerySet]'
        
        if isinstance(queryset, list):
            results = queryset
        else:
            results = list(queryset)
        print("RESULTS PARA TEMPLATE (final):", results[:3])

        # Só rode o enriquecimento se tem user__id!
        if filtro_rapido == "top_streamers":
            user_ids = [row["user_id"] for row in results if row.get("user_id")]

            idiomas = (Stream.objects
                .filter(user_id__in=user_ids)
                .values("user_id", "language")
                .annotate(total=Count("language"))
                .order_by("user_id", "-total"))

            idioma_por_user = {}
            for idioma in idiomas:
                uid = idioma["user_id"]
                if uid not in idioma_por_user:
                    idioma_por_user[uid] = idioma["language"]

            users = User.objects.in_bulk(user_ids)
            for row in results:
                user = users.get(row.get("user_id") or row.get("user id"))
                row["display_name"] = user.display_name if user else ""
                row["broadcaster_type"] = user.broadcaster_type if user else ""
                row["language"] = idioma_por_user.get(row["user_id"], "")
            results.sort(key=lambda x: x.get("viewer_count", 0), reverse=True)
            for row in results:
                ordered = {
                    "user_id": row.get("user_id"),
                    "display_name": row.get("display_name"),
                    "language": row.get("language"),
                    "viewer_count": row.get("viewer_count"),
                    "broadcaster_type": row.get("broadcaster_type"),
                }
                row.clear()
                row.update(ordered)

        elif filtro_rapido == "jogos_populares":
            queryset = (
                Stream.objects
                .values('game_id')
                .annotate(total_views=Sum('viewer_count'))
                .order_by('-total_views')
            )
            game_ids = [row['game_id'] for row in queryset]
            games = Game.objects.in_bulk(game_ids)

            results = []
            for row in queryset:
                ordered = {
                    "game_id": row['game_id'],
                    "game_name": games.get(row['game_id']).name if games.get(row['game_id']) else 'Desconhecido',
                    "total_views": row['total_views'],
                }
                results.append(ordered)

        elif filtro_rapido == "brpt":
            results.sort(key=lambda x: x.get("total_views", 0), reverse=True)
            for row in results:
                ordered = {
                    "display_name": row.get("display_name"),
                    "language": row.get("language"),
                    "broadcaster_type": row.get("broadcaster_type"),
                    "total_views": row.get("total_views"),
                }
                row.clear()
                row.update(ordered)

    paginator = Paginator(results, 10)
    page_number = request.GET.get("page")
    results_paginated = paginator.get_page(page_number)

    # Métricas
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
    import csv
    import json
    import io
    import pandas as pd

    get_data = request.GET.copy()
    fieldnames = get_data.getlist("fields")
    tables = get_data.getlist("tables")
    print("CAMPOS ENVIADOS PARA EXPORTAÇÃO:", fieldnames)

    if not fieldnames or not tables:
        return HttpResponse("Nenhum campo ou tabela selecionado.", status=400)

    queryset = montar_queryset(get_data)
    data = list(queryset)
    print("DATA PARA EXPORTAR:", data[:3])

    if not data:
        # Garante pelo menos cabeçalho vazio se não tem dados
        data = [{}]

    # Garante que TODAS as colunas aparecem (mesmo vazias!)
    export_data = []
    for row in data:
        linha = {campo: row.get(campo, "") for campo in fieldnames}
        export_data.append(linha)

    # CSV
    if format == "csv":
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = "attachment; filename=relatorio.csv"
        writer = csv.DictWriter(response, fieldnames=fieldnames)
        writer.writeheader()
        for row in export_data:
            writer.writerow(row)
        return response

    # JSON
    elif format == "json":
        response = HttpResponse(content_type="application/json")
        response["Content-Disposition"] = "attachment; filename=relatorio.json"
        response.write(json.dumps(export_data, ensure_ascii=False, indent=2))
        return response

    # EXCEL
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
        # Recebe os dados da tabela como JSON
        dados = json.loads(request.body.decode('utf-8'))

        # Exemplo: tenta pegar as colunas automaticamente
        colunas = dados["columns"]  # Exemplo: ['display_name', 'viewer_count']
        linhas = dados["rows"]      # Lista de dicts com cada linha

        # Aqui você escolhe qual gráfico faz sentido para o relatório (exemplo: barras)
        x = [row[colunas[0]] for row in linhas]
        y = []
        # Procura coluna numérica
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