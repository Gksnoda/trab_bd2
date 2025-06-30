from django import forms
from django.apps import apps

# Traduções para os nomes das tabelas
traducoes_modelos = {
    'users': 'Streamers',
    'streams': 'Transmissões',
    'games': 'Jogos',
    'videos': 'Vídeos',
    'clips': 'Clipes'
}

# Campos permitidos por tabela (igual no views.py)
campos_permitidos = {
    'users': ['display_name', 'broadcaster_type', 'id', 'description', 'created_at'],
    'streams': ['viewer_count', 'language', 'started_at', 'title', 'tag_ids'],
    'games': ['name'],
    'videos': ['title', 'url', 'view_count', 'duration', 'created_at'],
    'clips': ['title', 'url', 'view_count', 'duration', 'created_at'],
}

# Traduções dos campos
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
    },
    'clips': {
        'title': 'Título do Clipe',
        'url': 'Link do Clipe',
        'view_count': 'Visualizações',
        'duration': 'Duração',
        'created_at': 'Data de Criação',
    }
}


class ReportForm(forms.Form):
    busca_global = forms.CharField(
        required=False,
        label="Busca Global",
        widget=forms.TextInput(attrs={
            'placeholder': 'Buscar streamers, jogos, idiomas...',
            'class': 'input-text'
        })
    )

    tables = forms.MultipleChoiceField(
        choices=[],
        widget=forms.CheckboxSelectMultiple,
        label="Tabelas",
        required=True
    )

    fields = forms.MultipleChoiceField(
        choices=[],
        widget=forms.CheckboxSelectMultiple,
        label="Campos",
        required=True
    )

    filter_field = forms.ChoiceField(
        choices=[],
        required=False,
        label="Campo do Filtro"
    )

    filter_operator = forms.ChoiceField(
        choices=[
            ('=', 'Igual (=)'),
            ('!=', 'Diferente (!=)'),
            ('<', 'Menor que (<)'),
            ('>', 'Maior que (>)'),
            ('<=', 'Menor ou igual (<=)'),
            ('>=', 'Maior ou igual (>=)'),
            ('LIKE', 'Contém (LIKE)'),
        ],
        required=False,
        label="Operador"
    )

    filter_value = forms.CharField(
        required=False,
        label="Valor do Filtro",
        widget=forms.TextInput(attrs={'placeholder': 'Ex: pt, >1000, etc'})
    )

    data_inicio = forms.DateField(
        required=False,
        label="De",
        widget=forms.DateInput(attrs={'type': 'date'})
    )

    data_fim = forms.DateField(
        required=False,
        label="Até",
        widget=forms.DateInput(attrs={'type': 'date'})
    )

    order_field = forms.ChoiceField(
        choices=[],
        required=False,
        label="Ordenar por"
    )

    order_type = forms.ChoiceField(
        choices=[('ASC', 'Crescente (ASC)'), ('DESC', 'Decrescente (DESC)')],
        initial='DESC',
        required=False,
        label="Tipo de Ordenação"
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        models = apps.get_app_config('reports').get_models()
        tabela_choices = []
        campo_choices = []

        for model in models:
            nome_tabela = model._meta.db_table
            nome_modelo = model._meta.object_name
            nome_traduzido = traducoes_modelos.get(nome_tabela, nome_modelo)

            tabela_choices.append((nome_tabela, nome_traduzido))

            for field in model._meta.fields:
                nome_campo = field.get_attname_column()[1]

                # ✅ Filtra só os campos permitidos
                if nome_campo not in campos_permitidos.get(nome_tabela, []):
                    continue

                chave = f"{nome_tabela}.{nome_campo}"
                traducao = traducoes_campos.get(nome_tabela, {}).get(nome_campo, nome_campo.title())
                label = f"{nome_traduzido}: {traducao}"

                campo_choices.append((chave, label))

        self.fields['tables'].choices = tabela_choices
        self.fields['fields'].choices = campo_choices
        self.fields['filter_field'].choices = [('', '---')] + campo_choices
        self.fields['order_field'].choices = [('', '---')] + campo_choices
