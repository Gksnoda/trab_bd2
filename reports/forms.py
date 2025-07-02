from django import forms
from django.apps import apps

# Traduções para nomes de tabelas
traducoes_modelos = {
    'users': 'Streamers',
    'streams': 'Transmissões',
    'games': 'Jogos',
    'videos': 'Vídeos',
    'clips': 'Clipes'
}

# Campos permitidos por tabela
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

LOGICAL_CHOICES = [
    ('AND', 'E (AND)'),
    ('OR', 'OU (OR)')
]

OPERATOR_CHOICES = [
    ('=', 'Igual'),
    ('!=', 'Diferente'),
    ('<', 'Menor que'),
    ('<=', 'Menor ou igual'),
    ('>', 'Maior que'),
    ('>=', 'Maior ou igual'),
    ('LIKE', 'Contém'),
]

# NOVO: opções de agregação
AGGREGATION_CHOICES = [
    ('', '---'),
    ('COUNT', 'Contagem (COUNT)'),
    ('SUM', 'Soma (SUM)'),
    ('AVG', 'Média (AVG)'),
    ('MAX', 'Máximo (MAX)'),
    ('MIN', 'Mínimo (MIN)'),
]

class ReportForm(forms.Form):
    # Busca e seleção
    busca_global = forms.CharField(
        required=False,
        label="Busca Global",
        widget=forms.TextInput(attrs={'placeholder': 'Buscar streamers, jogos, idiomas...', 'class': 'input-text'})
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

    # Campos para filtro simples 
    filter_field = forms.ChoiceField(choices=[], required=False, label="Campo do Filtro")
    filter_operator = forms.ChoiceField(choices=OPERATOR_CHOICES, required=False, label="Operador")
    filter_value = forms.CharField(required=False, label="Valor do Filtro", widget=forms.TextInput(attrs={'placeholder': 'Ex: pt, >1000, etc'}))

    # Filtros avançados (Campo 1, Operador, Valor, operador lógico, Campo 2, Operador, Valor)
    filter_field1 = forms.ChoiceField(label='Campo 1', required=False, choices=[])
    filter_operator1 = forms.ChoiceField(label='Operador 1', required=False, choices=OPERATOR_CHOICES)
    filter_value1 = forms.CharField(label='Valor 1', required=False)
    logical_operator = forms.ChoiceField(label='Operador Lógico', required=False, choices=LOGICAL_CHOICES)
    filter_field2 = forms.ChoiceField(label='Campo 2', required=False, choices=[])
    filter_operator2 = forms.ChoiceField(label='Operador 2', required=False, choices=OPERATOR_CHOICES)
    filter_value2 = forms.CharField(label='Valor 2', required=False)

    # agregação
    aggregation_function = forms.ChoiceField(
        choices=AGGREGATION_CHOICES,
        required=False,
        label='Agregação'
    )
    aggregation_field = forms.ChoiceField(
        choices=[],
        required=False,
        label='Campo para Agregar'
    )

    # Datas e ordenação
    data_inicio = forms.DateField(required=False, label="De", widget=forms.DateInput(attrs={'type': 'date'}))
    data_fim = forms.DateField(required=False, label="Até", widget=forms.DateInput(attrs={'type': 'date'}))
    order_field = forms.ChoiceField(choices=[], required=False, label="Ordenar por")
    order_type = forms.ChoiceField(
        choices=[('ASC', 'Crescente (ASC)'), ('DESC', 'Decrescente (DESC)')],
        initial='DESC',
        required=False,
        label="Tipo de Ordenação"
    )

    def __init__(self, *args, campos_choices=None, **kwargs):
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
                if nome_campo not in campos_permitidos.get(nome_tabela, []):
                    continue
                chave = f"{nome_tabela}__{nome_campo}"
                traducao = traducoes_campos.get(nome_tabela, {}).get(nome_campo, nome_campo.title())
                label = f"{nome_traduzido}: {traducao}"
                campo_choices.append((chave, label))

        self.fields['tables'].choices = tabela_choices
        self.fields['fields'].choices = campo_choices
        self.fields['filter_field'].choices = [('', '---')] + campo_choices
        self.fields['order_field'].choices = [('', '---')] + campo_choices

        # Campos de filtros avançados (podem receber choices externos)
        if campos_choices is not None:
            self.fields['filter_field1'].choices = campos_choices
            self.fields['filter_field2'].choices = campos_choices
        else:
            self.fields['filter_field1'].choices = campo_choices
            self.fields['filter_field2'].choices = campo_choices
        # Operadores
        self.fields['filter_operator1'].choices = OPERATOR_CHOICES
        self.fields['filter_operator2'].choices = OPERATOR_CHOICES
        self.fields['logical_operator'].choices = LOGICAL_CHOICES

        # Agregação
        self.fields['aggregation_field'].choices = [('', '---')] + campo_choices
